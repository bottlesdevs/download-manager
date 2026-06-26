use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker, time::DelayQueue};
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::{
    DownloadError, DownloadResult, Event, Request,
    context::Context,
    events::EventKind,
    worker::{WorkerMsg, run},
};

pub struct ExponentialBackoff {
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl ExponentialBackoff {
    pub fn next_delay(&self, attempt: u32) -> Duration {
        let factor = 2f64.powi(attempt as i32);
        let delay = self.base_delay.mul_f64(factor);
        delay.min(self.max_delay)
    }
}

static BACKOFF_STRATEGY: ExponentialBackoff = ExponentialBackoff {
    base_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(10),
};

pub(crate) enum SchedulerCmd {
    Enqueue {
        request: Request,
        result_tx: oneshot::Sender<Result<DownloadResult, DownloadError>>,
        cancel_token: CancellationToken,
    },
    Cancel {
        id: Uuid,
    },
}

pub(crate) struct Scheduler {
    ctx: Arc<Context>,
    tracker: TaskTracker,
    shutdown_token: CancellationToken,

    cmd_rx: mpsc::Receiver<SchedulerCmd>,
    worker_tx: mpsc::Sender<WorkerMsg>,
    worker_rx: mpsc::Receiver<WorkerMsg>,

    jobs: HashMap<Uuid, Job>,
    ready: VecDeque<Uuid>,
    delayed: DelayQueue<Uuid>,
}

impl Scheduler {
    #[instrument(level = "info", skip(ctx, tracker, cmd_rx, shutdown_token))]
    pub fn new(
        shutdown_token: CancellationToken,
        ctx: Arc<Context>,
        tracker: TaskTracker,
        cmd_rx: mpsc::Receiver<SchedulerCmd>,
    ) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel(1024);
        Self {
            ctx,
            tracker,
            shutdown_token,
            cmd_rx,
            worker_tx,
            worker_rx,
            ready: VecDeque::new(),
            delayed: DelayQueue::new(),
            jobs: HashMap::new(),
        }
    }

    fn schedule(&mut self, job: Job) {
        let request = &job.request;
        let id = job.id();
        let _ = self.ctx.events.send(Event::new(
            id,
            EventKind::Queued {
                url: request.url().clone(),
                destination: request.destination().to_path_buf(),
            },
        ));
        debug!(%id, url = %request.url(), destination = ?request.destination(), "Job queued");
        self.jobs.insert(id, job);
        self.ready.push_back(id);
    }

    #[instrument(level = "info", skip(self))]
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => self.handle_cmd(cmd).await,
                Some(msg) = self.worker_rx.recv() =>self.handle_worker_msg(msg).await,
                expired = self.delayed.next(), if !self.delayed.is_empty() => {
                    if let Some(exp) = expired {
                        self.ready.push_back(exp.into_inner());
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    info!("Scheduler shutdown requested");
                    break;
                },
            }
            self.try_dispatch();
        }
        info!("Cancelling remaining jobs");
        self.jobs
            .drain()
            .for_each(|(_, job)| job.cancel(self.ctx.events.clone()));
    }

    #[instrument(level = "debug", skip(self, msg))]
    async fn handle_worker_msg(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Metadata { id, info } => {
                if let Some(_) = self.jobs.get(&id) {
                    let _ = self
                        .ctx
                        .events
                        .send(Event::new(id, EventKind::Probed { info }));
                }
            }
            WorkerMsg::Progress {
                id,
                bytes_downloaded,
                total_bytes,
                ..
            } => {
                if let Some(_) = self.jobs.get(&id) {
                    let _ = self.ctx.events.send(Event::new(
                        id,
                        EventKind::Progress {
                            bytes_downloaded,
                            total_bytes,
                        },
                    ));
                }
            }
            WorkerMsg::Finish { id, result } => match result {
                Ok(result) => {
                    let Some(job) = self.jobs.remove(&id) else {
                        return;
                    };
                    info!(%id, "Job completed successfully");
                    job.finish(self.ctx.events.clone(), result)
                }
                Err(DownloadError::Cancelled) => {
                    let Some(job) = self.jobs.remove(&id) else {
                        return;
                    };
                    warn!(%id, "Job cancelled");
                    job.cancel(self.ctx.events.clone())
                }
                Err(error) if error.is_retryable() => {
                    let Some(job) = self.jobs.get_mut(&id) else {
                        return;
                    };
                    if job.attempt >= job.request.config().retries() {
                        warn!(%id, attempt = job.attempt, retries = job.request.config().retries(), error = %error, "Retry limit exceeded; failing job");
                        self.jobs
                            .remove(&id)
                            .map(|job| job.fail(self.ctx.events.clone(), error));
                        return;
                    }
                    let delay = BACKOFF_STRATEGY.next_delay(job.attempt);
                    job.attempt += 1;
                    warn!(%id, attempt = job.attempt, delay_ms = delay.as_millis(), error = %error, "Retryable error; scheduling retry");
                    job.retry(self.ctx.events.clone(), delay);
                    self.delayed.insert(id, delay);
                }
                Err(error) => {
                    let Some(entry) = self.jobs.remove(&id) else {
                        return;
                    };
                    error!(%id, error = %error, "Job failed with non-retryable error");
                    entry.fail(self.ctx.events.clone(), error)
                }
            },
        }
    }

    #[instrument(level = "debug", skip(self, cmd))]
    async fn handle_cmd(&mut self, cmd: SchedulerCmd) {
        match cmd {
            SchedulerCmd::Enqueue {
                request,
                result_tx,
                cancel_token,
            } => {
                let id = request.id();
                debug!(%id, url = %request.url(), destination = ?request.destination(), "Enqueue request");
                self.schedule(Job {
                    request: Arc::new(request),
                    result: Some(result_tx),
                    attempt: 0,
                    cancel_token,
                });
            }
            SchedulerCmd::Cancel { id } => {
                info!(%id, "Received cancel command");
                self.jobs
                    .remove(&id)
                    .map(|job| job.cancel(self.ctx.events.clone()));
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn try_dispatch(&mut self) {
        while let Some(id) = self.ready.pop_front() {
            if self.shutdown_token.is_cancelled() {
                return;
            }
            let guard = match self.ctx.active_guard() {
                Ok(g) => g,
                Err(_) => {
                    // No permits left; put the job back to the front and stop dispatching for now.
                    trace!(%id, "No semaphore permits available; requeuing to front");
                    self.ready.push_front(id);
                    return;
                }
            };

            let Some(job) = self.jobs.get_mut(&id) else {
                drop(guard);
                trace!(%id, "Job not found when dispatching");
                continue;
            };

            let request = job.request.clone();
            let cancel_token = job.cancel_token.clone();
            let client = self.ctx.client.clone();
            let worker_tx = self.worker_tx.clone();

            info!(%id, "Dispatching job to worker");
            self.tracker.spawn(async move {
                let _guard = guard;
                run(request, client, worker_tx, cancel_token).await;
            });
        }
    }
}

pub(crate) struct Job {
    request: Arc<Request>,
    attempt: u32,
    result: Option<oneshot::Sender<Result<DownloadResult, DownloadError>>>,
    cancel_token: CancellationToken,
}

impl Job {
    fn id(&self) -> Uuid {
        self.request.id()
    }

    fn send_result(self, result: Result<DownloadResult, DownloadError>) {
        if let Some(result_tx) = self.result {
            let _ = result_tx.send(result);
        }
    }

    fn fail(self, event_tx: broadcast::Sender<Event>, error: DownloadError) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Failed {
                error: error.to_string(),
            },
        ));
        self.send_result(Err(error));
    }

    fn finish(self, event_tx: broadcast::Sender<Event>, result: DownloadResult) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Completed {
                path: result.path.clone(),
                bytes_downloaded: result.bytes_downloaded,
            },
        ));
        self.send_result(Ok(result))
    }

    fn retry(&self, event_tx: broadcast::Sender<Event>, delay: Duration) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Retrying {
                attempt: self.attempt,
                next_delay_ms: delay.as_millis() as u64,
            },
        ));
    }

    fn cancel(self, event_tx: broadcast::Sender<Event>) {
        self.cancel_token.cancel();
        let _ = event_tx.send(Event::new(self.id(), EventKind::Cancelled));
        self.send_result(Err(DownloadError::Cancelled))
    }
}
