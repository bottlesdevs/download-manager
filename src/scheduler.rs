use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    context::Context,
    download::DownloadResult,
    error::{Error, Result, ResultExt},
    events::{DownloadState, Event, EventKind},
    request::Request,
    storage,
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
        request: Arc<Request>,
        result_tx: oneshot::Sender<Result<DownloadResult>>,
        cancel_token: CancellationToken,
    },
    Cancel {
        id: Uuid,
    },
    CancelAll,
    SetMaxConcurrent {
        max_concurrent: NonZeroUsize,
    },
}

pub(crate) struct Scheduler {
    ctx: Arc<Context>,
    max_concurrent: NonZeroUsize,

    cmd_rx: mpsc::Receiver<SchedulerCmd>,
    worker_tx: mpsc::Sender<WorkerMsg>,
    worker_rx: mpsc::Receiver<WorkerMsg>,

    jobs: HashMap<Uuid, Job>,
    ready: VecDeque<Uuid>,
    delayed: DelayQueue<Uuid>,
    workers: JoinSet<(Uuid, Result<DownloadResult>)>,
}

impl Scheduler {
    #[instrument(level = "info", skip(ctx, cmd_rx))]
    pub fn new(
        max_concurrent: NonZeroUsize,
        ctx: Arc<Context>,
        cmd_rx: mpsc::Receiver<SchedulerCmd>,
    ) -> Self {
        let (worker_tx, worker_rx) = mpsc::channel(1024);
        Self {
            ctx,
            max_concurrent,
            cmd_rx,
            worker_tx,
            worker_rx,
            ready: VecDeque::new(),
            delayed: DelayQueue::new(),
            jobs: HashMap::new(),
            workers: JoinSet::new(),
        }
    }

    fn schedule(&mut self, job: Job) {
        let request = &job.request;
        let id = job.id();
        let _ = self.ctx.events.send(Event::new(
            id,
            EventKind::Lifecycle {
                state: DownloadState::Queued,
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
                cmd = self.cmd_rx.recv() => match cmd {
                    Some(cmd) => self.handle_cmd(cmd).await,
                    None => break,
                },
                Some(msg) = self.worker_rx.recv() => self.handle_worker_msg(msg).await,
                Some(result) = self.workers.join_next() => {
                    if let Some((id, result)) = result.log_warn() {
                        self.handle_worker_result(id, result);
                    }
                }
                Some(expired) = self.delayed.next() => {
                    let id = expired.into_inner();
                    if let Some(job) = self.jobs.get_mut(&id) {
                        job.state = DownloadState::Queued;
                        self.ready.push_back(id);
                    }
                }
                _ = self.ctx.cancel_root.cancelled() => break,
            }
            self.try_dispatch();
        }

        self.cmd_rx.close();
        self.handle_cmd(SchedulerCmd::CancelAll).await;

        while !self.workers.is_empty() {
            tokio::select! {
                Some(msg) = self.worker_rx.recv() => self.handle_worker_msg(msg).await,
                Some(result) = self.workers.join_next() => {
                    if let Some((id, result)) = result.log_warn() {
                        self.handle_worker_result(id, result);
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self, msg))]
    async fn handle_worker_msg(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::Metadata { id, info } => {
                if self.jobs.contains_key(&id) {
                    let _ = self
                        .ctx
                        .events
                        .send(Event::new(id, EventKind::Metadata { info }));
                }
            }
            WorkerMsg::Progress {
                id,
                bytes_downloaded,
                total_bytes,
                ..
            } => {
                if self.jobs.contains_key(&id) {
                    let _ = self.ctx.events.send(Event::new(
                        id,
                        EventKind::Progress {
                            bytes_downloaded,
                            total_bytes,
                        },
                    ));
                }
            }
        }
    }

    fn handle_worker_result(&mut self, id: Uuid, result: Result<DownloadResult>) {
        let Some(mut job) = self.jobs.remove(&id) else {
            return;
        };

        match result {
            Ok(result) => job.finish(self.ctx.events.clone(), result),
            Err(Error::Cancelled) => job.cancel(self.ctx.events.clone()),
            Err(error) if job.state != DownloadState::Cancelling && error.is_retryable() => {
                if job.attempt >= job.request.config.retries() {
                    warn!(%id, attempt = job.attempt, retries = job.request.config.retries(), error = %error, "Retry limit exceeded; failing job");
                    job.fail(self.ctx.events.clone(), error);
                    return;
                }
                let delay = BACKOFF_STRATEGY.next_delay(job.attempt);
                job.attempt += 1;
                job.state = DownloadState::Retrying;
                warn!(%id, attempt = job.attempt, delay_ms = delay.as_millis(), error = %error, "Retryable error; scheduling retry");
                job.retry(self.ctx.events.clone(), delay);
                self.jobs.insert(id, job);
                self.delayed.insert(id, delay);
            }
            Err(error) => job.fail(self.ctx.events.clone(), error),
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
                    request,
                    result: Some(result_tx),
                    attempt: 0,
                    cancel_token,
                    state: DownloadState::Queued,
                });
            }
            SchedulerCmd::Cancel { id } => {
                info!(%id, "Received cancel command");
                self.cancel_job(id).await;
            }
            SchedulerCmd::CancelAll => {
                self.ready.clear();
                self.delayed.clear();

                let ids: Vec<_> = self.jobs.keys().copied().collect();
                for id in ids {
                    self.cancel_job(id).await;
                }
            }
            SchedulerCmd::SetMaxConcurrent { max_concurrent } => {
                self.max_concurrent = max_concurrent;
                info!(max_concurrent, "Updated download concurrency limit");
            }
        }
    }

    async fn cancel_job(&mut self, id: Uuid) {
        let Some(job) = self.jobs.get_mut(&id) else {
            return;
        };

        match job.state {
            DownloadState::Queued | DownloadState::Retrying => {
                let job = self.jobs.remove(&id).unwrap();
                let cleanup = storage::discard_partial(job.request.destination()).await;
                if let Err(error) = cleanup {
                    job.fail(self.ctx.events.clone(), error);
                } else {
                    job.cancel(self.ctx.events.clone());
                }
            }
            DownloadState::Running => {
                job.state = DownloadState::Cancelling;
                job.cancel_token.cancel();
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Cancelling,
                    },
                ));
            }
            DownloadState::Cancelling => {}
            _ => {}
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn try_dispatch(&mut self) {
        while self.workers.len() < self.max_concurrent.get() {
            let Some(id) = self.ready.pop_front() else {
                break;
            };
            if self.ctx.cancel_root.is_cancelled() {
                return;
            }

            let Some(job) = self.jobs.get_mut(&id) else {
                continue;
            };

            job.state = DownloadState::Running;
            let _ = self.ctx.events.send(Event::new(
                id,
                EventKind::Lifecycle {
                    state: DownloadState::Running,
                },
            ));

            let request = job.request.clone();
            let cancel_token = job.cancel_token.clone();
            let client = self.ctx.client.clone();
            let worker_tx = self.worker_tx.clone();

            info!(%id, "Dispatching job to worker");
            self.workers.spawn(async move {
                let result = AssertUnwindSafe(run(request, client, worker_tx, cancel_token))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(Error::Unknown("Download worker panicked".into())));
                (id, result)
            });
        }
    }
}

pub(crate) struct Job {
    request: Arc<Request>,
    attempt: u32,
    result: Option<oneshot::Sender<Result<DownloadResult>>>,
    cancel_token: CancellationToken,
    state: DownloadState,
}

impl Job {
    fn id(&self) -> Uuid {
        self.request.id()
    }

    fn send_result(self, result: Result<DownloadResult>) {
        if let Some(result_tx) = self.result {
            let _ = result_tx.send(result);
        }
    }

    fn fail(self, event_tx: broadcast::Sender<Event>, error: Error) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Lifecycle {
                state: DownloadState::Failed {
                    error: error.to_string(),
                },
            },
        ));
        self.send_result(Err(error));
    }

    fn finish(self, event_tx: broadcast::Sender<Event>, result: DownloadResult) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Lifecycle {
                state: DownloadState::Completed,
            },
        ));
        self.send_result(Ok(result))
    }

    fn retry(&self, event_tx: broadcast::Sender<Event>, delay: Duration) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::RetryScheduled {
                attempt: self.attempt,
                next_delay_ms: delay.as_millis() as u64,
            },
        ));
    }

    fn cancel(self, event_tx: broadcast::Sender<Event>) {
        self.cancel_token.cancel();
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Lifecycle {
                state: DownloadState::Cancelled,
            },
        ));
        self.send_result(Err(Error::Cancelled))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_grows_and_caps() {
        let backoff = ExponentialBackoff {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(8),
        };

        assert_eq!(backoff.next_delay(0), Duration::from_secs(1));
        assert_eq!(backoff.next_delay(1), Duration::from_secs(2));
        assert_eq!(backoff.next_delay(2), Duration::from_secs(4));
        assert_eq!(backoff.next_delay(3), Duration::from_secs(8));
        assert_eq!(backoff.next_delay(10), Duration::from_secs(8));
    }

    #[tokio::test]
    async fn queued_job_can_be_cancelled_without_starting_a_worker() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let ctx = Context::new();
        let mut scheduler = Scheduler::new(NonZeroUsize::new(1).unwrap(), ctx, cmd_rx);
        let request = Arc::new(
            Request::builder(
                reqwest::Url::parse("https://example.com/file").unwrap(),
                std::env::temp_dir().join(format!("dm-queued-test-{}", Uuid::new_v4())),
            )
            .build()
            .unwrap(),
        );
        let id = request.id();
        let cancel_token = CancellationToken::new();
        let (result_tx, result_rx) = oneshot::channel();

        scheduler
            .handle_cmd(SchedulerCmd::Enqueue {
                request,
                result_tx,
                cancel_token: cancel_token.clone(),
            })
            .await;
        scheduler.handle_cmd(SchedulerCmd::Cancel { id }).await;

        assert!(cancel_token.is_cancelled());
        assert!(scheduler.jobs.is_empty());
        assert!(matches!(result_rx.await.unwrap(), Err(Error::Cancelled)));
    }
}
