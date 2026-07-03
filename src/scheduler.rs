use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, StreamExt};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::{
    sync::CancellationToken,
    time::{DelayQueue, delay_queue::Key},
};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    context::Context,
    download::DownloadResult,
    error::{Error, Result, ResultExt},
    events::{DownloadState, Event, EventKind, Progress},
    request::Request,
    storage,
    worker::{WorkerResult, run},
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
        id: Uuid,
        request: Arc<Request>,
        progress_tx: watch::Sender<Progress>,
        result_tx: oneshot::Sender<Result<DownloadResult>>,
    },
    Pause {
        id: Uuid,
        ack: oneshot::Sender<Result<()>>,
    },
    Resume {
        id: Uuid,
        ack: oneshot::Sender<Result<()>>,
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

    jobs: HashMap<Uuid, Job>,
    ready: VecDeque<Uuid>,
    delayed: DelayQueue<Uuid>,
    workers: JoinSet<(Uuid, Result<WorkerResult>)>,
}

impl Scheduler {
    #[instrument(level = "info", skip(ctx, cmd_rx))]
    pub fn new(
        max_concurrent: NonZeroUsize,
        ctx: Arc<Context>,
        cmd_rx: mpsc::Receiver<SchedulerCmd>,
    ) -> Self {
        Self {
            ctx,
            max_concurrent,
            cmd_rx,
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
                Some(result) = self.workers.join_next() => {
                    if let Some((id, result)) = result.log_warn() {
                        self.handle_worker_result(id, result).await;
                    }
                }
                Some(expired) = self.delayed.next() => {
                    let id = expired.into_inner();
                    if let Some(job) = self.jobs.get_mut(&id).filter(|job| job.state == DownloadState::Retrying) {
                        job.retry_key = None;
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

        while let Some(result) = self.workers.join_next().await {
            if let Some((id, result)) = result.log_warn() {
                self.handle_worker_result(id, result).await;
            }
        }
    }

    async fn handle_worker_result(&mut self, id: Uuid, result: Result<WorkerResult>) {
        let Some(mut job) = self.jobs.remove(&id) else {
            return;
        };

        match result {
            Ok(WorkerResult::Finished(result)) => {
                job.resolve_pause_waiters(Ok(()));
                job.finish(self.ctx.events.clone(), result)
            }
            Ok(WorkerResult::Stopped) if job.state == DownloadState::Pausing => {
                job.state = DownloadState::Paused;
                job.resolve_pause_waiters(Ok(()));
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Paused,
                    },
                ));
                self.jobs.insert(id, job);
            }
            Ok(WorkerResult::Stopped) => {
                match storage::discard_partial(job.request.destination()).await {
                    Ok(()) => job.cancel(self.ctx.events.clone()),
                    Err(error) => job.fail(self.ctx.events.clone(), error),
                }
            }
            Err(error) if job.state == DownloadState::Pausing => {
                job.resolve_pause_waiters(Err(error.clone()));
                job.fail(self.ctx.events.clone(), error);
            }
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
                job.retry_key = Some(self.delayed.insert(id, delay));
                self.jobs.insert(id, job);
            }
            Err(error) => job.fail(self.ctx.events.clone(), error),
        }
    }

    #[instrument(level = "debug", skip(self, cmd))]
    async fn handle_cmd(&mut self, cmd: SchedulerCmd) {
        match cmd {
            SchedulerCmd::Enqueue {
                id,
                request,
                progress_tx,
                result_tx,
            } => {
                debug!(%id, url = %request.url(), destination = ?request.destination(), "Enqueue request");
                self.schedule(Job {
                    id,
                    request,
                    progress_tx,
                    result: Some(result_tx),
                    pause_waiters: Vec::new(),
                    attempt: 0,
                    retry_key: None,
                    cancel_token: self.ctx.cancel_root.child_token(),
                    state: DownloadState::Queued,
                });
            }
            SchedulerCmd::Pause { id, ack } => self.pause_job(id, ack),
            SchedulerCmd::Resume { id, ack } => self.resume_job(id, ack),
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

    fn pause_job(&mut self, id: Uuid, ack: oneshot::Sender<Result<()>>) {
        let Some(job) = self.jobs.get_mut(&id) else {
            let _ = ack.send(Err(Error::Unknown(format!("unknown download {id}"))));
            return;
        };
        match job.state {
            DownloadState::Queued => {
                self.ready.retain(|queued| *queued != id);
                job.state = DownloadState::Paused;
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Paused,
                    },
                ));
                let _ = ack.send(Ok(()));
            }
            DownloadState::Retrying => {
                if let Some(key) = job.retry_key.take() {
                    self.delayed.remove(&key);
                }
                job.state = DownloadState::Paused;
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Paused,
                    },
                ));
                let _ = ack.send(Ok(()));
            }
            DownloadState::Paused => {
                let _ = ack.send(Ok(()));
            }
            DownloadState::Running => {
                job.state = DownloadState::Pausing;
                job.pause_waiters.push(ack);
                job.cancel_token.cancel();
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Pausing,
                    },
                ));
            }
            DownloadState::Pausing => job.pause_waiters.push(ack),
            _ => {
                let _ = ack.send(Err(Error::InvalidRequest(
                    "download cannot be paused in its current state".into(),
                )));
            }
        }
    }

    fn resume_job(&mut self, id: Uuid, ack: oneshot::Sender<Result<()>>) {
        let Some(job) = self.jobs.get_mut(&id) else {
            let _ = ack.send(Err(Error::Unknown(format!("unknown download {id}"))));
            return;
        };
        match job.state {
            DownloadState::Paused => {
                job.state = DownloadState::Queued;
                self.ready.push_back(id);
                let _ = self.ctx.events.send(Event::new(
                    id,
                    EventKind::Lifecycle {
                        state: DownloadState::Queued,
                    },
                ));
                let _ = ack.send(Ok(()));
            }
            DownloadState::Queued | DownloadState::Running | DownloadState::Retrying => {
                let _ = ack.send(Ok(()));
            }
            _ => {
                let _ = ack.send(Err(Error::InvalidRequest(
                    "download cannot be resumed in its current state".into(),
                )));
            }
        }
    }

    async fn cancel_job(&mut self, id: Uuid) {
        let Some(job) = self.jobs.get_mut(&id) else {
            return;
        };

        match job.state {
            DownloadState::Queued | DownloadState::Retrying | DownloadState::Paused => {
                let job = self.jobs.remove(&id).unwrap();
                let cleanup = storage::discard_partial(job.request.destination()).await;
                if let Err(error) = cleanup {
                    job.fail(self.ctx.events.clone(), error);
                } else {
                    job.cancel(self.ctx.events.clone());
                }
            }
            DownloadState::Running | DownloadState::Pausing => {
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
            let progress_tx = job.progress_tx.clone();
            let cancel_token = self.ctx.child_token();
            job.cancel_token = cancel_token.clone();
            let client = self.ctx.client.clone();

            info!(%id, "Dispatching job to worker");
            self.workers.spawn(async move {
                let result = AssertUnwindSafe(run(request, client, progress_tx, cancel_token))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(Error::Unknown("Download worker panicked".into())));
                (id, result)
            });
        }
    }
}

pub(crate) struct Job {
    id: Uuid,
    request: Arc<Request>,
    progress_tx: watch::Sender<Progress>,
    attempt: u32,
    retry_key: Option<Key>,
    result: Option<oneshot::Sender<Result<DownloadResult>>>,
    pause_waiters: Vec<oneshot::Sender<Result<()>>>,
    cancel_token: CancellationToken,
    state: DownloadState,
}

impl Job {
    fn id(&self) -> Uuid {
        self.id
    }

    fn resolve_pause_waiters(&mut self, result: Result<()>) {
        self.pause_waiters.drain(..).for_each(|waiter| {
            let _ = waiter.send(result.clone());
        });
    }

    fn send_result(mut self, result: Result<DownloadResult>) {
        self.resolve_pause_waiters(result.clone().map(|_| ()));
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

    async fn scheduler_with_job() -> (Scheduler, Uuid, oneshot::Receiver<Result<DownloadResult>>) {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let ctx = Context::new();
        let mut scheduler = Scheduler::new(NonZeroUsize::new(1).unwrap(), ctx, cmd_rx);
        let request = Arc::new(
            Request::builder(
                reqwest::Url::parse("https://example.com/file").unwrap(),
                std::env::temp_dir().join(format!("dm-pause-test-{}", Uuid::new_v4())),
            )
            .build()
            .unwrap(),
        );
        let id = Uuid::new_v4();
        let (progress_tx, _progress_rx) = watch::channel(Progress::new(0, None));
        let (result_tx, result_rx) = oneshot::channel();

        scheduler
            .handle_cmd(SchedulerCmd::Enqueue {
                id,
                request,
                progress_tx,
                result_tx,
            })
            .await;

        (scheduler, id, result_rx)
    }

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
        let id = Uuid::new_v4();
        let (progress_tx, _progress_rx) = watch::channel(Progress::new(0, None));
        let (result_tx, result_rx) = oneshot::channel();

        scheduler
            .handle_cmd(SchedulerCmd::Enqueue {
                id,
                request,
                progress_tx,
                result_tx,
            })
            .await;
        scheduler.handle_cmd(SchedulerCmd::Cancel { id }).await;

        assert!(scheduler.jobs.is_empty());
        assert!(matches!(result_rx.await.unwrap(), Err(Error::Cancelled)));
    }

    #[tokio::test]
    async fn pausing_retrying_job_removes_retry_timer() {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let ctx = Context::new();
        let mut scheduler = Scheduler::new(NonZeroUsize::new(1).unwrap(), ctx, cmd_rx);
        let request = Arc::new(
            Request::builder(
                reqwest::Url::parse("https://example.com/file").unwrap(),
                std::env::temp_dir().join(format!("dm-pause-test-{}", Uuid::new_v4())),
            )
            .build()
            .unwrap(),
        );
        let id = Uuid::new_v4();
        let (progress_tx, _progress_rx) = watch::channel(Progress::new(0, None));
        let (result_tx, _result_rx) = oneshot::channel();

        scheduler
            .handle_cmd(SchedulerCmd::Enqueue {
                id,
                request,
                progress_tx,
                result_tx,
            })
            .await;
        let retry_timer = scheduler.delayed.insert(id, Duration::from_secs(60));
        let job = scheduler.jobs.get_mut(&id).unwrap();
        job.state = DownloadState::Retrying;
        job.retry_key = Some(retry_timer);

        let (ack, result) = oneshot::channel();
        scheduler.pause_job(id, ack);

        assert!(result.await.unwrap().is_ok());
        assert!(scheduler.delayed.is_empty());
        let job = scheduler.jobs.get(&id).unwrap();
        assert_eq!(job.state, DownloadState::Paused);
        assert!(job.retry_key.is_none());
    }

    #[tokio::test]
    async fn running_job_pause_notifies_all_waiters_and_can_resume() {
        let (mut scheduler, id, mut download_result) = scheduler_with_job().await;
        let cancel_token = {
            let job = scheduler.jobs.get_mut(&id).unwrap();
            job.state = DownloadState::Running;
            job.cancel_token.clone()
        };
        let (first_ack, mut first_pause_result) = oneshot::channel();
        let (second_ack, mut second_pause_result) = oneshot::channel();

        scheduler.pause_job(id, first_ack);
        scheduler.pause_job(id, second_ack);

        assert!(cancel_token.is_cancelled());
        assert_eq!(scheduler.jobs[&id].state, DownloadState::Pausing);
        assert!(matches!(
            first_pause_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_pause_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        scheduler
            .handle_worker_result(id, Ok(WorkerResult::Stopped))
            .await;

        assert!(first_pause_result.await.unwrap().is_ok());
        assert!(second_pause_result.await.unwrap().is_ok());
        assert_eq!(scheduler.jobs[&id].state, DownloadState::Paused);
        assert!(matches!(
            download_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let (ack, resume_result) = oneshot::channel();
        scheduler.resume_job(id, ack);

        assert!(resume_result.await.unwrap().is_ok());
        assert_eq!(scheduler.jobs[&id].state, DownloadState::Queued);
        assert_eq!(scheduler.ready.back(), Some(&id));
    }

    #[tokio::test]
    async fn running_pause_reports_worker_failure() {
        let (mut scheduler, id, download_result) = scheduler_with_job().await;
        scheduler.jobs.get_mut(&id).unwrap().state = DownloadState::Running;
        let (ack, pause_result) = oneshot::channel();
        scheduler.pause_job(id, ack);

        scheduler
            .handle_worker_result(id, Err(Error::Unknown("checkpoint failed".into())))
            .await;

        assert!(matches!(
            pause_result.await.unwrap(),
            Err(Error::Unknown(message)) if message == "checkpoint failed"
        ));
        assert!(matches!(
            download_result.await.unwrap(),
            Err(Error::Unknown(message)) if message == "checkpoint failed"
        ));
        assert!(!scheduler.jobs.contains_key(&id));
    }
}
