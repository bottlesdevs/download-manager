mod job;

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use futures_util::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::time::DelayQueue;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    context::Context,
    download::DownloadResult,
    error::{Error, Result, ResultExt},
    events::{Event, EventKind, Progress},
    request::Request,
    storage,
    worker::{WorkerResult, run},
};

use job::{Job, JobEvent, JobState};

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
        let _ = self.ctx.events.send(Event::new(id, EventKind::Queued));
        debug!(%id, url = %request.url(), destination = ?request.destination(), "Job queued");
        self.jobs.insert(id, job);
        self.ready.push_back(id);
    }

    async fn cancel_job(&self, job: Job) {
        match storage::discard_partial(job.request.destination()).await {
            Ok(()) => job.finalize(self.ctx.events.clone(), Err(Error::Cancelled)),
            Err(error) => job.finalize(self.ctx.events.clone(), Err(error)),
        }
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
                        self.transition_job(id, JobEvent::Worker(result)).await;
                    }
                }
                Some(expired) = self.delayed.next() => {
                    let id = expired.into_inner();
                    self.transition_job(id, JobEvent::RetryElapsed).await;
                }
                _ = self.ctx.cancel_root.cancelled() => break,
            }
            self.try_dispatch().await;
        }

        self.cmd_rx.close();
        self.handle_cmd(SchedulerCmd::CancelAll).await;

        while let Some(result) = self.workers.join_next().await {
            if let Some((id, result)) = result.log_warn() {
                self.transition_job(id, JobEvent::Worker(result)).await;
            }
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
                    attempt: 0,
                    state: JobState::Queued,
                });
            }
            SchedulerCmd::Pause { id, ack } => self.transition_job(id, JobEvent::Pause(ack)).await,
            SchedulerCmd::Resume { id, ack } => {
                self.transition_job(id, JobEvent::Resume(ack)).await
            }
            SchedulerCmd::Cancel { id } => self.transition_job(id, JobEvent::Cancel).await,
            SchedulerCmd::CancelAll => {
                let ids: Vec<_> = self.jobs.keys().copied().collect();
                for id in ids {
                    self.transition_job(id, JobEvent::Cancel).await;
                }
                self.ready.clear();
                self.delayed.clear();
            }
            SchedulerCmd::SetMaxConcurrent { max_concurrent } => {
                self.max_concurrent = max_concurrent;
                info!(max_concurrent, "Updated download concurrency limit");
            }
        }
    }

    async fn transition_job(&mut self, id: Uuid, event: JobEvent) {
        match event {
            JobEvent::Dispatch => {
                let (request, progress_tx, stop) = {
                    let Some(job) = self.jobs.get_mut(&id) else {
                        return;
                    };
                    if !matches!(&job.state, JobState::Queued) {
                        return;
                    }

                    let stop = self.ctx.child_token();
                    job.state = JobState::Running { stop: stop.clone() };
                    (job.request.clone(), job.progress_tx.clone(), stop)
                };

                let _ = self.ctx.events.send(Event::new(id, EventKind::Started));
                let client = self.ctx.client.clone();
                info!(%id, "Dispatching job to worker");
                self.workers.spawn(async move {
                    let result = AssertUnwindSafe(run(request, client, progress_tx, stop))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| Err(Error::Unknown("Download worker panicked".into())));
                    (id, result)
                });
            }
            JobEvent::RetryElapsed => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    return;
                };
                if !matches!(&job.state, JobState::Retrying { .. }) {
                    return;
                }

                job.state = JobState::Queued;
                self.ready.push_back(id);
                let _ = self.ctx.events.send(Event::new(id, EventKind::Queued));
            }
            JobEvent::Pause(ack) => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    let _ = ack.send(Err(Error::Unknown(format!("unknown download {id}"))));
                    return;
                };

                match &mut job.state {
                    JobState::Queued => {
                        self.ready.retain(|queued| *queued != id);
                        job.pause(self.ctx.events.clone());
                        let _ = ack.send(Ok(()));
                    }
                    JobState::Retrying { timer } => {
                        self.delayed.remove(timer);
                        job.pause(self.ctx.events.clone());
                        let _ = ack.send(Ok(()));
                    }
                    JobState::Running { stop } => {
                        stop.cancel();
                        job.state = JobState::Pausing { waiters: vec![ack] };
                        let _ = self
                            .ctx
                            .events
                            .send(Event::new(id, EventKind::PauseStarted));
                    }
                    JobState::Pausing { waiters } => waiters.push(ack),
                    JobState::Paused => {
                        let _ = ack.send(Ok(()));
                    }
                    JobState::Cancelling => {
                        let _ = ack.send(Err(Error::InvalidRequest(
                            "download cannot be paused in its current state".into(),
                        )));
                    }
                }
            }
            JobEvent::Resume(ack) => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    let _ = ack.send(Err(Error::Unknown(format!("unknown download {id}"))));
                    return;
                };

                match &job.state {
                    JobState::Paused => {
                        job.state = JobState::Queued;
                        self.ready.push_back(id);
                        let _ = self.ctx.events.send(Event::new(id, EventKind::Queued));
                        let _ = ack.send(Ok(()));
                    }
                    JobState::Queued | JobState::Running { .. } | JobState::Retrying { .. } => {
                        let _ = ack.send(Ok(()));
                    }
                    JobState::Pausing { .. } | JobState::Cancelling => {
                        let _ = ack.send(Err(Error::InvalidRequest(
                            "download cannot be resumed in its current state".into(),
                        )));
                    }
                }
            }
            JobEvent::Cancel => {
                {
                    let Some(job) = self.jobs.get_mut(&id) else {
                        return;
                    };

                    match &mut job.state {
                        JobState::Queued | JobState::Paused => {}
                        JobState::Retrying { timer } => {
                            self.delayed.remove(timer);
                        }
                        JobState::Running { stop } => {
                            stop.cancel();
                            job.state = JobState::Cancelling;
                            let _ = self
                                .ctx
                                .events
                                .send(Event::new(id, EventKind::CancellationStarted));
                            return;
                        }
                        JobState::Pausing { waiters } => {
                            waiters.drain(..).for_each(|waiter| {
                                let _ = waiter.send(Err(Error::Cancelled));
                            });
                            job.state = JobState::Cancelling;
                            let _ = self
                                .ctx
                                .events
                                .send(Event::new(id, EventKind::CancellationStarted));
                            return;
                        }
                        JobState::Cancelling => return,
                    }
                }

                self.ready.retain(|queued| *queued != id);
                let job = self.jobs.remove(&id).unwrap();
                self.cancel_job(job).await;
            }
            JobEvent::Worker(result) => {
                let Some(mut job) = self.jobs.remove(&id) else {
                    return;
                };

                match (&job.state, result) {
                    (_, Ok(WorkerResult::Finished(result))) => {
                        job.finalize(self.ctx.events.clone(), Ok(result));
                    }
                    (JobState::Cancelling, _) => self.cancel_job(job).await,
                    (JobState::Pausing { .. }, Ok(WorkerResult::Stopped)) => {
                        job.pause(self.ctx.events.clone());
                        self.jobs.insert(id, job);
                    }
                    (JobState::Pausing { .. }, Err(error)) if error.is_retryable() => {
                        if job.attempt >= job.request.config.retries {
                            job.finalize(self.ctx.events.clone(), Err(error));
                            return;
                        }

                        job.attempt += 1;
                        job.pause(self.ctx.events.clone());
                        self.jobs.insert(id, job);
                    }
                    (JobState::Running { .. }, Ok(WorkerResult::Stopped)) => {
                        self.cancel_job(job).await;
                    }
                    (_, Err(error)) if error.is_retryable() => {
                        if job.attempt >= job.request.config.retries {
                            warn!(%id, attempt = job.attempt, retries = job.request.config.retries, error = %error, "Retry limit exceeded; failing job");
                            job.finalize(self.ctx.events.clone(), Err(error));
                            return;
                        }

                        let delay = BACKOFF_STRATEGY.next_delay(job.attempt);
                        let timer = self.delayed.insert(id, delay);
                        job.retry(self.ctx.events.clone(), timer, delay);
                        warn!(%id, attempt = job.attempt, delay_ms = delay.as_millis(), error = %error, "Retryable error; scheduling retry");
                        self.jobs.insert(id, job);
                    }
                    (_, Err(error)) => job.finalize(self.ctx.events.clone(), Err(error)),
                    (_, _) => job.finalize(
                        self.ctx.events.clone(),
                        Err(Error::Unknown("invalid worker state transition".into())),
                    ),
                }
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn try_dispatch(&mut self) {
        while self.workers.len() < self.max_concurrent.get() {
            let Some(id) = self.ready.pop_front() else {
                break;
            };
            if self.ctx.cancel_root.is_cancelled() {
                return;
            }
            self.transition_job(id, JobEvent::Dispatch).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

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

    async fn retryable_error() -> Error {
        reqwest::Client::new()
            .get("http://127.0.0.1:0")
            .send()
            .await
            .unwrap_err()
            .into()
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
        job.state = JobState::Retrying { timer: retry_timer };

        let (ack, result) = oneshot::channel();
        scheduler.transition_job(id, JobEvent::Pause(ack)).await;

        assert!(result.await.unwrap().is_ok());
        assert!(scheduler.delayed.is_empty());
        let job = scheduler.jobs.get(&id).unwrap();
        assert!(matches!(&job.state, JobState::Paused));
    }

    #[tokio::test]
    async fn running_job_pause_notifies_all_waiters_and_can_resume() {
        let (mut scheduler, id, mut download_result) = scheduler_with_job().await;
        let mut events = scheduler.ctx.events.subscribe();
        let cancel_token = {
            let job = scheduler.jobs.get_mut(&id).unwrap();
            let stop = CancellationToken::new();
            job.state = JobState::Running { stop: stop.clone() };
            stop
        };
        let (first_ack, mut first_pause_result) = oneshot::channel();
        let (second_ack, mut second_pause_result) = oneshot::channel();

        scheduler
            .transition_job(id, JobEvent::Pause(first_ack))
            .await;
        scheduler
            .transition_job(id, JobEvent::Pause(second_ack))
            .await;

        assert!(cancel_token.is_cancelled());
        assert_eq!(
            events.recv().await.unwrap().kind(),
            &EventKind::PauseStarted
        );
        assert!(matches!(
            &scheduler.jobs[&id].state,
            JobState::Pausing { waiters } if waiters.len() == 2
        ));
        assert!(matches!(
            first_pause_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second_pause_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        scheduler
            .transition_job(id, JobEvent::Worker(Ok(WorkerResult::Stopped)))
            .await;

        assert!(first_pause_result.await.unwrap().is_ok());
        assert!(second_pause_result.await.unwrap().is_ok());
        assert_eq!(events.recv().await.unwrap().kind(), &EventKind::Paused);
        assert!(matches!(&scheduler.jobs[&id].state, JobState::Paused));
        assert!(matches!(
            download_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let (ack, resume_result) = oneshot::channel();
        scheduler.transition_job(id, JobEvent::Resume(ack)).await;

        assert!(resume_result.await.unwrap().is_ok());
        assert_eq!(events.recv().await.unwrap().kind(), &EventKind::Queued);
        assert!(matches!(&scheduler.jobs[&id].state, JobState::Queued));
        assert_eq!(scheduler.ready.back(), Some(&id));
    }

    #[tokio::test]
    async fn running_pause_reports_worker_failure() {
        let (mut scheduler, id, download_result) = scheduler_with_job().await;
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Running {
            stop: CancellationToken::new(),
        };
        let (ack, pause_result) = oneshot::channel();
        scheduler.transition_job(id, JobEvent::Pause(ack)).await;

        scheduler
            .transition_job(
                id,
                JobEvent::Worker(Err(Error::Unknown("checkpoint failed".into()))),
            )
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

    #[tokio::test]
    async fn retryable_failure_while_pausing_stays_paused() {
        let (mut scheduler, id, mut download_result) = scheduler_with_job().await;
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Running {
            stop: CancellationToken::new(),
        };
        let (ack, pause_result) = oneshot::channel();
        scheduler.transition_job(id, JobEvent::Pause(ack)).await;

        let error = retryable_error().await;
        assert!(error.is_retryable());
        scheduler
            .transition_job(id, JobEvent::Worker(Err(error)))
            .await;

        assert!(pause_result.await.unwrap().is_ok());
        assert!(matches!(&scheduler.jobs[&id].state, JobState::Paused));
        assert_eq!(scheduler.jobs[&id].attempt, 1);
        assert!(matches!(
            download_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn retry_expiry_requeues_job_and_emits_event() {
        let (mut scheduler, id, _download_result) = scheduler_with_job().await;
        let mut events = scheduler.ctx.events.subscribe();
        let timer = scheduler.delayed.insert(id, Duration::ZERO);
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Retrying { timer };

        let expired = scheduler.delayed.next().await.unwrap();
        assert_eq!(expired.into_inner(), id);
        scheduler.transition_job(id, JobEvent::RetryElapsed).await;

        assert!(matches!(&scheduler.jobs[&id].state, JobState::Queued));
        assert_eq!(scheduler.ready.back(), Some(&id));
        assert_eq!(events.recv().await.unwrap().kind(), &EventKind::Queued);
    }

    #[tokio::test]
    async fn cancelling_pausing_job_fails_pause_before_worker_stops() {
        let (mut scheduler, id, mut download_result) = scheduler_with_job().await;
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Running {
            stop: CancellationToken::new(),
        };
        let (ack, pause_result) = oneshot::channel();

        scheduler.transition_job(id, JobEvent::Pause(ack)).await;
        scheduler.transition_job(id, JobEvent::Cancel).await;

        assert!(matches!(&scheduler.jobs[&id].state, JobState::Cancelling));
        assert!(matches!(pause_result.await.unwrap(), Err(Error::Cancelled)));
        assert!(matches!(
            download_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        scheduler
            .transition_job(id, JobEvent::Worker(Ok(WorkerResult::Stopped)))
            .await;

        assert!(matches!(
            download_result.await.unwrap(),
            Err(Error::Cancelled)
        ));
        assert!(!scheduler.jobs.contains_key(&id));
    }

    #[tokio::test]
    async fn running_job_stopped_by_root_is_cancelled() {
        let (mut scheduler, id, download_result) = scheduler_with_job().await;
        let stop = scheduler.ctx.child_token();
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Running { stop };

        scheduler.ctx.cancel_root.cancel();
        scheduler
            .transition_job(id, JobEvent::Worker(Ok(WorkerResult::Stopped)))
            .await;

        assert!(matches!(
            download_result.await.unwrap(),
            Err(Error::Cancelled)
        ));
        assert!(!scheduler.jobs.contains_key(&id));
    }

    #[tokio::test]
    async fn cancelling_job_discards_partial_when_worker_fails() {
        let (mut scheduler, id, download_result) = scheduler_with_job().await;
        let destination = scheduler.jobs[&id].request.destination().to_path_buf();
        let part = destination.with_added_extension("part");
        let manifest = part.with_added_extension("manifest.json");
        tokio::fs::write(&part, b"partial").await.unwrap();
        tokio::fs::write(&manifest, b"manifest").await.unwrap();
        scheduler.jobs.get_mut(&id).unwrap().state = JobState::Running {
            stop: CancellationToken::new(),
        };

        scheduler.transition_job(id, JobEvent::Cancel).await;
        scheduler
            .transition_job(
                id,
                JobEvent::Worker(Err(Error::Unknown("worker failed".into()))),
            )
            .await;

        assert!(matches!(
            download_result.await.unwrap(),
            Err(Error::Cancelled)
        ));
        assert!(!tokio::fs::try_exists(part).await.unwrap());
        assert!(!tokio::fs::try_exists(manifest).await.unwrap());
        assert!(!scheduler.jobs.contains_key(&id));
    }
}
