mod job;

use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

use async_broadcast::Sender as BroadcastSender;
use async_channel::{Receiver, Sender};
use async_io::Timer;
use futures_core::future::BoxFuture;
use futures_util::{
    FutureExt, StreamExt,
    future::{Either, pending},
    select_biased,
    stream::FuturesUnordered,
};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use crate::{
    context::Context,
    download::DownloadResult,
    error::{Error, Result},
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
        self.base_delay.mul_f64(factor).min(self.max_delay)
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
        progress_tx: BroadcastSender<Progress>,
        result_tx: Sender<Result<DownloadResult>>,
    },
    Pause {
        id: Uuid,
        ack: Sender<Result<()>>,
    },
    Resume {
        id: Uuid,
        ack: Sender<Result<()>>,
    },
    Cancel {
        id: Uuid,
    },
    CancelAll,
    SetMaxConcurrent {
        max_concurrent: NonZeroUsize,
    },
}

type WorkerFuture = BoxFuture<'static, (Uuid, Result<WorkerResult>)>;
type RetryFuture = BoxFuture<'static, (Uuid, u32)>;

pub(crate) struct Scheduler {
    ctx: Arc<Context>,
    max_concurrent: NonZeroUsize,
    cmd_rx: Receiver<SchedulerCmd>,
    jobs: HashMap<Uuid, Job>,
    ready: VecDeque<Uuid>,
    delayed: FuturesUnordered<RetryFuture>,
    workers: FuturesUnordered<WorkerFuture>,
}

enum Next {
    Command(std::result::Result<SchedulerCmd, async_channel::RecvError>),
    Worker(Option<(Uuid, Result<WorkerResult>)>),
    Retry(Option<(Uuid, u32)>),
    Stop,
}

impl Scheduler {
    #[instrument(level = "info", skip(ctx, cmd_rx))]
    pub fn new(
        max_concurrent: NonZeroUsize,
        ctx: Arc<Context>,
        cmd_rx: Receiver<SchedulerCmd>,
    ) -> Self {
        Self {
            ctx,
            max_concurrent,
            cmd_rx,
            ready: VecDeque::new(),
            delayed: FuturesUnordered::new(),
            jobs: HashMap::new(),
            workers: FuturesUnordered::new(),
        }
    }

    fn emit(&self, id: Uuid, kind: EventKind) {
        let _ = self.ctx.events.try_broadcast(Event::new(id, kind));
    }

    fn schedule(&mut self, job: Job) {
        let request = &job.request;
        let id = job.id();
        self.emit(id, EventKind::Queued);
        debug!(%id, url = %request.url(), destination = ?request.destination(), "Job queued");
        self.jobs.insert(id, job);
        self.ready.push_back(id);
    }

    async fn cancel_job(events: BroadcastSender<Event>, job: Job) {
        match storage::discard_partial(job.request.destination()).await {
            Ok(()) => job.finalize(events, Err(Error::Cancelled)),
            Err(error) => job.finalize(events, Err(error)),
        }
    }

    #[instrument(level = "info", skip(self))]
    pub async fn run(mut self) {
        loop {
            let next = {
                let stopped = self.ctx.cancel_root.cancelled().fuse();
                let command = self.cmd_rx.recv().fuse();
                let worker = if self.workers.is_empty() {
                    Either::Left(pending())
                } else {
                    Either::Right(self.workers.next())
                }
                .fuse();
                let retry = if self.delayed.is_empty() {
                    Either::Left(pending())
                } else {
                    Either::Right(self.delayed.next())
                }
                .fuse();
                futures_util::pin_mut!(stopped, command, worker, retry);

                select_biased! {
                    _ = stopped => Next::Stop,
                    command = command => Next::Command(command),
                    worker = worker => Next::Worker(worker),
                    retry = retry => Next::Retry(retry),
                }
            };

            match next {
                Next::Stop | Next::Command(Err(_)) => break,
                Next::Command(Ok(command)) => self.handle_cmd(command).await,
                Next::Worker(Some((id, result))) => {
                    self.transition_job(id, JobEvent::Worker(result)).await
                }
                Next::Retry(Some((id, attempt))) => {
                    self.transition_job(id, JobEvent::RetryElapsed(attempt))
                        .await
                }
                Next::Worker(None) | Next::Retry(None) => {}
            }
            self.try_dispatch().await;
        }

        self.cmd_rx.close();
        self.handle_cmd(SchedulerCmd::CancelAll).await;

        while let Some((id, result)) = self.workers.next().await {
            self.transition_job(id, JobEvent::Worker(result)).await;
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

                    let stop = self.ctx.cancel_root.child_token();
                    job.state = JobState::Running { stop: stop.clone() };
                    (job.request.clone(), job.progress_tx.clone(), stop)
                };

                self.emit(id, EventKind::Started);
                let client = self.ctx.client.clone();
                info!(%id, "Dispatching job to worker");
                self.workers.push(
                    async move {
                        let result = AssertUnwindSafe(run(request, client, progress_tx, stop))
                            .catch_unwind()
                            .await
                            .unwrap_or_else(|_| {
                                Err(Error::Unknown("Download worker panicked".into()))
                            });
                        (id, result)
                    }
                    .boxed(),
                );
            }
            JobEvent::RetryElapsed(attempt) => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    return;
                };
                if !matches!(&job.state, JobState::Retrying { attempt: current } if *current == attempt)
                {
                    return;
                }

                job.state = JobState::Queued;
                self.ready.push_back(id);
                self.emit(id, EventKind::Queued);
            }
            JobEvent::Pause(ack) => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    let _ = ack.try_send(Err(Error::Unknown(format!("unknown download {id}"))));
                    return;
                };

                match &mut job.state {
                    JobState::Queued => {
                        self.ready.retain(|queued| *queued != id);
                        job.pause(self.ctx.events.clone());
                        let _ = ack.try_send(Ok(()));
                    }
                    JobState::Retrying { .. } => {
                        job.pause(self.ctx.events.clone());
                        let _ = ack.try_send(Ok(()));
                    }
                    JobState::Running { stop } => {
                        stop.cancel();
                        job.state = JobState::Pausing { waiters: vec![ack] };
                        self.emit(id, EventKind::PauseStarted);
                    }
                    JobState::Pausing { waiters } => waiters.push(ack),
                    JobState::Paused => {
                        let _ = ack.try_send(Ok(()));
                    }
                    JobState::Cancelling => {
                        let _ = ack.try_send(Err(Error::InvalidRequest(
                            "download cannot be paused in its current state".into(),
                        )));
                    }
                }
            }
            JobEvent::Resume(ack) => {
                let Some(job) = self.jobs.get_mut(&id) else {
                    let _ = ack.try_send(Err(Error::Unknown(format!("unknown download {id}"))));
                    return;
                };

                match &job.state {
                    JobState::Paused => {
                        job.state = JobState::Queued;
                        self.ready.push_back(id);
                        self.emit(id, EventKind::Queued);
                        let _ = ack.try_send(Ok(()));
                    }
                    JobState::Queued | JobState::Running { .. } | JobState::Retrying { .. } => {
                        let _ = ack.try_send(Ok(()));
                    }
                    JobState::Pausing { .. } | JobState::Cancelling => {
                        let _ = ack.try_send(Err(Error::InvalidRequest(
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
                        JobState::Queued | JobState::Paused | JobState::Retrying { .. } => {}
                        JobState::Running { stop } => {
                            stop.cancel();
                            job.state = JobState::Cancelling;
                            self.emit(id, EventKind::CancellationStarted);
                            return;
                        }
                        JobState::Pausing { waiters } => {
                            waiters.drain(..).for_each(|waiter| {
                                let _ = waiter.try_send(Err(Error::Cancelled));
                            });
                            job.state = JobState::Cancelling;
                            self.emit(id, EventKind::CancellationStarted);
                            return;
                        }
                        JobState::Cancelling => return,
                    }
                }

                self.ready.retain(|queued| *queued != id);
                let job = self.jobs.remove(&id).unwrap();
                Self::cancel_job(self.ctx.events.clone(), job).await;
            }
            JobEvent::Worker(result) => {
                let Some(mut job) = self.jobs.remove(&id) else {
                    return;
                };

                match (&job.state, result) {
                    (_, Ok(WorkerResult::Finished(result))) => {
                        job.finalize(self.ctx.events.clone(), Ok(result));
                    }
                    (JobState::Cancelling, _) => {
                        Self::cancel_job(self.ctx.events.clone(), job).await
                    }
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
                        Self::cancel_job(self.ctx.events.clone(), job).await;
                    }
                    (_, Err(error)) if error.is_retryable() => {
                        if job.attempt >= job.request.config.retries {
                            warn!(%id, attempt = job.attempt, retries = job.request.config.retries, error = %error, "Retry limit exceeded; failing job");
                            job.finalize(self.ctx.events.clone(), Err(error));
                            return;
                        }

                        let delay = BACKOFF_STRATEGY.next_delay(job.attempt);
                        job.retry(self.ctx.events.clone(), delay);
                        let attempt = job.attempt;
                        self.delayed.push(
                            async move {
                                Timer::after(delay).await;
                                (id, attempt)
                            }
                            .boxed(),
                        );
                        warn!(%id, attempt, delay_ms = delay.as_millis(), error = %error, "Retryable error; scheduling retry");
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
}
