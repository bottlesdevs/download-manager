use std::{sync::Arc, time::Duration};

use async_broadcast::Sender as BroadcastSender;
use async_channel::Sender;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    download::DownloadResult,
    error::{Error, Result},
    events::{Event, EventKind, Progress},
    request::Request,
    worker::WorkerResult,
};

pub(super) enum JobState {
    Queued,
    Running { stop: CancellationToken },
    Retrying { attempt: u32 },
    Pausing { waiters: Vec<Sender<Result<()>>> },
    Paused,
    Cancelling,
}

pub(super) enum JobEvent {
    Dispatch,
    RetryElapsed(u32),
    Pause(Sender<Result<()>>),
    Resume(Sender<Result<()>>),
    Cancel,
    Worker(Result<WorkerResult>),
}

pub(super) struct Job {
    pub(super) id: Uuid,
    pub(super) request: Arc<Request>,
    pub(super) progress_tx: BroadcastSender<Progress>,
    pub(super) attempt: u32,
    pub(super) result: Option<Sender<Result<DownloadResult>>>,
    pub(super) state: JobState,
}

impl Job {
    pub(super) fn id(&self) -> Uuid {
        self.id
    }

    fn resolve_pause_waiters(&mut self, result: Result<()>) {
        let waiters = match &mut self.state {
            JobState::Pausing { waiters } => waiters,
            _ => return,
        };
        waiters.drain(..).for_each(|waiter| {
            let _ = waiter.try_send(result.clone());
        });
    }

    pub(super) fn finalize(
        mut self,
        event_tx: BroadcastSender<Event>,
        result: Result<DownloadResult>,
    ) {
        self.resolve_pause_waiters(match &result {
            Ok(_) => Ok(()),
            Err(error) => Err(error.clone()),
        });

        let kind = match &result {
            Ok(_) => EventKind::Completed,
            Err(Error::Cancelled) => EventKind::Cancelled,
            Err(error) => EventKind::Failed {
                error: error.to_string(),
            },
        };
        let _ = event_tx.try_broadcast(Event::new(self.id(), kind));

        if let Some(result_tx) = self.result {
            let _ = result_tx.try_send(result);
        }
    }

    pub(super) fn pause(&mut self, event_tx: BroadcastSender<Event>) {
        self.resolve_pause_waiters(Ok(()));
        self.state = JobState::Paused;
        let _ = event_tx.try_broadcast(Event::new(self.id(), EventKind::Paused));
    }

    pub(super) fn retry(&mut self, event_tx: BroadcastSender<Event>, delay: Duration) {
        self.attempt += 1;
        self.state = JobState::Retrying {
            attempt: self.attempt,
        };
        let _ = event_tx.try_broadcast(Event::new(
            self.id(),
            EventKind::RetryScheduled {
                attempt: self.attempt,
                next_delay_ms: delay.as_millis() as u64,
            },
        ));
    }
}
