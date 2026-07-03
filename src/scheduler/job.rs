use std::{sync::Arc, time::Duration};

use tokio::sync::{broadcast, oneshot, watch};
use tokio_util::{sync::CancellationToken, time::delay_queue::Key};
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
    Running {
        stop: CancellationToken,
    },
    Retrying {
        timer: Key,
    },
    Pausing {
        waiters: Vec<oneshot::Sender<Result<()>>>,
    },
    Paused,
    Cancelling,
}

pub(super) enum JobEvent {
    Dispatch,
    RetryElapsed,
    Pause(oneshot::Sender<Result<()>>),
    Resume(oneshot::Sender<Result<()>>),
    Cancel,
    Worker(Result<WorkerResult>),
}

pub(super) struct Job {
    pub(super) id: Uuid,
    pub(super) request: Arc<Request>,
    pub(super) progress_tx: watch::Sender<Progress>,
    pub(super) attempt: u32,
    pub(super) result: Option<oneshot::Sender<Result<DownloadResult>>>,
    pub(super) state: JobState,
}

impl Job {
    pub(super) fn id(&self) -> Uuid {
        self.id
    }

    pub(super) fn resolve_pause_waiters(&mut self, result: Result<()>) {
        let waiters = match &mut self.state {
            JobState::Pausing { waiters } => waiters,
            _ => return,
        };
        waiters.drain(..).for_each(|waiter| {
            let _ = waiter.send(result.clone());
        });
    }

    fn send_result(mut self, result: Result<DownloadResult>) {
        self.resolve_pause_waiters(result.clone().map(|_| ()));
        if let Some(result_tx) = self.result {
            let _ = result_tx.send(result);
        }
    }

    pub(super) fn fail(self, event_tx: broadcast::Sender<Event>, error: Error) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::Failed {
                error: error.to_string(),
            },
        ));
        self.send_result(Err(error));
    }

    pub(super) fn finish(self, event_tx: broadcast::Sender<Event>, result: DownloadResult) {
        let _ = event_tx.send(Event::new(self.id(), EventKind::Completed));
        self.send_result(Ok(result))
    }

    pub(super) fn retry(&self, event_tx: broadcast::Sender<Event>, delay: Duration) {
        let _ = event_tx.send(Event::new(
            self.id(),
            EventKind::RetryScheduled {
                attempt: self.attempt,
                next_delay_ms: delay.as_millis() as u64,
            },
        ));
    }

    pub(super) fn cancel(self, event_tx: broadcast::Sender<Event>) {
        let _ = event_tx.send(Event::new(self.id(), EventKind::Cancelled));
        self.send_result(Err(Error::Cancelled))
    }
}
