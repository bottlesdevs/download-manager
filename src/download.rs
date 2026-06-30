use crate::{Error, Event, error::Result, scheduler::SchedulerCmd};
use futures_core::Stream;
use std::path::PathBuf;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

/// Handle for a single download scheduled by DownloadManager.
///
/// Behavior:
/// - Implements Future; awaiting resolves to DownloadResult or Error.
/// - Exposes per-download streams via [Download::progress()] and [Download::events()].
/// - Cancellation is cooperative via [Download::cancel()]; the worker aborts the HTTP request and removes any partial file.
pub struct Download {
    id: Uuid,
    events: broadcast::Receiver<Event>,
    result: oneshot::Receiver<Result<DownloadResult>>,
    cmd_tx: mpsc::Sender<SchedulerCmd>,
}

impl Download {
    pub(crate) fn new(
        id: Uuid,
        events: broadcast::Receiver<Event>,
        result: oneshot::Receiver<Result<DownloadResult>>,
        cmd_tx: mpsc::Sender<SchedulerCmd>,
    ) -> Self {
        Download {
            id,
            events,
            result,
            cmd_tx,
        }
    }

    /// Unique identifier for this download, matching [DownloadEvent] IDs.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Request cancellation and wait for it to take terminal effect.
    ///
    /// Resolves only once the download has reached a terminal state and any
    /// partial file/manifest has been removed (cleanup succeeded). Returns:
    /// - `Ok(())` when the download is terminally cancelled, was already
    ///   finished, or completed before cancellation could win the race.
    /// - `Err(..)` if cleanup failed or the manager was shut down.
    pub async fn cancel(self) -> Result<()> {
        self.cmd_tx
            .send(SchedulerCmd::Cancel { id: self.id })
            .await
            .map_err(Error::from)?;
        match self.result.await.map_err(|_| Error::ManagerShutdown)? {
            Err(Error::Cancelled) => Ok(()),
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Stream of [DownloadEvent] values scoped to this download only.
    ///
    /// Backed by a broadcast channel; lagged consumers may drop messages.
    /// This stream filters events to those whose id matches this handle.
    pub fn events(&self) -> impl Stream<Item = Event> + 'static {
        use tokio_stream::StreamExt as _;

        let download_id = self.id;
        BroadcastStream::new(self.events.resubscribe())
            .filter_map(|res| res.ok())
            .filter(move |event| event.id() == download_id)
    }
}

impl std::future::Future for Download {
    type Output = Result<DownloadResult>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::pin::Pin;
        use std::task::Poll;

        match Pin::new(&mut self.result).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::ManagerShutdown)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct DownloadResult {
    pub path: PathBuf,
    pub bytes_downloaded: u64,
}

#[derive(Debug, Clone)]
/// Remote metadata observed from the download response.
/// Availability depends on server support; fields are None when not provided.
pub struct RemoteInfo {
    pub content_length: Option<u64>,
    pub accept_ranges: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_type: Option<String>,
}
