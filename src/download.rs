use crate::{
    error::{Error, Result, ResultExt},
    events::{Event, Progress},
    scheduler::SchedulerCmd,
};
use futures_core::Stream;
use std::path::PathBuf;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
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
    progress: watch::Receiver<Progress>,
    result: oneshot::Receiver<Result<DownloadResult>>,
    cmd_tx: mpsc::Sender<SchedulerCmd>,
}

impl Download {
    pub(crate) fn new(
        id: Uuid,
        events: broadcast::Receiver<Event>,
        progress: watch::Receiver<Progress>,
        result: oneshot::Receiver<Result<DownloadResult>>,
        cmd_tx: mpsc::Sender<SchedulerCmd>,
    ) -> Self {
        Download {
            id,
            events,
            progress,
            result,
            cmd_tx,
        }
    }

    /// Unique identifier for this download, matching [`Event`] IDs.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Subscribe to the latest progress for this download.
    pub fn progress(&self) -> watch::Receiver<Progress> {
        self.progress.clone()
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

    /// Stream of [`Event`] values scoped to this download only.
    ///
    /// Backed by a broadcast channel; lagged consumers may drop messages.
    /// This stream filters events to those whose id matches this handle.
    pub fn events(&self) -> impl Stream<Item = Event> + 'static {
        use tokio_stream::StreamExt as _;

        let download_id = self.id;
        BroadcastStream::new(self.events.resubscribe())
            .filter_map(|result| result.log_warn())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn download(
        result: oneshot::Receiver<Result<DownloadResult>>,
        cmd_tx: mpsc::Sender<SchedulerCmd>,
    ) -> Download {
        let (_event_tx, event_rx) = broadcast::channel(1);
        let (_progress_tx, progress_rx) = watch::channel(Progress::new(0, None));
        Download::new(Uuid::new_v4(), event_rx, progress_rx, result, cmd_tx)
    }

    #[test]
    fn progress_returns_latest_value() {
        let (_event_tx, event_rx) = broadcast::channel(1);
        let (progress_tx, progress_rx) = watch::channel(Progress::new(0, None));
        let (_result_tx, result_rx) = oneshot::channel();
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let download = Download::new(Uuid::new_v4(), event_rx, progress_rx, result_rx, cmd_tx);
        let progress_rx = download.progress();
        let mut progress = Progress::new(0, Some(100));
        progress.add(40);

        progress_tx.send_replace(progress);

        assert_eq!(progress_rx.borrow().bytes_downloaded(), 40);
        assert_eq!(progress_rx.borrow().total_bytes(), Some(100));
    }

    #[tokio::test]
    async fn cancel_waits_for_terminal_cancellation_result() {
        let (result_tx, result_rx) = oneshot::channel();
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let download = download(result_rx, cmd_tx);
        let id = download.id();
        let responder = tokio::spawn(async move {
            let Some(SchedulerCmd::Cancel { id: cancelled_id }) = cmd_rx.recv().await else {
                panic!("expected cancel command");
            };
            assert_eq!(cancelled_id, id);
            let _ = result_tx.send(Err(Error::Cancelled));
        });

        assert!(download.cancel().await.is_ok());
        responder.await.unwrap();
    }
}
