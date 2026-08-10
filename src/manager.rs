use crate::{
    context::Context,
    download::Download,
    error::{Result, ResultExt},
    events::{Event, Progress},
    request::Request,
    scheduler::{Scheduler, SchedulerCmd},
};
use async_channel::{Receiver, Sender};
use derive_builder::Builder;
use futures_core::Stream;
use http_client::HttpClient;
use std::{num::NonZeroUsize, path::Path, sync::Arc};
use tracing::{info, instrument};
use url::Url;
use uuid::Uuid;

/// Entry point for scheduling, observing, and cancelling downloads.
///
/// Behavior
/// - Enforces a global concurrency limit across all downloads.
/// - Publishes global DownloadEvent notifications and exposes per-download streams via Download.
///
/// Notes
/// - Events are delivered over a broadcast channel with a bounded buffer; slow consumers can miss events.
/// - Use events() to get a fallible-safe stream that drops lagged messages.
/// - Use shutdown() for a graceful stop: it cancels all work and waits for workers to finish.
pub struct DownloadManager {
    scheduler_tx: Sender<SchedulerCmd>,
    ctx: Arc<Context>,
    done: Receiver<()>,
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        self.ctx.cancel_root.cancel();
    }
}

impl DownloadManager {
    /// Create a manager and start its scheduler on a private thread.
    #[instrument(level = "info", skip(client, config))]
    pub fn new(
        client: Arc<dyn HttpClient>,
        config: DownloadManagerConfig,
    ) -> Result<DownloadManager> {
        let (cmd_tx, cmd_rx) = async_channel::bounded(1024);
        let (done_tx, done) = async_channel::bounded(1);
        let ctx = Context::new(client);
        let scheduler = Scheduler::new(config.max_concurrent, ctx.clone(), cmd_rx);
        let _ = std::thread::Builder::new()
            .name("download-manager".into())
            .spawn(move || {
                async_io::block_on(scheduler.run());
                let _ = done_tx.try_send(());
            })?;

        let manager = DownloadManager {
            scheduler_tx: cmd_tx,
            ctx: ctx.clone(),
            done,
        };

        info!(
            max_concurrent = config.max_concurrent,
            "DownloadManager initialized"
        );

        Ok(manager)
    }

    /// Start a download with default request settings.
    ///
    /// - Returns a [`Download`] handle which is also a future yielding
    ///   [`DownloadResult`](crate::download::DownloadResult) or an error.
    /// - You can observe progress and per-download events from the returned handle.
    /// - Cancellation: call [`Download::cancel()`] on the handle.
    #[instrument(level = "info", skip(self, destination), fields(url = %url))]
    pub fn download(&self, url: Url, destination: impl AsRef<Path>) -> Result<Download> {
        let request = Request::builder(url, destination).build()?;
        self.enqueue(request)
    }

    /// Enqueue a download request.
    ///
    /// - Returns a [`Download`] handle which is also a future yielding
    ///   [`DownloadResult`](crate::download::DownloadResult) or an error.
    /// - You can observe progress and per-download events from the returned handle.
    /// - Cancellation: call [`Download::cancel()`] on the handle.
    #[instrument(level = "info", skip(self, request))]
    pub fn enqueue(&self, request: Request) -> Result<Download> {
        let id = Uuid::new_v4();
        let event_rx = self.ctx.events.new_receiver();
        let (mut progress_tx, progress_rx) = async_broadcast::broadcast(1);
        progress_tx.set_overflow(true);
        let _ = progress_tx.try_broadcast(Progress::new(0, None));
        let (result_tx, result_rx) = async_channel::bounded(1);

        self.scheduler_tx.try_send(SchedulerCmd::Enqueue {
            id,
            request: Arc::new(request),
            progress_tx,
            result_tx,
        })?;

        Ok(Download::new(
            id,
            event_rx,
            progress_rx,
            result_rx,
            self.scheduler_tx.clone(),
        ))
    }

    /// Cancel all queued and in-flight downloads managed by this instance.
    ///
    /// This triggers cooperative cancellation for workers and removes partial files.
    #[instrument(level = "info", skip(self))]
    pub async fn cancel_all(&self) {
        info!("Cancelling all downloads");
        let _ = self
            .scheduler_tx
            .send(SchedulerCmd::CancelAll)
            .await
            .log_warn();
    }

    /// Change the maximum number of downloads that may run concurrently.
    ///
    /// Lowering the limit does not cancel active downloads. The scheduler waits
    /// for enough of them to finish before dispatching more queued work.
    pub async fn set_max_concurrent(&self, max_concurrent: NonZeroUsize) -> Result<()> {
        self.scheduler_tx
            .send(SchedulerCmd::SetMaxConcurrent { max_concurrent })
            .await?;
        Ok(())
    }

    /// A stream of global [`Event`] values.
    ///
    /// Internally wraps the broadcast receiver and filters out lagged/closed errors.
    #[instrument(level = "debug", skip(self))]
    pub fn events(&self) -> impl Stream<Item = Event> + 'static {
        self.ctx.events.new_receiver()
    }

    /// Gracefully stop the manager.
    ///
    /// - Cancels all in-flight work ([DownloadManager::cancel_all()]).
    /// - Prevents new tasks from being scheduled and waits for all worker tasks to finish.
    /// Call this before dropping the manager if you need deterministic teardown.
    #[instrument(level = "info", skip(self))]
    pub async fn shutdown(&self) {
        info!("Shutting down DownloadManager");
        self.ctx.cancel_root.cancel();
        let _ = self.done.recv().await;
        info!("DownloadManager shutdown complete");
    }
}

#[derive(Builder)]
pub struct DownloadManagerConfig {
    #[builder(default = "NonZeroUsize::new(3).unwrap()")]
    pub(crate) max_concurrent: NonZeroUsize,
}

impl Default for DownloadManagerConfig {
    #[instrument(level = "debug")]
    fn default() -> Self {
        Self {
            max_concurrent: NonZeroUsize::new(3).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Response;
    use http_client::{MockClient, body};

    fn mock() -> Arc<dyn HttpClient> {
        Arc::new(MockClient::new(|_| {
            Ok(Response::builder().status(200).body(body([]))?)
        }))
    }

    #[test]
    fn dropping_manager_stops_scheduler() {
        let manager = DownloadManager::new(mock(), DownloadManagerConfig::default()).unwrap();
        let context = Arc::downgrade(&manager.ctx);
        let done = manager.done.clone();

        drop(manager);
        futures_lite::future::block_on(done.recv()).unwrap();

        assert!(context.upgrade().is_none());
    }

    #[test]
    fn concurrency_limit_can_be_changed_at_runtime() {
        futures_lite::future::block_on(async {
            let manager = DownloadManager::new(mock(), DownloadManagerConfig::default()).unwrap();

            manager
                .set_max_concurrent(NonZeroUsize::new(5).unwrap())
                .await
                .unwrap();
            manager.shutdown().await;
        });
    }

    #[test]
    fn downloads_run_without_an_external_executor() {
        futures_lite::future::block_on(async {
            let manager = DownloadManager::new(mock(), DownloadManagerConfig::default()).unwrap();
            let destination =
                std::env::temp_dir().join(format!("dm-manager-test-{}", Uuid::new_v4()));

            let result = manager
                .download(
                    Url::parse("https://example.com/file").unwrap(),
                    &destination,
                )
                .unwrap()
                .await
                .unwrap();

            assert_eq!(result.path, destination);
            manager.shutdown().await;
            std::fs::remove_file(destination).unwrap();
        });
    }
}
