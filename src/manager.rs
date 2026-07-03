use crate::{
    context::Context,
    download::Download,
    error::{Result, ResultExt},
    events::{Event, Progress},
    request::{Request, RequestBuilder},
    scheduler::{Scheduler, SchedulerCmd},
};
use derive_builder::Builder;
use futures_core::Stream;
use reqwest::Url;
use std::{num::NonZeroUsize, path::Path, sync::Arc};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{info, instrument, warn};
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
    scheduler_tx: mpsc::Sender<SchedulerCmd>,
    ctx: Arc<Context>,
    scheduler: JoinHandle<()>,
}

impl Drop for DownloadManager {
    fn drop(&mut self) {
        self.ctx.cancel_root.cancel();
    }
}

impl Default for DownloadManager {
    #[instrument(level = "debug")]
    fn default() -> Self {
        DownloadManager::with_config(DownloadManagerConfig::default())
    }
}

impl DownloadManager {
    /// Create a new builder for DownloadManager.
    ///
    /// You must set a positive max_concurrent on the builder before build().
    /// If you want a sensible default quickly, see [DownloadManager::default()].
    #[instrument(level = "info", skip(config))]
    pub fn with_config(config: DownloadManagerConfig) -> DownloadManager {
        let (cmd_tx, cmd_rx) = mpsc::channel(1024);
        let ctx = Context::new();
        let scheduler = Scheduler::new(config.max_concurrent, ctx.clone(), cmd_rx);
        let scheduler = tokio::spawn(scheduler.run());

        let manager = DownloadManager {
            scheduler_tx: cmd_tx,
            ctx: ctx.clone(),
            scheduler,
        };

        info!(
            max_concurrent = config.max_concurrent,
            "DownloadManager initialized and scheduler started"
        );

        manager
    }

    /// Start a download with default request settings.
    ///
    /// - Returns a [`Download`] handle which is also a future yielding
    ///   [`DownloadResult`](crate::download::DownloadResult) or an error.
    /// - You can observe progress and per-download events from the returned handle.
    /// - Cancellation: call [`Download::cancel()`] on the handle.
    #[instrument(level = "info", skip(self, destination), fields(url = %url))]
    pub fn download(&self, url: Url, destination: impl AsRef<Path>) -> Result<Download> {
        let request = self.download_builder(url, destination).build()?;
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
        let event_rx = self.ctx.events.subscribe();
        let (progress_tx, progress_rx) = watch::channel(Progress::new(0, None));
        let (result_tx, result_rx) = oneshot::channel();

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

    /// Create a [RequestBuilder] to customize a download (headers, retries, overwrite, callbacks).
    ///
    /// Use this if you need non-default behavior or want to hook into progress/event callbacks before start().
    pub fn download_builder(&self, url: Url, destination: impl AsRef<Path>) -> RequestBuilder {
        Request::builder(url, destination)
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
        BroadcastStream::new(self.ctx.events.subscribe()).filter_map(|result| result.log_warn())
    }

    /// Gracefully stop the manager.
    ///
    /// - Cancels all in-flight work ([DownloadManager::cancel_all()]).
    /// - Prevents new tasks from being scheduled and waits for all worker tasks to finish.
    /// Call this before dropping the manager if you need deterministic teardown.
    #[instrument(level = "info", skip(self))]
    pub async fn shutdown(mut self) {
        info!("Shutting down DownloadManager");
        self.ctx.cancel_root.cancel();
        let _ = (&mut self.scheduler).await.log_warn();
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

    #[tokio::test]
    async fn dropping_manager_stops_scheduler() {
        let context = {
            let manager = DownloadManager::default();
            Arc::downgrade(&manager.ctx)
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while context.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduler should release its context after manager drop");
    }

    #[tokio::test]
    async fn concurrency_limit_can_be_changed_at_runtime() {
        let manager = DownloadManager::default();

        manager
            .set_max_concurrent(NonZeroUsize::new(5).unwrap())
            .await
            .unwrap();

        manager.shutdown().await;
    }
}
