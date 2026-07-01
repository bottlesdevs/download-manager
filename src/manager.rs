use crate::{
    Download, Event, Request, Result,
    context::Context,
    error::ResultExt,
    request::RequestBuilder,
    scheduler::{Scheduler, SchedulerCmd},
};
use derive_builder::Builder;
use futures_core::Stream;
use reqwest::Url;
use std::{path::Path, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{debug, info, instrument, warn};
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
    /// - Returns a [Download] handle which is also a Future yielding [DownloadResult] or Error.
    /// - You can stream progress and per-download events from the returned handle.
    /// - Cancellation: call [Download::cancel()] on the handle, or [DownloadManager::cancel(id)].
    #[instrument(level = "info", skip(self, destination), fields(url = %url))]
    pub fn download(&self, url: Url, destination: impl AsRef<Path>) -> Result<Download> {
        let request = self.download_builder(url, destination).build()?;
        self.enqueue(request)
    }

    /// Enqueue a download request.
    ///
    /// - Returns a [Download] handle which is also a Future yielding [DownloadResult] or Error.
    /// - You can stream progress and per-download events from the returned handle.
    /// - Cancellation: call [Download::cancel()] on the handle, or [DownloadManager::cancel(id)].
    #[instrument(level = "info", skip(self, request))]
    pub fn enqueue(&self, request: Request) -> Result<Download> {
        let id = request.id();
        let event_rx = self.ctx.events.subscribe();
        let (result_tx, result_rx) = oneshot::channel();
        let cancel_token = self.ctx.child_token();

        self.scheduler_tx.try_send(SchedulerCmd::Enqueue {
            request: Arc::new(request),
            result_tx,
            cancel_token: cancel_token.clone(),
        })?;

        Ok(Download::new(
            id,
            event_rx,
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

    /// Best-effort attempt to request cancellation for a download by ID.
    ///
    /// - No-op if the job is already finished or missing.
    /// - Returns an error if the internal command channel is unavailable or the buffer is full.
    #[instrument(level = "info", skip(self), fields(?id = id))]
    pub fn try_cancel(&self, id: Uuid) -> Result<()> {
        match self.scheduler_tx.try_send(SchedulerCmd::Cancel { id }) {
            Ok(_) => {
                debug!(%id, "Cancel command enqueued (try_cancel)");
                Ok(())
            }
            Err(e) => {
                warn!(%id, error = %e, "Failed to send cancel command with try_send");
                Err(e.into())
            }
        }
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

    /// A fallible-safe stream of global [DownloadEvent] values.
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
    #[builder(default = 3, setter(custom))]
    pub(crate) max_concurrent: usize,
}

impl Default for DownloadManagerConfig {
    #[instrument(level = "debug")]
    fn default() -> Self {
        Self { max_concurrent: 3 }
    }
}
