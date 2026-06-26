mod context;
mod download;
mod error;
mod events;
mod request;
mod scheduler;
mod worker;

pub use crate::download::{Download, DownloadResult};
pub use crate::error::{Error, Result};
pub use crate::events::Event;
pub use crate::request::Request;

pub mod prelude {
    pub use crate::{
        download::{Download, DownloadResult},
        error::{Error, Result},
        events::{Event, ProgressTracker},
        request::Request,
    };
}

use crate::{
    context::Context,
    request::RequestBuilder,
    scheduler::{Scheduler, SchedulerCmd},
};
use derive_builder::Builder;
use futures_core::Stream;
use reqwest::Url;
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, instrument, trace, warn};
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
    tracker: TaskTracker,
    shutdown_token: CancellationToken,
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
        let tracker = TaskTracker::new();
        let shutdown_token = CancellationToken::new();
        let ctx = Context::new(&config, shutdown_token.child_token());
        let scheduler =
            Scheduler::new(shutdown_token.clone(), ctx.clone(), tracker.clone(), cmd_rx);

        let manager = DownloadManager {
            scheduler_tx: cmd_tx,
            ctx: ctx.clone(),
            tracker: tracker.clone(),
            shutdown_token,
        };

        tracker.spawn(async move { scheduler.run().await });
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

    fn enqueue(&self, request: Request) -> Result<Download> {
        let id = request.id();
        let event_rx = self.ctx.events.subscribe();
        let (result_tx, result_rx) = oneshot::channel();
        let cancel_token = self.ctx.child_token();

        self.scheduler_tx.try_send(SchedulerCmd::Enqueue {
            request,
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

    /// Request cancellation for a download by ID.
    ///
    /// - No-op if the job is already finished or missing.
    /// - Returns an error only if the internal command channel is unavailable.
    #[instrument(level = "info", skip(self), fields(?id = id))]
    pub async fn cancel(&self, id: Uuid) -> Result<()> {
        match self.scheduler_tx.send(SchedulerCmd::Cancel { id }).await {
            Ok(_) => {
                info!(%id, "Cancel command sent");
                Ok(())
            }
            Err(e) => {
                warn!(%id, error = %e, "Failed to send cancel command");
                Err(e.into())
            }
        }
    }

    /// Number of currently active (running) downloads.
    ///
    /// Does not include queued or delayed retries. Reflects active semaphore permits.
    #[instrument(level = "trace", skip(self))]
    pub fn active_downloads(&self) -> usize {
        let n = self.ctx.active.load(Ordering::Relaxed);
        trace!(active = n, "Active downloads");
        n
    }

    /// Cancel all queued and in-flight downloads managed by this instance.
    ///
    /// This triggers cooperative cancellation for workers and removes partial files.
    #[instrument(level = "info", skip(self))]
    pub fn cancel_all(&self) {
        info!("Cancelling all downloads");
        self.ctx.cancel_all();
    }

    /// Return a child [CancellationToken] tied to the manager's root token.
    #[instrument(level = "trace", skip(self))]
    pub fn child_token(&self) -> CancellationToken {
        self.ctx.child_token()
    }

    /// Subscribe to all [DownloadEvent] notifications across the manager.
    ///
    /// The underlying broadcast channel has a bounded buffer (1024). Slow consumers may lag and
    /// miss events. Consider using [DownloadManager::events()] for a stream that skips lagged messages gracefully.
    #[instrument(level = "debug", skip(self))]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.ctx.events.subscribe()
    }

    /// A fallible-safe stream of global [DownloadEvent] values.
    ///
    /// Internally wraps the broadcast receiver and filters out lagged/closed errors.
    #[instrument(level = "debug", skip(self))]
    pub fn events(&self) -> impl Stream<Item = Event> + 'static {
        BroadcastStream::new(self.ctx.events.subscribe()).filter_map(|r| r.ok())
    }

    /// Gracefully stop the manager.
    ///
    /// - Cancels all in-flight work ([DownloadManager::cancel_all()]).
    /// - Prevents new tasks from being scheduled and waits for all worker tasks to finish.
    /// Call this before dropping the manager if you need deterministic teardown.
    #[instrument(level = "info", skip(self))]
    pub async fn shutdown(&self) {
        info!("Shutting down DownloadManager");
        self.shutdown_token.cancel();
        self.tracker.close();
        self.tracker.wait().await;
        info!("DownloadManager shutdown complete");
    }
}

#[derive(Builder)]
pub struct DownloadManagerConfig {
    #[builder(default = 3, setter(custom))]
    max_concurrent: usize,
}

impl Default for DownloadManagerConfig {
    #[instrument(level = "debug")]
    fn default() -> Self {
        Self { max_concurrent: 3 }
    }
}
