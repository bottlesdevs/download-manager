use crate::{error::Result, events::Event, manager::DownloadManagerConfig};
use reqwest::Client;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Shared runtime context for coordinating downloads. Internal to the crate.
/// Holds the concurrency semaphore, root cancellation token, HTTP client,
/// atomic counters, and the global [DownloadEvent] broadcast sender.
/// Cloned and shared across scheduler and workers.
#[derive(Debug)]
pub(crate) struct Context {
    /// Semaphore limiting concurrent active downloads.
    pub semaphore: Arc<Semaphore>,
    /// Root cancellation token; children inherit via [Context::child_token()].
    pub cancel_root: CancellationToken,
    /// Shared reqwest client reused across attempts.
    pub client: Client,

    // Counters
    /// Number of currently active (running) downloads.
    pub active: AtomicUsize,

    /// Global [DownloadEvent] broadcaster (buffered). Slow subscribers may miss events.
    pub events: broadcast::Sender<Event>,
}

impl Context {
    /// Create a new shared Context.
    /// - Initializes the semaphore with `max_concurrent` permits.
    /// - Creates a root [CancellationToken] and a broadcast channel (capacity 1024).
    /// - Constructs a shared [reqwest::Client].
    pub fn new(config: &DownloadManagerConfig, cancel_root: CancellationToken) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(1024);
        let ctx = Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            cancel_root,
            active: AtomicUsize::new(0),
            client: Client::new(),
            events: tx,
        });
        info!(
            max_concurrent = config.max_concurrent,
            "Context initialized"
        );
        ctx
    }

    /// Create a child [CancellationToken] tied to the manager's root token.
    /// Cancelling the root cascades to all children.
    #[inline]
    pub fn child_token(&self) -> CancellationToken {
        self.cancel_root.child_token()
    }

    pub fn active_guard(self: &Arc<Self>) -> Result<ActiveGuard> {
        let permit = self.semaphore.clone().try_acquire_owned()?;
        self.active.fetch_add(1, Ordering::Relaxed);
        Ok(ActiveGuard {
            ctx: self.clone(),
            _permit: permit,
        })
    }
}

/// RAII guard tracking the acvtive-downloads counter alongside a semaphore permit
pub(crate) struct ActiveGuard {
    ctx: Arc<Context>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.ctx.active.fetch_sub(1, Ordering::Relaxed);
    }
}
