use crate::events::Event;
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Shared runtime context for coordinating downloads. Internal to the crate.
/// Holds the root cancellation token, HTTP client, and global [DownloadEvent]
/// broadcast sender.
/// Cloned and shared across scheduler and workers.
#[derive(Debug)]
pub(crate) struct Context {
    /// Root cancellation token; children inherit via [Context::child_token()].
    pub cancel_root: CancellationToken,
    /// Shared reqwest client reused across attempts.
    pub client: Client,
    /// Global [DownloadEvent] broadcaster (buffered). Slow subscribers may miss events.
    pub events: broadcast::Sender<Event>,
}

impl Context {
    /// Create a new shared Context.
    /// - Creates a root [CancellationToken] and a broadcast channel (capacity 1024).
    /// - Constructs a shared [reqwest::Client].
    pub fn new(cancel_root: CancellationToken) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(1024);
        Arc::new(Self {
            cancel_root,
            client: Client::new(),
            events: tx,
        })
    }

    /// Create a child [CancellationToken] tied to the manager's root token.
    /// Cancelling the root cascades to all children.
    #[inline]
    pub fn child_token(&self) -> CancellationToken {
        self.cancel_root.child_token()
    }
}
