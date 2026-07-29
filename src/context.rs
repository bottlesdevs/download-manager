use std::sync::Arc;

use async_broadcast::{InactiveReceiver, Sender};
use http_client::HttpClient;
use tokio_util::sync::CancellationToken;

use crate::events::Event;

/// Shared runtime context for coordinating downloads. Internal to the crate.
/// Holds the root cancellation token, HTTP client, and global [DownloadEvent]
/// broadcast sender.
/// Cloned and shared across scheduler and workers.
pub(crate) struct Context {
    pub cancel_root: CancellationToken,
    pub client: Arc<dyn HttpClient>,
    pub events: Sender<Event>,
    _events_guard: InactiveReceiver<Event>,
}

impl Context {
    pub fn new(client: Arc<dyn HttpClient>) -> Arc<Self> {
        let (mut events, receiver) = async_broadcast::broadcast(1024);
        events.set_overflow(true);
        events.set_await_active(false);
        Arc::new(Self {
            cancel_root: CancellationToken::new(),
            client,
            events,
            _events_guard: receiver.deactivate(),
        })
    }
}
