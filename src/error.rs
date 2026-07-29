use std::{path::PathBuf, sync::Arc};

use http::StatusCode;
use thiserror::Error;
use tracing::instrument;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[allow(dead_code)]
pub(crate) trait ResultExt<T, E> {
    fn log_error(self) -> Option<T>;
    fn log_warn(self) -> Option<T>;
    fn log_info(self) -> Option<T>;
    fn log_debug(self) -> Option<T>;
}

impl<T, E: std::error::Error> ResultExt<T, E> for std::result::Result<T, E> {
    fn log_error(self) -> Option<T> {
        self.inspect_err(|error| tracing::error!(%error)).ok()
    }

    fn log_warn(self) -> Option<T> {
        self.inspect_err(|error| tracing::warn!(%error)).ok()
    }

    fn log_info(self) -> Option<T> {
        self.inspect_err(|error| tracing::info!(%error)).ok()
    }

    fn log_debug(self) -> Option<T> {
        self.inspect_err(|error| tracing::debug!(%error)).ok()
    }
}

#[derive(Clone, Error, Debug)]
pub enum Error {
    #[error("Network error: {0}")]
    Network(Arc<str>),
    #[error("HTTP request failed with status {0}")]
    HttpStatus(StatusCode),
    #[error("I/O error: {0}")]
    Io(#[source] Arc<std::io::Error>),
    #[error("Download was cancelled")]
    Cancelled,
    #[error("Retry limit exceeded: {last_error}")]
    RetriesExhausted { last_error: Box<Error> },
    #[error("Download manager has been shut down")]
    ManagerShutdown,
    #[error("File already exists: {path}")]
    FileExists { path: PathBuf },
    #[error("Invalid header value `{value}`: {source}")]
    InvalidHeaderValue {
        value: String,
        #[source]
        source: Arc<http::header::InvalidHeaderValue>,
    },
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Download manager command queue is full")]
    CommandQueueFull,
    #[error("Download manager command channel is closed")]
    CommandChannelClosed,
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl Error {
    /// Classify whether this error should be retried by the scheduler.
    ///
    /// Returns true for transport errors and HTTP 5xx.
    /// Returns false for Cancelled, Io, and other non-transient variants.
    #[instrument(level = "trace", skip(self))]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::HttpStatus(status) => status.is_server_error(),
            Self::Cancelled | Self::Io(_) => false,
            _ => false,
        }
    }
}

impl From<http_client::Error> for Error {
    fn from(error: http_client::Error) -> Self {
        Self::Network(error.to_string().into())
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(_: async_channel::SendError<T>) -> Self {
        Self::CommandChannelClosed
    }
}

impl<T> From<async_channel::TrySendError<T>> for Error {
    fn from(error: async_channel::TrySendError<T>) -> Self {
        match error {
            async_channel::TrySendError::Full(_) => Self::CommandQueueFull,
            async_channel::TrySendError::Closed(_) => Self::CommandChannelClosed,
        }
    }
}
