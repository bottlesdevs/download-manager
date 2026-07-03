use std::{path::PathBuf, sync::Arc};
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
    Network(#[source] Arc<reqwest::Error>),
    #[error("I/O error: {0}")]
    Io(#[source] Arc<std::io::Error>),
    #[error("Task join error: {0}")]
    Join(#[source] Arc<tokio::task::JoinError>),
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
        source: Arc<reqwest::header::InvalidHeaderValue>,
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
    /// Returns true for transient reqwest errors (timeout, connect, request) and HTTP 5xx.
    /// If the HTTP status is unavailable, the error is treated as retryable by default.
    /// Returns false for Cancelled, Io, and other non-transient variants.
    #[instrument(level = "trace", skip(self))]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(network_err) => {
                network_err.is_timeout()
                    || network_err.is_connect()
                    || network_err.is_request()
                    || network_err
                        .status()
                        .map(|status_code| status_code.is_server_error())
                        .unwrap_or(true)
            }
            Self::Cancelled | Self::Io(_) => false,
            _ => false,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Network(Arc::new(error))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Join(Arc::new(error))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::CommandChannelClosed
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for Error {
    fn from(error: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => Self::CommandQueueFull,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => Self::CommandChannelClosed,
        }
    }
}
