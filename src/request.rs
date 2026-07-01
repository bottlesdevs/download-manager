use derive_builder::Builder;
use reqwest::{
    Url,
    header::{HeaderMap, HeaderValue, IntoHeaderName},
};
use std::path::{Path, PathBuf};
use tracing::instrument;
use uuid::Uuid;

use crate::{Error, Result};

/// Immutable description of a single download request.
///
/// Built by [RequestBuilder] and executed by the scheduler. Holds destination,
/// headers and retry policy. Most users should prefer creating
/// requests via [DownloadManager::download_builder()].
///
/// `Request` must not implement [`Clone`]: its ID is the scheduler's job key,
/// so enqueuing a clone could replace another job with the same ID. Build a
/// fresh request for each download instead.
#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct Request {
    id: Uuid,
    #[builder(setter(custom))]
    url: Url,
    #[builder(field(ty = "PathBuf"))]
    destination: PathBuf,
    #[builder(field(ty = "DownloadConfigBuilder"))]
    config: DownloadConfig,
}

/// Per-request configuration for retries, overwrite behavior, and headers.
///
/// Behavior
/// - `retries`: maximum retry attempts for retryable network errors (default 3).
/// - `overwrite`: when false, existing destination paths cause FileExists errors.
/// - `headers`: extra HTTP headers (e.g., User-Agent).
#[derive(Debug, Builder, Clone)]
#[builder(pattern = "owned")]
pub struct DownloadConfig {
    #[builder(default = "3")]
    retries: u32,
    #[builder(default = "false")]
    overwrite: bool,
    #[builder(field(ty = "HeaderMap"), setter(custom))]
    headers: HeaderMap,
}

impl DownloadConfigBuilder {
    /// Add an HTTP header to the request configuration.
    ///
    /// The value must be a valid HTTP header value.
    pub fn header(mut self, header: impl IntoHeaderName, value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let value = HeaderValue::from_str(value).map_err(|source| Error::InvalidHeaderValue {
            value: value.to_string(),
            source,
        })?;
        self.headers.insert(header, value);
        Ok(self)
    }
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            retries: 3,
            overwrite: false,
            headers: HeaderMap::new(),
        }
    }
}

impl DownloadConfig {
    /// Maximum retry attempts for retryable network errors.
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Whether an existing destination file may be overwritten.
    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    /// Additional headers applied to the download GET request.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl Request {
    pub fn builder(url: Url, destination: impl AsRef<Path>) -> RequestBuilder {
        RequestBuilder {
            id: None,
            url: Some(url),
            destination: destination.as_ref().to_path_buf(),
            config: DownloadConfigBuilder::default(),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn destination(&self) -> &Path {
        self.destination.as_path()
    }

    pub fn config(&self) -> &DownloadConfig {
        &self.config
    }
}

impl RequestBuilder {
    /// Set the maximum retry attempts for retryable network errors.
    pub fn retries(mut self, retries: u32) -> Self {
        self.config = self.config.retries(retries);
        self
    }

    /// Convenience for setting the User-Agent header.
    pub fn user_agent(self, user_agent: impl AsRef<str>) -> Result<Self> {
        self.header(reqwest::header::USER_AGENT, user_agent)
    }

    /// Control whether an existing destination file may be overwritten.
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.config = self.config.overwrite(overwrite);
        self
    }

    /// Add an HTTP header (e.g., Authorization, Range).
    ///
    /// Note: value must be a valid header value.
    pub fn header(mut self, header: impl IntoHeaderName, value: impl AsRef<str>) -> Result<Self> {
        self.config = self.config.header(header, value)?;
        Ok(self)
    }

    #[instrument(level = "info", skip(self))]
    pub fn build(self) -> Result<Request> {
        let id = Uuid::new_v4();
        let url = self
            .url
            .ok_or_else(|| Error::InvalidRequest("URL must be set".to_string()))?;
        let destination = self.destination;
        let config = self
            .config
            .build()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;

        Ok(Request {
            id,
            url: url.clone(),
            destination: destination.clone(),
            config,
        })
    }
}
