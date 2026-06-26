use derive_builder::Builder;
use reqwest::{
    Url,
    header::{HeaderMap, IntoHeaderName},
};
use std::path::{Path, PathBuf};
use tracing::instrument;
use uuid::Uuid;

/// Immutable description of a single download request.
///
/// Built by [RequestBuilder] and executed by the scheduler. Holds destination,
/// headers and retry policy. Most users should prefer creating
/// requests via [DownloadManager::download_builder()].
#[derive(Clone, Builder)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct Request {
    #[builder(field(ty = "Uuid"))]
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
    /// The value must be a valid HTTP header value; invalid values will panic during parsing.
    pub fn header(mut self, header: impl IntoHeaderName, value: impl AsRef<str>) -> Self {
        self.headers.insert(header, value.as_ref().parse().unwrap());
        self
    }
}

impl Default for DownloadConfig {
    fn default() -> Self {
        DownloadConfigBuilder::default().build().unwrap()
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

    /// Additional headers applied startto both the HEAD probe and the GET request.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl Request {
    pub fn builder(url: Url, destination: impl AsRef<Path>) -> RequestBuilder {
        RequestBuilder {
            id: Uuid::new_v4(),
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
    pub fn user_agent(self, user_agent: impl AsRef<str>) -> Self {
        self.header(reqwest::header::USER_AGENT, user_agent)
    }

    /// Control whether an existing destination file may be overwritten.
    pub fn overwrite(mut self, overwrite: bool) -> Self {
        self.config = self.config.overwrite(overwrite);
        self
    }

    /// Add an HTTP header (e.g., Authorization, Range).
    ///
    /// Note: value must be a valid header value; invalid values cause a panic during build.
    pub fn header(mut self, header: impl IntoHeaderName, value: impl AsRef<str>) -> Self {
        self.config = self.config.header(header, value);
        self
    }

    #[instrument(level = "info", skip(self))]
    pub fn build(self) -> anyhow::Result<Request> {
        let id = self.id;
        let url = self.url.ok_or_else(|| anyhow::anyhow!("URL must be set"))?;
        let destination = self.destination;
        let config = self.config.build()?;

        Ok(Request {
            id,
            url: url.clone(),
            destination: destination.clone(),
            config,
        })
    }
}
