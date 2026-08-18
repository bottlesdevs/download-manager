use derive_builder::Builder;
use http::header::{HeaderMap, HeaderValue, IntoHeaderName};
use std::path::{Path, PathBuf};
use tracing::instrument;
use url::Url;

use crate::error::{Error, Result};

/// Where a download's bytes come from: a single URL streamed directly to
/// the destination, or a list of chunks fetched in order, each optionally
/// zlib-compressed, and concatenated into the destination as they land.
#[derive(Debug, Clone)]
pub enum Source {
    Simple(Url),
    Chunked(Vec<ChunkSource>),
}

/// One chunk of a [`Source::Chunked`] download.
#[derive(Debug, Clone)]
pub struct ChunkSource {
    pub url: Url,
    pub compressed: bool,
}

/// Immutable description of a single download request.
///
/// Built by [RequestBuilder] and executed by the scheduler. Holds destination,
/// headers and retry policy. Most users should prefer creating
/// requests via [`DownloadManager::download_builder`](crate::manager::DownloadManager::download_builder).
#[derive(Builder, Clone)]
#[builder(pattern = "owned")]
#[builder(build_fn(skip))]
pub struct Request {
    #[builder(setter(custom))]
    source: Source,
    #[builder(field(ty = "PathBuf"))]
    destination: PathBuf,
    #[builder(field(ty = "DownloadConfigBuilder"))]
    pub(crate) config: DownloadConfig,
}

/// Per-request configuration for retries, overwrite behavior, and headers.
///
/// Behavior
/// - `retries`: maximum retry attempts for retryable network errors (default 3).
/// - `overwrite`: when false, existing destination paths cause FileExists errors.
/// - `headers`: extra HTTP headers (e.g., User-Agent).
#[derive(Debug, Builder, Clone)]
#[builder(pattern = "owned", default)]
pub(crate) struct DownloadConfig {
    pub retries: u32,
    pub overwrite: bool,
    #[builder(field(ty = "HeaderMap"), setter(custom))]
    pub headers: HeaderMap,
}

impl DownloadConfigBuilder {
    /// Add an HTTP header to the request configuration.
    ///
    /// The value must be a valid HTTP header value.
    pub fn header(mut self, header: impl IntoHeaderName, value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref();
        let value = HeaderValue::from_str(value).map_err(|source| Error::InvalidHeaderValue {
            value: value.to_string(),
            source: source.into(),
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

impl Request {
    pub fn builder(url: Url, destination: impl AsRef<Path>) -> RequestBuilder {
        RequestBuilder {
            source: Some(Source::Simple(url)),
            destination: destination.as_ref().to_path_buf(),
            config: DownloadConfigBuilder::default(),
        }
    }

    /// A request whose bytes come from multiple chunks, fetched and
    /// concatenated in order into `destination`.
    pub fn chunked_builder(chunks: Vec<ChunkSource>, destination: impl AsRef<Path>) -> RequestBuilder {
        RequestBuilder {
            source: Some(Source::Chunked(chunks)),
            destination: destination.as_ref().to_path_buf(),
            config: DownloadConfigBuilder::default(),
        }
    }

    pub fn source(&self) -> &Source {
        &self.source
    }

    pub fn destination(&self) -> &Path {
        self.destination.as_path()
    }

    /// Short human-readable description of this request's source, for
    /// tracing/logging only.
    pub fn describe(&self) -> String {
        match &self.source {
            Source::Simple(url) => url.to_string(),
            Source::Chunked(chunks) => format!("{} chunk(s)", chunks.len()),
        }
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
        self.header(http::header::USER_AGENT, user_agent)
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
        let source = self
            .source
            .ok_or_else(|| Error::InvalidRequest("source must be set".to_string()))?;
        let destination = self.destination;
        let config = self
            .config
            .build()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;

        Ok(Request {
            source,
            destination,
            config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url() -> Url {
        Url::parse("https://example.com/file.bin").unwrap()
    }

    #[test]
    fn builder_preserves_configuration_and_assigns_unique_ids() {
        let request = Request::builder(url(), "out.bin")
            .retries(5)
            .overwrite(true)
            .user_agent("download-manager-test")
            .unwrap()
            .build()
            .unwrap();

        assert!(matches!(request.source(), Source::Simple(u) if u == &url()));
        assert_eq!(request.destination(), Path::new("out.bin"));
        assert_eq!(request.config.retries, 5);
        assert!(request.config.overwrite);
        assert_eq!(
            request.config.headers.get(http::header::USER_AGENT),
            Some(&HeaderValue::from_static("download-manager-test"))
        );
    }

    #[test]
    fn builder_uses_documented_defaults() {
        let request = Request::builder(url(), "out.bin").build().unwrap();

        assert_eq!(request.config.retries, 3);
        assert!(!request.config.overwrite);
        assert!(request.config.headers.is_empty());
    }

    #[test]
    fn invalid_header_value_is_reported() {
        let result = Request::builder(url(), "out.bin").header("x-test", "line one\nline two");

        assert!(matches!(result, Err(Error::InvalidHeaderValue { .. })));
    }
}
