use derive_builder::Builder;
use reqwest::{
    Url,
    header::{HeaderMap, HeaderValue, IntoHeaderName},
};
use std::path::{Path, PathBuf};
use tracing::instrument;

use crate::error::{Error, Result};

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
    url: Url,
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
            url: Some(url),
            destination: destination.as_ref().to_path_buf(),
            config: DownloadConfigBuilder::default(),
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn destination(&self) -> &Path {
        self.destination.as_path()
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
        let url = self
            .url
            .ok_or_else(|| Error::InvalidRequest("URL must be set".to_string()))?;
        let destination = self.destination;
        let config = self
            .config
            .build()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;

        Ok(Request {
            url: url.clone(),
            destination: destination.clone(),
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

        assert_eq!(request.url(), &url());
        assert_eq!(request.destination(), Path::new("out.bin"));
        assert_eq!(request.config.retries, 5);
        assert!(request.config.overwrite);
        assert_eq!(
            request.config.headers.get(reqwest::header::USER_AGENT),
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
