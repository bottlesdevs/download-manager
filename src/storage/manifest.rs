use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::warn;

use super::manifest_path;
use crate::error::{Error, Result, ResultExt};

/// A half-open byte range `[start, end)` written to the `.part` file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ByteRange {
    pub start: u64,
    pub end: u64,
}

/// Durable resume metadata written next to the `.part` file.
// Single-stream keeps one contiguous range `[0, N)` via [`set_contiguous`].
// TODO: For Multi-segment add disjoint ranges to the same vec.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub total_length: Option<u64>,
    pub completed_ranges: Vec<ByteRange>,
}

impl Manifest {
    /// Load the manifest sibling for `dest`. A missing, unreadable, or torn
    /// manifest is treated as absent so the caller restarts cleanly.
    pub async fn load(dest: &Path) -> Option<Manifest> {
        let bytes = match fs::read(manifest_path(dest)).await {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
            result => result.log_warn()?,
        };
        match serde_json::from_slice(&bytes) {
            Ok(manifest) => Some(manifest),
            Err(e) => {
                warn!(error = %e, "Ignoring unreadable manifest; restarting clean");
                None
            }
        }
    }

    /// Atomically persist the manifest: write a temp sibling, then rename it over
    /// the real path. A crash now leaves either the old manifest or the new one,
    /// never a torn JSON.
    pub async fn save(&self, dest: &Path) -> Result<()> {
        let path = manifest_path(dest);
        let tmp = path.with_added_extension("tmp");
        let bytes = serde_json::to_vec(self).map_err(|e| Error::Unknown(e.to_string()))?;
        fs::write(&tmp, bytes).await?;
        fs::rename(&tmp, &path).await?;
        Ok(())
    }

    /// Length of the contiguous downloaded prefix starting at byte 0.
    pub fn resume_offset(&self) -> u64 {
        let mut offset = 0u64;
        while let Some(r) = self
            .completed_ranges
            .iter()
            .find(|r| r.start == offset && r.end > offset)
        {
            offset = r.end;
        }
        offset
    }

    /// Replace `completed_ranges` with the single contiguous prefix `[0, written)`.
    pub fn set_contiguous(&mut self, written: u64) {
        self.completed_ranges = if written > 0 {
            vec![ByteRange {
                start: 0,
                end: written,
            }]
        } else {
            Vec::new()
        };
    }

    /// Preferred resume validator (`ETag`, falling back to `Last-Modified`).
    pub fn validator(&self) -> Option<&str> {
        self.etag.as_deref().or(self.last_modified.as_deref())
    }

    pub fn is_resumable_for(&self, url: &str) -> bool {
        self.url == url && self.resume_offset() > 0 && self.validator().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_offset_from_contiguous_prefix() {
        let mut m = Manifest {
            url: "http://x/y".into(),
            ..Default::default()
        };
        assert_eq!(m.resume_offset(), 0);
        m.set_contiguous(40);
        assert_eq!(m.resume_offset(), 40);
    }

    #[test]
    fn resume_offset_stops_at_first_gap_regardless_of_range_order() {
        let manifest = Manifest {
            completed_ranges: vec![
                ByteRange { start: 40, end: 80 },
                ByteRange { start: 0, end: 20 },
                ByteRange {
                    start: 90,
                    end: 100,
                },
                ByteRange { start: 20, end: 40 },
            ],
            ..Default::default()
        };

        assert_eq!(manifest.resume_offset(), 80);
    }

    #[test]
    fn validator_prefers_etag_then_last_modified() {
        let mut manifest = Manifest {
            etag: Some("\"v1\"".into()),
            last_modified: Some("Mon, 01 Jan 2024 00:00:00 GMT".into()),
            ..Default::default()
        };

        assert_eq!(manifest.validator(), Some("\"v1\""));
        manifest.etag = None;
        assert_eq!(manifest.validator(), Some("Mon, 01 Jan 2024 00:00:00 GMT"));
    }

    #[test]
    fn resume_requires_matching_url_prefix_and_validator() {
        let mut manifest = Manifest {
            url: "https://example.com/file".into(),
            etag: Some("\"v1\"".into()),
            ..Default::default()
        };
        manifest.set_contiguous(10);

        assert!(manifest.is_resumable_for("https://example.com/file"));
        assert!(!manifest.is_resumable_for("https://example.com/other"));

        manifest.etag = None;
        assert!(!manifest.is_resumable_for("https://example.com/file"));
    }

    #[tokio::test]
    async fn corrupt_manifest_is_treated_as_absent() {
        let dir = std::env::temp_dir().join(format!("dm-manifest-test-{}", uuid::Uuid::new_v4()));
        let dest = dir.join("file.bin");
        fs::create_dir_all(&dir).await.unwrap();
        fs::write(manifest_path(&dest), b"not-json").await.unwrap();

        assert!(Manifest::load(&dest).await.is_none());

        let _ = fs::remove_dir_all(dir).await;
    }
}
