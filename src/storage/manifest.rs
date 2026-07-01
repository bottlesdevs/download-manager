use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::warn;

use super::manifest_path;
use crate::{Error, Result};

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
        let bytes = fs::read(manifest_path(dest)).await.ok()?;
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
}
