use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::trace;

use super::{Manifest, manifest_path, part_path};
use crate::error::Result;

/// The `<dest>.part` file plus its durable manifest. Writes always target the
/// `.part`; the final destination only appears via an atomic rename on success.
pub(crate) struct PartFile {
    dest: PathBuf,
    file: File,
    written: u64,
    durable: u64,
    manifest: Manifest,
}

impl PartFile {
    /// Checkpoint (fsync + manifest write) at most every 4 MiB, so a
    /// crash re-downloads at most ~4 MiB. Upgrade path: add a time-based cadence
    /// for very slow links where 4 MiB spans a long wall-clock window.
    const CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;

    /// Open the `.part` for writing at `offset`, truncating any bytes past
    /// `offset`. The caller must pass an `offset <= part_len(dest)` so this only
    /// ever drops an un-checkpointed tail and never zero-extends onto unsynced data.
    pub async fn open(dest: &Path, offset: u64, manifest: Manifest) -> Result<Self> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(part_path(dest))
            .await?;
        file.set_len(offset).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(Self {
            dest: dest.to_path_buf(),
            file,
            written: offset,
            durable: offset,
            manifest,
        })
    }

    /// Sequential append at the current write position.
    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.write_at(self.written, buf).await
    }

    /// Write `bytes` at `offset`. Single-stream callers pass `offset == written`;
    /// a mismatch seeks first (positioned writes for future multi-segment).
    pub async fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if offset != self.written {
            self.file.seek(SeekFrom::Start(offset)).await?;
            self.written = offset;
        }
        self.file.write_all(bytes).await?;
        self.written += bytes.len() as u64;
        if self.written - self.durable >= Self::CHECKPOINT_BYTES {
            self.checkpoint().await?;
        }
        Ok(())
    }

    /// fsync the data, then persist the manifest with the synced offset. The
    /// order matters: `completed_ranges` must never exceed the durable bytes.
    pub async fn checkpoint(&mut self) -> Result<()> {
        if self.written == self.durable {
            return Ok(());
        }
        self.file.sync_data().await?;
        self.manifest.set_contiguous(self.written);
        self.manifest.save(&self.dest).await?;
        self.durable = self.written;
        trace!(downloaded = self.durable, "Checkpointed manifest");
        Ok(())
    }

    /// fsync, atomically rename `.part` -> dest, then remove the manifest.
    pub async fn finalize(self) -> Result<PathBuf> {
        let PartFile { dest, file, .. } = self;
        file.sync_all().await?;
        drop(file);
        fs::rename(part_path(&dest), &dest).await?;
        let _ = fs::remove_file(manifest_path(&dest)).await;
        Ok(dest)
    }

    /// Terminal discard (graceful cancel): remove `.part` and manifest.
    pub async fn discard(self) -> Result<()> {
        let PartFile { dest, file, .. } = self;
        drop(file);
        Self::remove(&dest).await;
        let _ = fs::remove_file(manifest_path(&dest)).await;
        Ok(())
    }

    /// Best-effort removal of the `.part` file for `dest`.
    pub async fn remove(dest: &Path) {
        let _ = fs::remove_file(part_path(dest)).await;
    }
}
