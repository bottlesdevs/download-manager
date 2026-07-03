use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tracing::trace;

use super::{Manifest, manifest_path, part_path};
use crate::error::{Result, ResultExt};

/// The `<dest>.part` file plus its durable manifest. Writes always target the
/// `.part`; the final destination only appears via an atomic rename on success.
pub(crate) struct PartFile {
    dest: PathBuf,
    file: File,
    dirty: bool,
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
            dirty: false,
            manifest,
        })
    }

    /// Sequential append at the current write position.
    pub async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf).await?;
        self.dirty = true;
        let written = self.file.stream_position().await?;
        if written.saturating_sub(self.manifest.resume_offset()) >= Self::CHECKPOINT_BYTES {
            self.checkpoint().await?;
        }
        Ok(())
    }

    /// fsync the data, then persist the manifest with the synced offset. The
    /// order matters: `completed_ranges` must never exceed the durable bytes.
    pub async fn checkpoint(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let written = self.file.stream_position().await?;
        self.file.sync_data().await?;
        self.manifest.set_contiguous(written);
        self.manifest.save(&self.dest).await?;
        self.dirty = false;
        trace!(downloaded = written, "Checkpointed manifest");
        Ok(())
    }

    /// fsync, atomically rename `.part` -> dest, then remove the manifest.
    pub async fn finalize(self) -> Result<PathBuf> {
        let PartFile { dest, file, .. } = self;
        file.sync_all().await?;
        drop(file);
        fs::rename(part_path(&dest), &dest).await?;
        let _ = fs::remove_file(manifest_path(&dest)).await.log_debug();
        Ok(dest)
    }
}
