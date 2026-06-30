mod file;
mod manifest;

pub(crate) use file::PartFile;
pub(crate) use manifest::Manifest;

use std::path::{Path, PathBuf};
use tokio::fs;

/// Bytes actually present in the `.part` file (0 if it is absent).
pub(crate) async fn part_len(dest: &Path) -> u64 {
    fs::metadata(part_path(dest))
        .await
        .map(|m| m.len())
        .unwrap_or(0)
}

fn part_path(dest: &Path) -> PathBuf {
    dest.with_added_extension("part")
}

fn manifest_path(dest: &Path) -> PathBuf {
    part_path(dest).with_added_extension("manifest.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Self-check for the crash-resume invariant: a checkpointed prefix survives
    /// a drop, resumes at the recorded offset, and finalizes to the final path.
    #[test]
    fn checkpoint_then_resume_then_finalize() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut dir = std::env::temp_dir();
            dir.push(format!("dm-storage-test-{}", uuid::Uuid::new_v4()));
            let dest = dir.join("file.bin");

            let manifest = Manifest {
                url: "http://example/file".into(),
                etag: Some("\"abc\"".into()),
                total_length: Some(10),
                ..Default::default()
            };

            // Write 5 bytes, checkpoint, drop (simulate a crash before finish).
            {
                let mut part = PartFile::open(&dest, 0, manifest).await.unwrap();
                part.write(b"hello").await.unwrap();
                part.checkpoint().await.unwrap();
            }

            // The manifest records exactly the durable prefix.
            let loaded = Manifest::load(&dest).await.unwrap();
            assert_eq!(loaded.resume_offset(), 5);
            assert!(loaded.validator().is_some());
            assert_eq!(part_len(&dest).await, 5);

            // Resume from the recorded offset, append, finalize.
            let offset = loaded.resume_offset().min(part_len(&dest).await);
            let mut part = PartFile::open(&dest, offset, loaded).await.unwrap();
            part.write(b"world").await.unwrap();
            let path = part.finalize().await.unwrap();

            assert_eq!(fs::read(&path).await.unwrap(), b"helloworld");
            // Finalize cleans up the sidecars.
            assert!(Manifest::load(&dest).await.is_none());
            assert_eq!(part_len(&dest).await, 0);

            let _ = fs::remove_dir_all(&dir).await;
        });
    }
}
