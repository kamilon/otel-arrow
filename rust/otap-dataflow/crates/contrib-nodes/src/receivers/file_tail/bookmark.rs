// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Position bookmarking for durable read-progress tracking across restarts.
//!
//! Each monitored file has a checkpoint consisting of its path, file identity
//! (inode on Unix, file-index on Windows), and the byte offset of the last
//! fully consumed record. Bookmarks are serialised to JSON and flushed to
//! disk atomically (`write → fsync → rename`).

use crate::receivers::file_tail::rotation::FileIdentity;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// Bookmark for a single file — the position that has been fully consumed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileBookmark {
    /// Canonical path of the monitored file.
    pub path: String,
    /// Byte offset past the last consumed record.
    pub offset: u64,
    /// Seconds since epoch when the bookmark was last updated.
    pub last_updated: u64,
    /// Inode on Unix (0 on Windows where we fall back to size-based identity).
    #[serde(default)]
    pub inode: u64,
}

/// On-disk representation: a JSON array of [`FileBookmark`]s.
#[derive(Debug, Default, Serialize, Deserialize)]
struct BookmarkFile {
    version: u32,
    bookmarks: Vec<FileBookmark>,
}

const BOOKMARK_VERSION: u32 = 1;

/// In-memory bookmark store backed by a JSON file on disk.
pub struct BookmarkStore {
    /// Bookmarks keyed by canonical file path.
    entries: HashMap<String, FileBookmark>,
    /// Directory where the bookmark file lives.
    store_path: PathBuf,
}

impl BookmarkStore {
    /// Open (or create) a bookmark store at `dir/file_tail_bookmarks.json`.
    pub fn open(dir: &Path) -> io::Result<Self> {
        let store_path = dir.join("file_tail_bookmarks.json");
        let entries = if store_path.exists() {
            let data = std::fs::read(&store_path)?;
            let bf: BookmarkFile = serde_json::from_slice(&data).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad bookmark file: {e}"),
                )
            })?;
            bf.bookmarks
                .into_iter()
                .map(|b| (b.path.clone(), b))
                .collect()
        } else {
            HashMap::new()
        };
        Ok(Self {
            entries,
            store_path,
        })
    }

    /// Look up the stored offset for a file path.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<&FileBookmark> {
        self.entries.get(path)
    }

    /// Look up the stored offset for a file path, but only if the stored inode
    /// matches the current file identity (i.e. the file has not been replaced).
    /// Returns `None` when there is no bookmark or when the inode differs.
    #[must_use]
    pub fn get_validated(
        &self,
        path: &str,
        current_identity: &FileIdentity,
    ) -> Option<&FileBookmark> {
        self.entries.get(path).filter(|b| {
            let current_inode = current_identity.inode();
            // An inode of 0 means the platform does not support real inodes;
            // fall back to trusting the path-based bookmark.
            b.inode == 0 || current_inode == 0 || b.inode == current_inode
        })
    }

    /// Update (or insert) the bookmark for `path`.
    pub fn update(&mut self, path: &str, offset: u64, identity: &FileIdentity) {
        let inode = identity_inode(identity);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let entry = self.entries.entry(path.to_owned()).or_insert(FileBookmark {
            path: path.to_owned(),
            offset,
            last_updated: now,
            inode,
        });
        entry.offset = offset;
        entry.last_updated = now;
        entry.inode = inode;
    }

    /// Remove the bookmark for a path (e.g. when the file has been deleted).
    pub fn remove(&mut self, path: &str) {
        let _ = self.entries.remove(path);
    }

    /// Flush all bookmarks to disk atomically (`write-tmp → fsync → rename`).
    pub fn flush(&self) -> io::Result<()> {
        let bf = BookmarkFile {
            version: BOOKMARK_VERSION,
            bookmarks: self.entries.values().cloned().collect(),
        };
        let data =
            serde_json::to_vec_pretty(&bf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Ensure the parent directory exists.
        if let Some(parent) = self.store_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = self.store_path.with_extension("tmp");
        std::fs::write(&tmp_path, &data)?;

        // fsync the temp file before renaming for durability.
        let f = std::fs::File::open(&tmp_path)?;
        f.sync_all()?;
        drop(f);

        std::fs::rename(&tmp_path, &self.store_path)?;
        Ok(())
    }
}

/// Extract an inode-like value from a [`FileIdentity`] for serialisation.
fn identity_inode(identity: &FileIdentity) -> u64 {
    identity.inode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut store = BookmarkStore::open(dir.path()).expect("open");

        let id = FileIdentity::from_path(Path::new(file!())).unwrap_or_else(|_| {
            // In CI the source file may not exist; use a dummy.
            let p = dir.path().join("dummy");
            std::fs::write(&p, b"x").expect("write");
            FileIdentity::from_path(&p).expect("id")
        });

        store.update("/var/log/app.log", 4096, &id);
        store.flush().expect("flush");

        let store2 = BookmarkStore::open(dir.path()).expect("reopen");
        let bk = store2.get("/var/log/app.log").expect("should exist");
        assert_eq!(bk.offset, 4096);
    }

    #[test]
    fn test_remove() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut store = BookmarkStore::open(dir.path()).expect("open");

        let p = dir.path().join("dummy");
        std::fs::write(&p, b"x").expect("write");
        let id = FileIdentity::from_path(&p).expect("id");

        store.update("/tmp/file.log", 100, &id);
        store.remove("/tmp/file.log");
        assert!(store.get("/tmp/file.log").is_none());
    }

    #[test]
    fn test_get_validated_same_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut store = BookmarkStore::open(dir.path()).expect("open");

        let p = dir.path().join("samefile.log");
        std::fs::write(&p, b"hello\nworld\n").expect("write");
        let id = FileIdentity::from_path(&p).expect("id");

        store.update(p.to_str().unwrap(), 6, &id);

        // Same file identity → bookmark should be returned.
        let current_id = FileIdentity::from_path(&p).expect("id");
        let bk = store
            .get_validated(p.to_str().unwrap(), &current_id)
            .expect("should match");
        assert_eq!(bk.offset, 6);
    }

    #[test]
    fn test_get_validated_replaced_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let mut store = BookmarkStore::open(dir.path()).expect("open");

        let p = dir.path().join("replaced.log");
        std::fs::write(&p, b"original content\n").expect("write");
        let id = FileIdentity::from_path(&p).expect("id");

        store.update(p.to_str().unwrap(), 17, &id);

        // Replace the file (delete + recreate gives a new inode).
        std::fs::remove_file(&p).expect("remove");
        std::fs::write(&p, b"new content\n").expect("write new");
        let new_id = FileIdentity::from_path(&p).expect("new id");

        // Inode changed → get_validated should return None.
        assert_ne!(
            id.inode(),
            new_id.inode(),
            "new file should have a different inode"
        );
        assert!(
            store.get_validated(p.to_str().unwrap(), &new_id).is_none(),
            "stale bookmark should be rejected when inode differs"
        );
    }
}
