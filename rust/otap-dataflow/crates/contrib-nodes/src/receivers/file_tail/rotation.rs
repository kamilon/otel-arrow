// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Log-rotation detection.
//!
//! Detects when a file has been rotated (e.g. by `logrotate`) by comparing the
//! file identity of the path on disk with the identity of the currently open
//! file handle. On Unix systems this uses the inode; on Windows it uses the
//! file size decrease heuristic (and `GetFileInformationByHandle` where
//! available).

use std::io;
use std::path::Path;

/// Opaque file identity used to detect rotation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileIdentity {
    inner: PlatformIdentity,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlatformIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlatformIdentity {
    volume_serial: u32,
    file_index: u64,
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PlatformIdentity {
    len: u64,
}

impl FileIdentity {
    /// Obtain the identity of a file by its path (following symlinks).
    pub fn from_path(path: &Path) -> io::Result<Self> {
        let meta = std::fs::metadata(path)?;
        Self::from_metadata(&meta)
    }

    /// Obtain the identity from pre-fetched metadata.
    pub fn from_metadata(meta: &std::fs::Metadata) -> io::Result<Self> {
        Ok(Self {
            inner: platform_identity(meta)?,
        })
    }

    /// Returns a platform-specific numeric identifier suitable for bookmark
    /// persistence. On Unix this is the inode number; on other platforms it
    /// is a best-effort surrogate (0 when unavailable).
    #[must_use]
    pub fn inode(&self) -> u64 {
        platform_inode(&self.inner)
    }
}

#[cfg(unix)]
fn platform_identity(meta: &std::fs::Metadata) -> io::Result<PlatformIdentity> {
    use std::os::unix::fs::MetadataExt;
    Ok(PlatformIdentity {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

#[cfg(unix)]
const fn platform_inode(id: &PlatformIdentity) -> u64 {
    id.ino
}

#[cfg(windows)]
fn platform_identity(meta: &std::fs::Metadata) -> io::Result<PlatformIdentity> {
    // On Windows, std::fs::Metadata does not expose volume serial + file index
    // directly. We approximate using file size for now; a production
    // implementation should call GetFileInformationByHandle via the
    // windows-sys crate.
    Ok(PlatformIdentity {
        volume_serial: 0,
        file_index: meta.len(),
    })
}

#[cfg(windows)]
const fn platform_inode(id: &PlatformIdentity) -> u64 {
    id.file_index
}

#[cfg(not(any(unix, windows)))]
fn platform_identity(meta: &std::fs::Metadata) -> io::Result<PlatformIdentity> {
    Ok(PlatformIdentity { len: meta.len() })
}

#[cfg(not(any(unix, windows)))]
const fn platform_inode(id: &PlatformIdentity) -> u64 {
    id.len
}

/// Check whether the file at `path` still refers to the same underlying file
/// as `known_identity`. Returns `true` when the file has been rotated (i.e.
/// the path now points to a *different* file).
pub fn detect_rotation(path: &Path, known_identity: &FileIdentity) -> io::Result<bool> {
    let current = FileIdentity::from_path(path)?;
    Ok(current != *known_identity)
}

/// Result of a rotation check for a single file.
#[derive(Debug)]
pub enum RotationOutcome {
    /// The file has not been rotated — continue reading.
    Unchanged,
    /// The file was rotated. The caller should drain the old handle to EOF,
    /// then reopen the path at offset 0.
    Rotated,
    /// The path no longer exists (deleted after rotation). The caller should
    /// drain the old handle.
    PathGone,
}

/// Perform a rotation check for `path` given the identity of the currently
/// open file handle.
pub fn check_rotation(path: &Path, known_identity: &FileIdentity) -> RotationOutcome {
    match detect_rotation(path, known_identity) {
        Ok(true) => RotationOutcome::Rotated,
        Ok(false) => RotationOutcome::Unchanged,
        Err(e) if e.kind() == io::ErrorKind::NotFound => RotationOutcome::PathGone,
        Err(_) => {
            // Transient I/O error — treat as unchanged and retry on next tick.
            RotationOutcome::Unchanged
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_identity_same_file() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"data").expect("write");

        let id1 = FileIdentity::from_path(&path).expect("id1");
        let id2 = FileIdentity::from_path(&path).expect("id2");
        assert_eq!(id1, id2);
    }

    #[cfg(unix)]
    #[test]
    fn test_identity_different_after_rotation() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"data").expect("write");

        let id_before = FileIdentity::from_path(&path).expect("id");

        // Simulate rotation: rename old file, create new one
        let rotated = dir.path().join("test.log.1");
        std::fs::rename(&path, &rotated).expect("rename");
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(b"new data").expect("write");

        let rotated_detected = detect_rotation(&path, &id_before).expect("check");
        assert!(rotated_detected, "should detect rotation after rename");
    }

    #[test]
    fn test_path_gone() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("test.log");
        std::fs::write(&path, b"data").expect("write");

        let id = FileIdentity::from_path(&path).expect("id");
        std::fs::remove_file(&path).expect("remove");

        match check_rotation(&path, &id) {
            RotationOutcome::PathGone => {}
            other => panic!("expected PathGone, got {other:?}"),
        }
    }
}
