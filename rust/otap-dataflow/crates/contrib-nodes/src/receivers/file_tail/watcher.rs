// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! File-system watcher that discovers and monitors files matching configured
//! glob patterns.
//!
//! Uses [`notify`] for platform-native FS events (inotify / kqueue /
//! ReadDirectoryChanges) with an automatic fallback to polling when the native
//! backend is unavailable.

use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

/// Events produced by the file watcher for the receiver main loop.
#[derive(Debug)]
pub enum WatchEvent {
    /// A file matching the configured globs was created or modified.
    FileModified(PathBuf),
    /// A file matching the configured globs was removed.
    FileRemoved(PathBuf),
}

/// Wraps a platform-native FS watcher that filters events against the
/// configured glob patterns.
pub struct FileWatcher {
    /// Receives raw events from the `notify` backend.
    rx: mpsc::Receiver<notify::Result<Event>>,
    /// The watcher must stay alive for events to flow.
    _watcher: RecommendedWatcher,
    /// Compiled glob set used to filter events.
    globs: GlobSet,
    /// Directories already being watched (to avoid duplicate watches).
    watched_dirs: HashSet<PathBuf>,
    /// Current core ID for round-robin assignment.
    core_id: usize,
    /// Total cores this pipeline runs on.
    num_cores: usize,
    /// All known file paths that passed glob filtering, in discovery order.
    known_files: Vec<PathBuf>,
}

impl FileWatcher {
    /// Create a new watcher for the given glob patterns.
    ///
    /// `core_id` and `num_cores` control round-robin file ownership:
    /// only files where `file_index % num_cores == core_id` are claimed
    /// by this watcher instance.
    pub fn new(
        patterns: &[String],
        core_id: usize,
        num_cores: usize,
        poll_interval_ms: u64,
    ) -> Result<Self, WatcherError> {
        // Build the glob set for filtering.
        let mut builder = GlobSetBuilder::new();
        for p in patterns {
            let g = Glob::new(p).map_err(|e| WatcherError::GlobPattern(e.to_string()))?;
            let _ = builder.add(g);
        }
        let globs = builder
            .build()
            .map_err(|e| WatcherError::GlobPattern(e.to_string()))?;

        // Create the notify watcher with a channel backend.
        let (tx, rx) = mpsc::channel();
        let watcher = RecommendedWatcher::new(
            move |res| {
                // Send the event; ignore errors if the receiver is gone.
                let _ = tx.send(res);
            },
            notify::Config::default().with_poll_interval(Duration::from_millis(poll_interval_ms)),
        )
        .map_err(WatcherError::Notify)?;

        Ok(Self {
            rx,
            _watcher: watcher,
            globs,
            watched_dirs: HashSet::new(),
            core_id,
            num_cores,
            known_files: Vec::new(),
        })
    }

    /// Start watching the parent directories of all glob patterns and
    /// perform an initial scan for existing files.
    ///
    /// Returns the list of file paths owned by this core.
    pub fn initial_scan(&mut self, patterns: &[String]) -> Result<Vec<PathBuf>, WatcherError> {
        let mut dirs = HashSet::new();

        for pattern in patterns {
            // Derive the directory prefix from the glob pattern by taking
            // everything up to the first wildcard character.
            let dir = glob_base_dir(pattern);
            if dir.is_dir() {
                let _ = dirs.insert(dir);
            }
        }

        // Watch each unique directory.
        for dir in &dirs {
            if self.watched_dirs.insert(dir.clone()) {
                self._watcher
                    .watch(dir, RecursiveMode::Recursive)
                    .map_err(WatcherError::Notify)?;
            }
        }

        // Walk directories and collect matching files.
        let mut all_files: Vec<PathBuf> = Vec::new();
        for dir in &dirs {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_file() && self.matches(&path) && !all_files.contains(&path) {
                        all_files.push(path);
                    }
                }
            }
        }

        // Sort for deterministic ordering so round-robin assignment is stable.
        all_files.sort();

        // Assign files to this core via round-robin and record all known files.
        let owned: Vec<PathBuf> = all_files
            .iter()
            .enumerate()
            .filter(|(idx, _)| idx % self.num_cores == self.core_id)
            .map(|(_, p)| p.clone())
            .collect();

        self.known_files = all_files;

        Ok(owned)
    }

    /// Drain pending FS events and return watch events for files owned by
    /// this core.
    pub fn poll_events(&mut self) -> Vec<WatchEvent> {
        let mut events = Vec::new();

        while let Ok(result) = self.rx.try_recv() {
            match result {
                Ok(event) => {
                    for path in &event.paths {
                        if !self.matches(path) {
                            continue;
                        }

                        match event.kind {
                            EventKind::Create(_) | EventKind::Modify(_) => {
                                // Possibly a new file — add to known list and check ownership.
                                if !self.known_files.contains(path) {
                                    self.known_files.push(path.clone());
                                    // Re-sort so future round-robin is stable.
                                    self.known_files.sort();
                                }

                                if self.owns(path) {
                                    events.push(WatchEvent::FileModified(path.clone()));
                                }
                            }
                            EventKind::Remove(_) => {
                                if self.owns(path) {
                                    events.push(WatchEvent::FileRemoved(path.clone()));
                                }
                                self.known_files.retain(|p| p != path);
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => {
                    // Transient watcher error — skip.
                }
            }
        }

        events
    }

    /// Returns `true` if `path` matches the configured glob patterns.
    fn matches(&self, path: &Path) -> bool {
        self.globs.is_match(path)
    }

    /// Returns `true` if this core owns `path` according to round-robin.
    fn owns(&self, path: &Path) -> bool {
        if let Some(idx) = self.known_files.iter().position(|p| p == path) {
            idx % self.num_cores == self.core_id
        } else {
            // Unknown file — tentatively claim if it matches.
            true
        }
    }
}

/// Extract the longest non-glob prefix of a pattern as a directory path.
fn glob_base_dir(pattern: &str) -> PathBuf {
    let mut dir = PathBuf::new();
    for component in Path::new(pattern).components() {
        let s = component.as_os_str().to_string_lossy();
        if s.contains('*') || s.contains('?') || s.contains('[') {
            break;
        }
        dir.push(component);
    }
    if dir.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        dir
    }
}

/// Errors from the file watcher.
#[derive(Debug)]
pub enum WatcherError {
    /// An invalid glob pattern was specified.
    GlobPattern(String),
    /// The underlying `notify` library returned an error.
    Notify(notify::Error),
}

impl std::fmt::Display for WatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GlobPattern(e) => write!(f, "invalid glob pattern: {e}"),
            Self::Notify(e) => write!(f, "file watcher error: {e}"),
        }
    }
}

impl std::error::Error for WatcherError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Notify(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_base_dir() {
        assert_eq!(glob_base_dir("/var/log/*.log"), PathBuf::from("/var/log"));
        assert_eq!(
            glob_base_dir("/var/log/**/*.log"),
            PathBuf::from("/var/log")
        );
        assert_eq!(glob_base_dir("*.log"), PathBuf::from("."));
        assert_eq!(
            glob_base_dir("/absolute/path"),
            PathBuf::from("/absolute/path")
        );
    }

    #[test]
    fn test_initial_scan() {
        let dir = tempfile::tempdir().expect("tmpdir");
        let log_a = dir.path().join("a.log");
        let log_b = dir.path().join("b.log");
        let txt = dir.path().join("c.txt");
        std::fs::write(&log_a, b"a").expect("write");
        std::fs::write(&log_b, b"b").expect("write");
        std::fs::write(&txt, b"c").expect("write");

        let pattern = format!("{}/*.log", dir.path().display());
        let mut watcher = FileWatcher::new(&[pattern.clone()], 0, 1, 250).expect("watcher");
        let files = watcher.initial_scan(&[pattern]).expect("scan");
        assert_eq!(files.len(), 2);
        assert!(files.contains(&log_a));
        assert!(files.contains(&log_b));
    }

    #[test]
    fn test_round_robin_assignment() {
        let dir = tempfile::tempdir().expect("tmpdir");
        for i in 0..4 {
            std::fs::write(dir.path().join(format!("{i}.log")), b"x").expect("write");
        }

        let pattern = format!("{}/*.log", dir.path().display());
        let mut w0 = FileWatcher::new(&[pattern.clone()], 0, 2, 250).expect("w0");
        let mut w1 = FileWatcher::new(&[pattern.clone()], 1, 2, 250).expect("w1");

        let f0 = w0.initial_scan(&[pattern.clone()]).expect("scan0");
        let f1 = w1.initial_scan(&[pattern]).expect("scan1");

        assert_eq!(f0.len(), 2);
        assert_eq!(f1.len(), 2);

        // No overlap
        for path in &f0 {
            assert!(!f1.contains(path), "overlap: {path:?}");
        }
    }
}
