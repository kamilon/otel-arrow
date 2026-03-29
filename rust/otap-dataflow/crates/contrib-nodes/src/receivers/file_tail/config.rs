// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration types for the file-tail receiver.

use serde::Deserialize;

/// Default read buffer size per file (32 KiB).
pub const DEFAULT_BUFFER_SIZE: usize = 32 * 1024;

/// Default maximum buffer size per file (1 MiB).
pub const DEFAULT_MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// Default poll interval in milliseconds when native FS events are unavailable.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

/// Default maximum number of concurrently open file handles.
pub const DEFAULT_MAX_OPEN_FILES: usize = 256;

/// Default maximum number of records per batch.
pub const DEFAULT_BATCH_MAX_SIZE: usize = 100;

/// Default maximum batch accumulation duration in milliseconds.
pub const DEFAULT_BATCH_MAX_DURATION_MS: u64 = 100;

/// Default bookmark flush interval in milliseconds.
pub const DEFAULT_BOOKMARK_FLUSH_INTERVAL_MS: u64 = 1000;

/// Default rotation check interval in milliseconds.
pub const DEFAULT_ROTATION_CHECK_INTERVAL_MS: u64 = 1000;

/// Top-level configuration for the file-tail receiver.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Glob patterns for file paths to monitor (e.g. `["/var/log/*.log"]`).
    pub paths: Vec<String>,

    /// Initial read buffer size in bytes per file. Grows as needed up to
    /// `max_buffer_size`. Regrown buffers are kept to avoid re-allocation.
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,

    /// Maximum buffer size in bytes. A single record longer than this is
    /// truncated.
    #[serde(default = "default_max_buffer_size")]
    pub max_buffer_size: usize,

    /// Fallback poll interval in milliseconds when platform-native file-system
    /// notifications are unavailable.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Maximum number of concurrently open file handles. When the limit is
    /// reached the least-recently-read file handle is evicted and re-opened
    /// on demand.
    #[serde(default = "default_max_open_files")]
    pub max_open_files: usize,

    /// Record delimiter strategy.
    #[serde(default)]
    pub delimiter: Delimiter,

    /// Where to start reading newly discovered files.
    #[serde(default)]
    pub start_position: StartPosition,

    /// Source file encoding (e.g. `"utf-8"`, `"latin1"`, `"shift_jis"`).
    /// When omitted the receiver assumes UTF-8.
    #[serde(default)]
    pub source_encoding: Option<String>,

    /// Target encoding for emitted records.
    /// When omitted, output is UTF-8.
    #[serde(default)]
    pub target_encoding: Option<String>,

    /// Bookmark (position checkpoint) configuration.
    #[serde(default)]
    pub bookmark: BookmarkConfig,

    /// Log-rotation detection configuration.
    #[serde(default)]
    pub rotation: RotationConfig,

    /// Docker container log format settings (future).
    #[serde(default)]
    pub docker: Option<DockerConfig>,

    /// Batching configuration controlling how records are accumulated before
    /// being sent downstream.
    #[serde(default)]
    pub batch: BatchConfig,
}

/// How individual records are delimited within a file.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Delimiter {
    /// Lines terminated by `\n` (handles `\r\n` transparently).
    #[default]
    Newline,
    /// CSV rows — newline-delimited but respecting quoted fields.
    Csv,
    /// TSV rows — same as CSV but tab-separated.
    Tsv,
    /// One JSON object per record (brace-balanced).
    Json,
    /// Arbitrary byte sequence used as separator.
    Custom(String),
}

/// Where to begin reading a file the first time it is discovered and no
/// bookmark exists.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// Start from the beginning of the file.
    Beginning,
    /// Wait for new data appended after discovery.
    #[default]
    End,
}

/// Settings for position bookmarking across process restarts.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookmarkConfig {
    /// Whether bookmarking is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often (ms) to persist bookmarks to durable storage.
    #[serde(default = "default_bookmark_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

impl Default for BookmarkConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_interval_ms: DEFAULT_BOOKMARK_FLUSH_INTERVAL_MS,
        }
    }
}

/// Settings for log-rotation detection.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RotationConfig {
    /// Whether rotation detection is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// How often (ms) to check for file rotation.
    #[serde(default = "default_rotation_check_interval_ms")]
    pub check_interval_ms: u64,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_ms: DEFAULT_ROTATION_CHECK_INTERVAL_MS,
        }
    }
}

/// Configuration for batching records before emission.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    /// Maximum number of records per batch before flushing.
    #[serde(default = "default_batch_max_size")]
    pub max_size: usize,

    /// Maximum time in milliseconds to accumulate records before flushing.
    #[serde(default = "default_batch_max_duration_ms")]
    pub max_duration_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_BATCH_MAX_SIZE,
            max_duration_ms: DEFAULT_BATCH_MAX_DURATION_MS,
        }
    }
}

/// Docker container log driver format (future).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DockerConfig {
    /// Which Docker log driver format to parse.
    pub format: DockerLogFormat,
}

/// Supported Docker logging driver formats.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DockerLogFormat {
    /// Docker default JSON-file driver: `{"log":"...","stream":"...","time":"..."}`.
    JsonFile,
    /// Journald driver (future).
    Journald,
    /// Fluentd driver (future).
    Fluentd,
    /// Syslog driver (future).
    Syslog,
}

// --- serde default helpers ---

const fn default_true() -> bool {
    true
}

const fn default_buffer_size() -> usize {
    DEFAULT_BUFFER_SIZE
}

const fn default_max_buffer_size() -> usize {
    DEFAULT_MAX_BUFFER_SIZE
}

const fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

const fn default_max_open_files() -> usize {
    DEFAULT_MAX_OPEN_FILES
}

const fn default_batch_max_size() -> usize {
    DEFAULT_BATCH_MAX_SIZE
}

const fn default_batch_max_duration_ms() -> u64 {
    DEFAULT_BATCH_MAX_DURATION_MS
}

const fn default_bookmark_flush_interval_ms() -> u64 {
    DEFAULT_BOOKMARK_FLUSH_INTERVAL_MS
}

const fn default_rotation_check_interval_ms() -> u64 {
    DEFAULT_ROTATION_CHECK_INTERVAL_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let json = serde_json::json!({
            "paths": ["/var/log/*.log"]
        });
        let cfg: Config = serde_json::from_value(json).expect("should parse minimal config");
        assert_eq!(cfg.buffer_size, DEFAULT_BUFFER_SIZE);
        assert_eq!(cfg.max_buffer_size, DEFAULT_MAX_BUFFER_SIZE);
        assert_eq!(cfg.max_open_files, DEFAULT_MAX_OPEN_FILES);
        assert!(cfg.bookmark.enabled);
        assert!(cfg.rotation.enabled);
        assert!(matches!(cfg.delimiter, Delimiter::Newline));
        assert!(matches!(cfg.start_position, StartPosition::End));
    }

    #[test]
    fn test_full_config() {
        let json = serde_json::json!({
            "paths": ["/var/log/*.log", "/tmp/app.log"],
            "buffer_size": 65536,
            "max_buffer_size": 2097152,
            "poll_interval_ms": 500,
            "max_open_files": 128,
            "delimiter": "csv",
            "start_position": "beginning",
            "source_encoding": "latin1",
            "target_encoding": "utf-8",
            "bookmark": {
                "enabled": true,
                "flush_interval_ms": 2000
            },
            "rotation": {
                "enabled": false,
                "check_interval_ms": 5000
            },
            "batch": {
                "max_size": 200,
                "max_duration_ms": 50
            }
        });
        let cfg: Config = serde_json::from_value(json).expect("should parse full config");
        assert_eq!(cfg.paths.len(), 2);
        assert_eq!(cfg.buffer_size, 65536);
        assert_eq!(cfg.max_open_files, 128);
        assert!(matches!(cfg.delimiter, Delimiter::Csv));
        assert!(matches!(cfg.start_position, StartPosition::Beginning));
        assert_eq!(cfg.source_encoding.as_deref(), Some("latin1"));
        assert!(!cfg.rotation.enabled);
    }

    #[test]
    fn test_deny_unknown_fields() {
        let json = serde_json::json!({
            "paths": ["/var/log/*.log"],
            "unknown_field": true
        });
        let result = serde_json::from_value::<Config>(json);
        assert!(result.is_err());
    }
}
