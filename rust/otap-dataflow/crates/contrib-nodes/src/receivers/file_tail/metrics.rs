// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Internal telemetry metrics for the file-tail receiver.

use otap_df_telemetry::instrument::{Counter, Gauge};
use otap_df_telemetry_macros::metric_set;

/// RFC-aligned metrics for the file-tail receiver.
#[metric_set(name = "file_tail.receiver.metrics")]
#[derive(Debug, Default, Clone)]
pub struct FileTailMetrics {
    /// Number of files currently being monitored.
    #[metric(unit = "{file}")]
    pub files_monitored: Gauge<u64>,

    /// Total bytes read across all tailed files.
    #[metric(unit = "By")]
    pub bytes_read: Counter<u64>,

    /// Total records successfully emitted downstream.
    #[metric(unit = "{record}")]
    pub records_emitted: Counter<u64>,

    /// Number of log-rotation events detected.
    #[metric(unit = "{event}")]
    pub rotation_events: Counter<u64>,

    /// Number of errors encountered (I/O, encoding, etc.).
    #[metric(unit = "{error}")]
    pub errors: Counter<u64>,

    /// Number of records that failed to forward downstream.
    #[metric(unit = "{record}")]
    pub records_forward_failed: Counter<u64>,

    /// Number of bookmark flush operations performed.
    #[metric(unit = "{flush}")]
    pub bookmark_flushes: Counter<u64>,
}
