// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Load generator for sustained file-tail benchmarks.
//!
//! Spawns multiple writer threads (one per file), each writing log lines at
//! its proportional share of the target events-per-second rate.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Statistics collected by the load generator after stopping.
pub struct LoadStats {
    /// Total events (lines) written.
    pub total_events: u64,
    /// Actual average events per second achieved.
    pub actual_eps: f64,
    /// Wall-clock duration of the load generation.
    pub duration: Duration,
}

/// Configuration for the load generator.
pub struct LoadGenConfig {
    /// Number of files to write to.
    pub num_files: usize,
    /// Target events per second across all files.
    pub target_eps: u64,
    /// Approximate size in bytes of each log line (excluding newline).
    pub line_size: usize,
}

/// A multi-threaded load generator that appends lines to files at a target rate.
pub struct LoadGenerator {
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<(u64, Duration)>>,
    counter: Arc<AtomicU64>,
}

impl LoadGenerator {
    /// Start the load generator. Creates `config.num_files` files in `dir`,
    /// spawns one writer thread per file, and begins writing immediately.
    pub fn start(dir: &Path, config: LoadGenConfig, events_written: Arc<AtomicU64>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));

        // Pre-create files.
        let paths: Vec<PathBuf> = (0..config.num_files)
            .map(|i| dir.join(format!("bench_{i:04}.log")))
            .collect();
        for p in &paths {
            let _ = File::create(p);
        }

        let template = Arc::new(build_template_line(config.line_size));

        // Each file gets an equal share of the target EPS.
        let per_file_eps = (config.target_eps as f64 / config.num_files as f64).ceil() as u64;

        let handles: Vec<_> = paths
            .into_iter()
            .enumerate()
            .map(|(i, path)| {
                let stop = stop.clone();
                let counter = events_written.clone();
                let template = template.clone();
                std::thread::Builder::new()
                    .name(format!("load-gen-{i}"))
                    .spawn(move || writer_thread(path, per_file_eps, &template, stop, counter))
                    .expect("spawn writer thread")
            })
            .collect();

        Self {
            stop,
            handles,
            counter: events_written,
        }
    }

    /// Stop the generator and return collected statistics.
    pub fn stop(self) -> LoadStats {
        self.stop.store(true, Ordering::Release);

        let mut total: u64 = 0;
        let mut max_duration = Duration::ZERO;
        for h in self.handles {
            let (count, dur) = h.join().expect("writer thread panicked");
            total += count;
            if dur > max_duration {
                max_duration = dur;
            }
        }

        // Use the shared counter as the authoritative total (may include
        // a few more events flushed between our stop signal and thread exit).
        let counter_total = self.counter.load(Ordering::Acquire);
        let final_total = counter_total.max(total);

        LoadStats {
            total_events: final_total,
            actual_eps: final_total as f64 / max_duration.as_secs_f64(),
            duration: max_duration,
        }
    }
}

/// Per-file writer loop. Writes at `target_eps` events/sec to a single file.
fn writer_thread(
    path: PathBuf,
    target_eps: u64,
    template: &[u8],
    stop: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
) -> (u64, Duration) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open file for append");

    let start = Instant::now();
    let mut total: u64 = 0;

    let tick = Duration::from_millis(1);
    let events_per_tick = (target_eps as f64 / 1000.0).ceil() as u64;
    let mut next_tick = start + tick;

    while !stop.load(Ordering::Acquire) {
        for _ in 0..events_per_tick {
            let _ = file.write_all(template);
            let _ = file.write_all(b"\n");
            total += 1;
        }
        let _ = file.flush();
        counter.fetch_add(events_per_tick, Ordering::Release);

        let now = Instant::now();
        if now < next_tick {
            std::thread::sleep(next_tick - now);
        }
        next_tick += tick;
    }

    (total, start.elapsed())
}

/// Build a log line template of approximately `size` bytes.
fn build_template_line(size: usize) -> Vec<u8> {
    let prefix = b"2026-03-28T00:00:00.000Z INFO benchmark payload: ";
    let mut line = Vec::with_capacity(size);
    line.extend_from_slice(prefix);
    let pad = size.saturating_sub(prefix.len());
    line.resize(line.len() + pad, b'X');
    line
}
