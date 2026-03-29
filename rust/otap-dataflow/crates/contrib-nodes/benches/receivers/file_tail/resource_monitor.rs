// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Resource monitor for sustained file-tail benchmarks.
//!
//! Spawns a background sampling thread that periodically collects RSS, CPU%,
//! jemalloc stats, and event throughput, then produces a summary report.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A single resource sample.
#[derive(Clone, Debug)]
struct Sample {
    rss_bytes: u64,
    #[cfg(not(windows))]
    jemalloc_allocated: u64,
    cpu_percent: f32,
    events_since_last: u64,
    sample_interval: Duration,
}

/// Summary report of resource usage over a benchmark run.
pub struct ResourceReport {
    /// Average RSS in bytes.
    pub avg_rss_bytes: u64,
    /// Peak RSS in bytes.
    pub max_rss_bytes: u64,
    /// Average jemalloc allocated bytes (0 on Windows).
    pub avg_jemalloc_allocated: u64,
    /// Peak jemalloc allocated bytes (0 on Windows).
    pub max_jemalloc_allocated: u64,
    /// Average CPU utilisation (0-100%).
    pub avg_cpu_percent: f32,
    /// Peak CPU utilisation (0-100%).
    pub max_cpu_percent: f32,
    /// Average events per second (computed from counter deltas).
    pub avg_events_per_sec: f64,
    /// Peak events per second observed in any single sample interval.
    pub max_events_per_sec: f64,
    /// Total wall-clock duration.
    pub duration: Duration,
    /// Number of samples collected.
    pub num_samples: usize,
}

impl ResourceReport {
    /// Print a formatted summary table to stdout.
    pub fn print_table(&self, header: &str) {
        println!();
        println!("── {header} ──────────────────────────────────────────");
        println!(
            "  Duration:     {:.1}s  ({} samples)",
            self.duration.as_secs_f64(),
            self.num_samples,
        );
        println!();
        println!(
            "  Throughput:   avg={:.0} eps   max={:.0} eps",
            self.avg_events_per_sec, self.max_events_per_sec,
        );
        println!(
            "  CPU:          avg={:.1}%      max={:.1}%",
            self.avg_cpu_percent, self.max_cpu_percent,
        );
        println!(
            "  RSS:          avg={:.1} MiB   max={:.1} MiB",
            self.avg_rss_bytes as f64 / 1_048_576.0,
            self.max_rss_bytes as f64 / 1_048_576.0,
        );
        #[cfg(not(windows))]
        println!(
            "  Jemalloc:     avg={:.1} MiB   max={:.1} MiB",
            self.avg_jemalloc_allocated as f64 / 1_048_576.0,
            self.max_jemalloc_allocated as f64 / 1_048_576.0,
        );
        println!("────────────────────────────────────────────────────────");
        println!();
    }
}

/// Configuration for the resource monitor.
pub struct MonitorConfig {
    /// How often to sample (default 100ms).
    pub sample_interval: Duration,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(100),
        }
    }
}

/// A background resource-sampling thread.
pub struct ResourceMonitor {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<ResourceReport>>,
}

impl ResourceMonitor {
    /// Start the resource monitor. `events_counter` should be an atomic counter
    /// incremented by the receiver as records are consumed.
    pub fn start(config: MonitorConfig, events_counter: Arc<AtomicU64>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();

        let handle = std::thread::Builder::new()
            .name("resource-monitor".into())
            .spawn(move || Self::run(config, stop_clone, events_counter))
            .expect("spawn resource-monitor thread");

        Self {
            stop,
            handle: Some(handle),
        }
    }

    /// Stop the monitor and return the summary report.
    pub fn stop(mut self) -> ResourceReport {
        self.stop.store(true, Ordering::Release);
        self.handle
            .take()
            .expect("handle")
            .join()
            .expect("resource-monitor thread panicked")
    }

    fn run(
        config: MonitorConfig,
        stop: Arc<AtomicBool>,
        events_counter: Arc<AtomicU64>,
    ) -> ResourceReport {
        use sysinfo::System;

        let mut sys = System::new();
        let start = Instant::now();
        let mut samples: Vec<Sample> = Vec::new();
        let mut prev_events: u64 = events_counter.load(Ordering::Acquire);

        // Advance jemalloc epoch once at start.
        #[cfg(not(windows))]
        {
            let _ = tikv_jemalloc_ctl::epoch::advance();
        }

        while !stop.load(Ordering::Acquire) {
            std::thread::sleep(config.sample_interval);

            let rss_bytes = memory_stats::memory_stats()
                .map(|m| m.physical_mem as u64)
                .unwrap_or(0);

            #[cfg(not(windows))]
            let jemalloc_allocated = {
                let _ = tikv_jemalloc_ctl::epoch::advance();
                tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64
            };

            sys.refresh_cpu_usage();
            let cpu_percent = sys.global_cpu_usage();

            let current_events = events_counter.load(Ordering::Acquire);
            let events_since_last = current_events.saturating_sub(prev_events);
            prev_events = current_events;

            samples.push(Sample {
                rss_bytes,
                #[cfg(not(windows))]
                jemalloc_allocated,
                cpu_percent,
                events_since_last,
                sample_interval: config.sample_interval,
            });
        }

        let duration = start.elapsed();
        Self::compute_report(samples, duration)
    }

    fn compute_report(samples: Vec<Sample>, duration: Duration) -> ResourceReport {
        if samples.is_empty() {
            return ResourceReport {
                avg_rss_bytes: 0,
                max_rss_bytes: 0,
                avg_jemalloc_allocated: 0,
                max_jemalloc_allocated: 0,
                avg_cpu_percent: 0.0,
                max_cpu_percent: 0.0,
                avg_events_per_sec: 0.0,
                max_events_per_sec: 0.0,
                duration,
                num_samples: 0,
            };
        }

        let n = samples.len() as f64;

        let avg_rss_bytes = (samples.iter().map(|s| s.rss_bytes).sum::<u64>() as f64 / n) as u64;
        let max_rss_bytes = samples.iter().map(|s| s.rss_bytes).max().unwrap_or(0);

        #[cfg(not(windows))]
        let avg_jemalloc_allocated =
            (samples.iter().map(|s| s.jemalloc_allocated).sum::<u64>() as f64 / n) as u64;
        #[cfg(not(windows))]
        let max_jemalloc_allocated = samples
            .iter()
            .map(|s| s.jemalloc_allocated)
            .max()
            .unwrap_or(0);

        #[cfg(windows)]
        let avg_jemalloc_allocated = 0u64;
        #[cfg(windows)]
        let max_jemalloc_allocated = 0u64;

        let avg_cpu_percent = samples.iter().map(|s| s.cpu_percent).sum::<f32>() / n as f32;
        let max_cpu_percent = samples.iter().map(|s| s.cpu_percent).fold(0.0f32, f32::max);

        // Compute events per second from per-sample deltas.
        let eps_values: Vec<f64> = samples
            .iter()
            .map(|s| s.events_since_last as f64 / s.sample_interval.as_secs_f64())
            .collect();
        let avg_events_per_sec = eps_values.iter().sum::<f64>() / n;
        let max_events_per_sec = eps_values.iter().cloned().fold(0.0f64, f64::max);

        ResourceReport {
            avg_rss_bytes,
            max_rss_bytes,
            avg_jemalloc_allocated,
            max_jemalloc_allocated,
            avg_cpu_percent,
            max_cpu_percent,
            avg_events_per_sec,
            max_events_per_sec,
            duration,
            num_samples: samples.len(),
        }
    }
}
