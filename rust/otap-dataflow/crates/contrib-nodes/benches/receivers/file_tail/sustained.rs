// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sustained-load macro-benchmark for the file-tail receiver.
//!
//! Writes N events/sec across N files while N cores each run a file-tail
//! reading loop. Reports avg/max CPU, RSS, jemalloc, and event throughput.
//!
//! Configuration via environment variables:
//!
//! | Variable        | Default | Description                          |
//! |-----------------|---------|--------------------------------------|
//! | `NUM_FILES`     | 10      | Number of log files to write to      |
//! | `TARGET_EPS`    | 100000  | Target events per second (total)     |
//! | `LINE_SIZE`     | 256     | Approximate log line size in bytes   |
//! | `DURATION_SECS` | 30      | Benchmark duration in seconds        |
//! | `NUM_CORES`     | all     | Number of reader threads             |
//!
//! Example:
//! ```sh
//! NUM_FILES=20 TARGET_EPS=200000 DURATION_SECS=15 \
//!   cargo bench -p otap-df-contrib-nodes --features file-tail-receiver \
//!   --bench file_tail_sustained
//! ```

#![allow(unused_results)]

#[cfg(not(windows))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod load_gen;
mod resource_monitor;

use load_gen::{LoadGenConfig, LoadGenerator};
use resource_monitor::{MonitorConfig, ResourceMonitor};

use otap_df_contrib_nodes::receivers::file_tail::config::{Delimiter, StartPosition};
use otap_df_contrib_nodes::receivers::file_tail::delimiter::DelimiterScanner;
use otap_df_contrib_nodes::receivers::file_tail::encoding::Transcoder;
use otap_df_contrib_nodes::receivers::file_tail::tailer::FileTailer;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Parse an environment variable with a default.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let num_files: usize = env_or("NUM_FILES", 10);
    let target_eps: u64 = env_or("TARGET_EPS", 100_000);
    let line_size: usize = env_or("LINE_SIZE", 256);
    let duration_secs: u64 = env_or("DURATION_SECS", 30);
    let num_cores: usize = env_or(
        "NUM_CORES",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1),
    );

    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║       File-Tail Receiver Sustained Load Benchmark    ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();
    println!(
        "  Config: {num_files} files × {target_eps} eps × {line_size}B lines × {num_cores} cores"
    );
    println!("  Duration: {duration_secs}s");
    println!();

    let dir = tempfile::tempdir().expect("create temp directory");

    // Shared counters.
    let events_written = Arc::new(AtomicU64::new(0));
    let events_received = Arc::new(AtomicU64::new(0));
    let stop_readers = Arc::new(AtomicBool::new(false));
    let per_core_counts: Vec<Arc<AtomicU64>> = (0..num_cores)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // --- Start resource monitor ---
    let monitor = ResourceMonitor::start(MonitorConfig::default(), events_received.clone());

    // --- Start load generator ---
    let load_gen = LoadGenerator::start(
        dir.path(),
        LoadGenConfig {
            num_files,
            target_eps,
            line_size,
        },
        events_written.clone(),
    );

    // Give the load generator a moment to create files and write initial data.
    std::thread::sleep(Duration::from_millis(200));

    // --- Discover files ---
    let mut all_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect();
    all_files.sort();

    println!("  Discovered {} files", all_files.len());
    println!();

    // --- Spawn reader threads (one per core) ---
    let reader_handles: Vec<std::thread::JoinHandle<()>> = (0..num_cores)
        .map(|core_id| {
            // Round-robin: this core owns files where file_index % num_cores == core_id.
            let my_files: Vec<std::path::PathBuf> = all_files
                .iter()
                .enumerate()
                .filter(|(idx, _)| idx % num_cores == core_id)
                .map(|(_, p)| p.clone())
                .collect();

            let stop = stop_readers.clone();
            let received = events_received.clone();
            let core_counter = per_core_counts[core_id].clone();

            std::thread::Builder::new()
                .name(format!("reader-{core_id}"))
                .spawn(move || {
                    // Pin to core if possible.
                    if let Some(cores) = core_affinity::get_core_ids() {
                        if let Some(core) = cores.get(core_id) {
                            core_affinity::set_for_current(*core);
                        }
                    }

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("thread-local runtime");

                    rt.block_on(reader_loop(my_files, stop, received, core_counter));
                })
                .expect("spawn reader thread")
        })
        .collect();

    // --- Run for the configured duration ---
    std::thread::sleep(Duration::from_secs(duration_secs));

    // --- Stop everything ---
    stop_readers.store(true, Ordering::Release);
    let load_stats = load_gen.stop();

    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }

    let report = monitor.stop();

    // --- Print results ---
    let total_received = events_received.load(Ordering::Acquire);
    let total_written = load_stats.total_events;
    let loss = if total_written > 0 {
        (1.0 - (total_received as f64 / total_written as f64)) * 100.0
    } else {
        0.0
    };

    println!("── Results ─────────────────────────────────────────────");
    println!();
    println!("  Events:       written={total_written}  received={total_received}  loss={loss:.2}%");
    println!(
        "  Load Gen:     actual={:.0} eps over {:.1}s",
        load_stats.actual_eps,
        load_stats.duration.as_secs_f64(),
    );
    println!();

    // Per-core breakdown.
    println!("  Per-core records received:");
    for (i, c) in per_core_counts.iter().enumerate() {
        let count = c.load(Ordering::Acquire);
        let file_count = all_files
            .iter()
            .enumerate()
            .filter(|(idx, _)| idx % num_cores == i)
            .count();
        println!("    core {i:>2}: {count:>12} records  ({file_count} files)");
    }
    println!();

    report.print_table("Resource Usage");
}

/// Per-core reader loop: opens tailers for assigned files and reads until stopped.
async fn reader_loop(
    files: Vec<std::path::PathBuf>,
    stop: Arc<AtomicBool>,
    global_counter: Arc<AtomicU64>,
    core_counter: Arc<AtomicU64>,
) {
    let transcoder = Transcoder::new(None, None).expect("transcoder");
    let buf_size = 256 * 1024; // 256KB
    let max_buf_size = 1024 * 1024; // 1MB

    let mut tailers: Vec<FileTailer> = Vec::new();
    let mut scanners: Vec<DelimiterScanner> = Vec::new();
    for path in &files {
        match FileTailer::open(
            path,
            &StartPosition::Beginning,
            None,
            buf_size,
            max_buf_size,
        )
        .await
        {
            Ok(t) => {
                tailers.push(t);
                scanners.push(DelimiterScanner::new(&Delimiter::Newline));
            }
            Err(_) => continue,
        }
    }

    let poll_interval = Duration::from_millis(5);
    let mut local_count: u64 = 0;

    while !stop.load(Ordering::Acquire) {
        let mut any_data = false;
        for (tailer, scanner) in tailers.iter_mut().zip(scanners.iter_mut()) {
            match tailer.read_records(scanner, &transcoder).await {
                Ok((records, bytes_read)) => {
                    let n = records.len() as u64;
                    if n > 0 || bytes_read > 0 {
                        any_data = true;
                    }
                    local_count += n;
                }
                Err(_) => continue,
            }
        }

        // Flush counts periodically to reduce atomic contention.
        if local_count > 0 {
            global_counter.fetch_add(local_count, Ordering::Release);
            core_counter.fetch_add(local_count, Ordering::Release);
            local_count = 0;
        }

        if !any_data {
            tokio::time::sleep(poll_interval).await;
        }
    }

    // Drain remaining data.
    for (tailer, scanner) in tailers.iter_mut().zip(scanners.iter_mut()) {
        if let Ok(records) = tailer.drain(scanner, &transcoder).await {
            let n = records.len() as u64;
            global_counter.fetch_add(n, Ordering::Release);
            core_counter.fetch_add(n, Ordering::Release);
        }
    }
}
