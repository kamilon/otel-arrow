// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Burst-read benchmark for the file-tail receiver.
//!
//! Pre-writes all data to files, then measures how fast N reader cores can
//! consume everything. Eliminates the load-generator bottleneck to reveal
//! the true read-path throughput ceiling.
//!
//! Configuration via environment variables:
//!
//! | Variable        | Default | Description                          |
//! |-----------------|---------|--------------------------------------|
//! | `NUM_FILES`     | 5       | Number of log files                  |
//! | `TOTAL_EVENTS`  | 36000000| Total events pre-written             |
//! | `LINE_SIZE`     | 1024    | Approximate log line size in bytes   |
//! | `NUM_CORES`     | 3       | Number of reader threads             |
//! | `TIMEOUT_SECS`  | 300     | Safety timeout in seconds            |

#![allow(unused_results)]

#[cfg(not(windows))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(windows))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod resource_monitor;

use resource_monitor::{MonitorConfig, ResourceMonitor};

use otap_df_contrib_nodes::receivers::file_tail::config::{Delimiter, StartPosition};
use otap_df_contrib_nodes::receivers::file_tail::delimiter::DelimiterScanner;
use otap_df_contrib_nodes::receivers::file_tail::encoding::Transcoder;
use otap_df_contrib_nodes::receivers::file_tail::tailer::FileTailer;

use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Parse an environment variable with a default.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let num_files: usize = env_or("NUM_FILES", 5);
    let total_events: u64 = env_or("TOTAL_EVENTS", 36_000_000);
    let line_size: usize = env_or("LINE_SIZE", 1024);
    let num_cores: usize = env_or("NUM_CORES", 3);
    let timeout_secs: u64 = env_or("TIMEOUT_SECS", 300);

    let events_per_file = total_events / num_files as u64;
    let actual_total = events_per_file * num_files as u64;

    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║       File-Tail Receiver Burst-Read Benchmark        ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();

    let dir = tempfile::tempdir().expect("create temp directory");

    // ── Phase 1: Pre-write all data ──────────────────────────────────
    println!("  Phase 1: Pre-writing {actual_total} events across {num_files} files ...");
    let template = build_template_line(line_size);
    let bytes_per_line = template.len() + 1; // +1 for newline
    let total_bytes = actual_total as usize * bytes_per_line;

    let prewrite_start = Instant::now();
    let paths: Vec<std::path::PathBuf> = (0..num_files)
        .map(|i| dir.path().join(format!("bench_{i:04}.log")))
        .collect();

    // Write files in parallel (one thread per file).
    let write_handles: Vec<_> = paths
        .iter()
        .cloned()
        .map(|path| {
            let template = template.clone();
            let count = events_per_file;
            std::thread::spawn(move || {
                let mut file = File::create(&path).expect("create file");
                // Write in batches for efficiency.
                let batch_size = 10_000;
                let mut buf = Vec::with_capacity(batch_size as usize * (template.len() + 1));
                let mut remaining = count;
                while remaining > 0 {
                    buf.clear();
                    let n = remaining.min(batch_size);
                    for _ in 0..n {
                        buf.extend_from_slice(&template);
                        buf.push(b'\n');
                    }
                    file.write_all(&buf).expect("write batch");
                    remaining -= n;
                }
                file.flush().expect("flush file");
            })
        })
        .collect();

    for h in write_handles {
        h.join().expect("writer thread panicked");
    }
    let prewrite_elapsed = prewrite_start.elapsed();

    let total_mb = total_bytes as f64 / 1_048_576.0;
    println!(
        "  Pre-write complete: {total_mb:.0} MiB in {:.1}s ({:.0} MiB/s)",
        prewrite_elapsed.as_secs_f64(),
        total_mb / prewrite_elapsed.as_secs_f64(),
    );
    println!();

    // ── Phase 2: Burst read ──────────────────────────────────────────
    println!(
        "  Phase 2: Reading with {num_cores} cores ({num_files} files, {line_size}B lines) ..."
    );
    println!();

    let events_received = Arc::new(AtomicU64::new(0));
    let stop_readers = Arc::new(AtomicBool::new(false));
    let per_core_counts: Vec<Arc<AtomicU64>> = (0..num_cores)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Start resource monitor.
    let monitor = ResourceMonitor::start(MonitorConfig::default(), events_received.clone());

    let read_start = Instant::now();

    // Spawn reader threads.
    let mut all_files = paths.clone();
    all_files.sort();

    let reader_handles: Vec<std::thread::JoinHandle<()>> = (0..num_cores)
        .map(|core_id| {
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

    // Wait until all events are consumed or timeout.
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let received = events_received.load(Ordering::Acquire);
        if received >= actual_total {
            break;
        }
        if Instant::now() > deadline {
            println!("  WARNING: Timeout after {timeout_secs}s — received {received}/{actual_total}");
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let read_elapsed = read_start.elapsed();

    // Stop readers.
    stop_readers.store(true, Ordering::Release);
    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }

    let report = monitor.stop();

    // ── Results ──────────────────────────────────────────────────────
    let total_received = events_received.load(Ordering::Acquire);
    let read_secs = read_elapsed.as_secs_f64();
    let events_per_sec = total_received as f64 / read_secs;
    let bytes_per_sec = total_received as f64 * bytes_per_line as f64 / read_secs;

    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║                    Burst-Read Results                 ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration:");
    println!("    Files:          {num_files}");
    println!("    Events/file:    {events_per_file}");
    println!("    Total events:   {actual_total}");
    println!("    Line size:      {line_size} bytes");
    println!("    Reader cores:   {num_cores}");
    println!();
    println!("  Prewrite:");
    println!("    Total data:     {total_mb:.0} MiB");
    println!(
        "    Write speed:    {:.0} MiB/s",
        total_mb / prewrite_elapsed.as_secs_f64()
    );
    println!(
        "    Write time:     {:.1}s",
        prewrite_elapsed.as_secs_f64()
    );
    println!();
    println!("  Read Performance:");
    println!("    Events read:    {total_received}");
    println!("    Read time:      {read_secs:.3}s");
    println!("    Throughput:     {events_per_sec:.0} events/sec");
    println!(
        "                    {:.1} MiB/s",
        bytes_per_sec / 1_048_576.0
    );
    println!(
        "                    {:.2} GiB/s",
        bytes_per_sec / 1_073_741_824.0
    );
    println!();
    println!("  Per-core breakdown:");
    for (i, c) in per_core_counts.iter().enumerate() {
        let count = c.load(Ordering::Acquire);
        let file_count = all_files
            .iter()
            .enumerate()
            .filter(|(idx, _)| idx % num_cores == i)
            .count();
        println!("    core {i:>2}: {count:>12} events  ({file_count} files)");
    }
    println!();

    report.print_table("Resource Usage (read phase)");
}

/// Per-core reader loop: reads assigned files as fast as possible until stopped.
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
        match FileTailer::open(path, &StartPosition::Beginning, None, buf_size, max_buf_size).await
        {
            Ok(t) => {
                tailers.push(t);
                scanners.push(DelimiterScanner::new(&Delimiter::Newline));
            }
            Err(_) => continue,
        }
    }

    let poll_interval = Duration::from_millis(1);
    let mut local_count: u64 = 0;
    let flush_threshold: u64 = 10_000;

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

        // Batch-flush to reduce atomic contention on the hot path.
        if local_count >= flush_threshold {
            global_counter.fetch_add(local_count, Ordering::Release);
            core_counter.fetch_add(local_count, Ordering::Release);
            local_count = 0;
        }

        if !any_data {
            // Flush before sleeping so the main thread sees progress.
            if local_count > 0 {
                global_counter.fetch_add(local_count, Ordering::Release);
                core_counter.fetch_add(local_count, Ordering::Release);
                local_count = 0;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    // Drain remaining data.
    for (tailer, scanner) in tailers.iter_mut().zip(scanners.iter_mut()) {
        if let Ok(records) = tailer.drain(scanner, &transcoder).await {
            local_count += records.len() as u64;
        }
    }
    if local_count > 0 {
        global_counter.fetch_add(local_count, Ordering::Release);
        core_counter.fetch_add(local_count, Ordering::Release);
    }
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
