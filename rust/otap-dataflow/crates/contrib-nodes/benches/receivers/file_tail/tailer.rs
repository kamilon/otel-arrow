// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Criterion microbenchmarks for [`FileTailer`].
//!
//! Measures file reading throughput across different line sizes, file sizes,
//! and buffer sizes.

#![allow(unused_results)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otap_df_contrib_nodes::receivers::file_tail::config::{Delimiter, StartPosition};
use otap_df_contrib_nodes::receivers::file_tail::delimiter::DelimiterScanner;
use otap_df_contrib_nodes::receivers::file_tail::encoding::Transcoder;
use otap_df_contrib_nodes::receivers::file_tail::tailer::FileTailer;
use std::io::Write;

/// Write a temp file with `num_records` newline-terminated lines of
/// `line_bytes` each (excluding the newline).
fn write_test_file(
    dir: &std::path::Path,
    line_bytes: usize,
    num_records: usize,
) -> std::path::PathBuf {
    let path = dir.join("bench.log");
    let mut f = std::fs::File::create(&path).expect("create file");
    for i in 0..num_records {
        let prefix = format!("line-{i:010}: ");
        let pad = line_bytes.saturating_sub(prefix.len());
        f.write_all(prefix.as_bytes()).expect("write");
        let padding = vec![b'x'; pad];
        f.write_all(&padding).expect("write");
        f.write_all(b"\n").expect("write");
    }
    f.flush().expect("flush");
    path
}

/// Read all records from a tailer until EOF, returning the total record count.
async fn drain_tailer(tailer: &mut FileTailer, scanner: &mut DelimiterScanner) -> usize {
    let transcoder = Transcoder::new(None, None).expect("transcoder");
    let mut total = 0;
    loop {
        let (records, bytes_read) = tailer
            .read_records(scanner, &transcoder)
            .await
            .expect("read");
        total += records.len();
        if bytes_read == 0 {
            break;
        }
    }
    total
}

fn bench_read_records(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    // Parametric: line_size × file_size_mb × buffer_size_kb
    let line_sizes: &[usize] = &[128, 512, 2048];
    let file_sizes_kb: &[usize] = &[1024, 10_240]; // 1MB, 10MB
    let buffer_sizes_kb: &[usize] = &[32, 256, 1024];

    for &line_size in line_sizes {
        for &file_kb in file_sizes_kb {
            let file_bytes = file_kb * 1024;
            let num_records = file_bytes / (line_size + 1); // +1 for newline
            let actual_file_bytes = num_records * (line_size + 1);

            for &buf_kb in buffer_sizes_kb {
                let buf_size = buf_kb * 1024;

                let mut group =
                    c.benchmark_group(format!("tailer/line_{line_size}B/file_{file_kb}KB"));
                group.throughput(Throughput::Bytes(actual_file_bytes as u64));

                group.bench_with_input(
                    BenchmarkId::new("read", format!("buf_{buf_kb}KB")),
                    &(line_size, num_records, buf_size),
                    |b, &(ls, nr, bs)| {
                        let dir = tempfile::tempdir().expect("tmpdir");
                        let path = write_test_file(dir.path(), ls, nr);
                        b.to_async(&rt).iter(|| {
                            let path = path.clone();
                            async move {
                                let mut tailer = FileTailer::open(
                                    &path,
                                    &StartPosition::Beginning,
                                    None,
                                    bs,
                                    bs * 4,
                                )
                                .await
                                .expect("open");
                                let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
                                drain_tailer(&mut tailer, &mut scanner).await
                            }
                        });
                    },
                );
                group.finish();
            }
        }
    }
}

criterion_group!(benches, bench_read_records);
criterion_main!(benches);
