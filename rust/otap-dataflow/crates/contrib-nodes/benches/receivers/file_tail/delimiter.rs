// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Criterion microbenchmarks for [`DelimiterScanner`].
//!
//! Measures scanning throughput across delimiter variants and buffer sizes.

#![allow(unused_results)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otap_df_contrib_nodes::receivers::file_tail::config::Delimiter;
use otap_df_contrib_nodes::receivers::file_tail::delimiter::{DelimiterScanner, ScanResult};

/// Generate a buffer containing `num_records` newline-terminated records, each
/// roughly `line_bytes` long (padding with 'x').
fn make_newline_buf(line_bytes: usize, num_records: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(num_records * (line_bytes + 1));
    for i in 0..num_records {
        let prefix = format!("record-{i:08}: ");
        let pad = line_bytes.saturating_sub(prefix.len());
        buf.extend_from_slice(prefix.as_bytes());
        buf.resize(buf.len() + pad, b'x');
        buf.push(b'\n');
    }
    buf
}

/// Generate a buffer of CSV rows: three quoted fields per row.
fn make_csv_buf(line_bytes: usize, num_records: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(num_records * (line_bytes + 1));
    for i in 0..num_records {
        let field = format!("\"field-{i:06}\"");
        let row = format!("{field},{field},{field}");
        let pad = line_bytes.saturating_sub(row.len());
        buf.extend_from_slice(row.as_bytes());
        buf.resize(buf.len() + pad, b'x');
        buf.push(b'\n');
    }
    buf
}

/// Generate a buffer of JSON objects.
fn make_json_buf(obj_bytes: usize, num_records: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(num_records * (obj_bytes + 1));
    for i in 0..num_records {
        let prefix = format!("{{\"id\":{i},\"msg\":\"");
        // -3 for closing `"}}`  and newline
        let pad = obj_bytes.saturating_sub(prefix.len() + 3);
        buf.extend_from_slice(prefix.as_bytes());
        buf.resize(buf.len() + pad, b'a');
        buf.extend_from_slice(b"\"}\n");
    }
    buf
}

/// Generate a buffer with a custom separator `||`.
fn make_custom_buf(record_bytes: usize, num_records: usize) -> Vec<u8> {
    let sep = b"||";
    let mut buf = Vec::with_capacity(num_records * (record_bytes + sep.len()));
    for i in 0..num_records {
        let prefix = format!("rec-{i:08}: ");
        let pad = record_bytes.saturating_sub(prefix.len());
        buf.extend_from_slice(prefix.as_bytes());
        buf.resize(buf.len() + pad, b'y');
        buf.extend_from_slice(sep);
    }
    buf
}

/// Scan an entire buffer, counting complete records.
fn scan_all(scanner: &mut DelimiterScanner, buf: &[u8]) -> usize {
    let mut count = 0;
    let mut pos = 0;
    loop {
        match scanner.scan(buf, pos) {
            ScanResult::Complete { end, .. } => {
                count += 1;
                pos = end;
            }
            ScanResult::Incomplete => break,
        }
    }
    count
}

fn bench_newline(c: &mut Criterion) {
    let mut group = c.benchmark_group("delimiter/newline");
    for buf_kb in [1, 32, 256, 1024] {
        let buf_size = buf_kb * 1024;
        // ~128-byte lines
        let num_records = buf_size / 128;
        let buf = make_newline_buf(127, num_records);
        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("scan", format!("{buf_kb}KB")),
            &buf,
            |b, buf| {
                b.iter(|| {
                    let mut scanner = DelimiterScanner::new(&Delimiter::Newline);
                    scan_all(&mut scanner, buf)
                })
            },
        );
    }
    group.finish();
}

fn bench_csv(c: &mut Criterion) {
    let mut group = c.benchmark_group("delimiter/csv");
    for buf_kb in [1, 32, 256, 1024] {
        let buf_size = buf_kb * 1024;
        let num_records = buf_size / 128;
        let buf = make_csv_buf(127, num_records);
        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("scan", format!("{buf_kb}KB")),
            &buf,
            |b, buf| {
                b.iter(|| {
                    let mut scanner = DelimiterScanner::new(&Delimiter::Csv);
                    scan_all(&mut scanner, buf)
                })
            },
        );
    }
    group.finish();
}

fn bench_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("delimiter/json");
    for buf_kb in [1, 32, 256, 1024] {
        let buf_size = buf_kb * 1024;
        let num_records = buf_size / 256;
        let buf = make_json_buf(255, num_records.max(1));
        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("scan", format!("{buf_kb}KB")),
            &buf,
            |b, buf| {
                b.iter(|| {
                    let mut scanner = DelimiterScanner::new(&Delimiter::Json);
                    scan_all(&mut scanner, buf)
                })
            },
        );
    }
    group.finish();
}

fn bench_custom(c: &mut Criterion) {
    let mut group = c.benchmark_group("delimiter/custom");
    for buf_kb in [1, 32, 256, 1024] {
        let buf_size = buf_kb * 1024;
        let num_records = buf_size / 128;
        let buf = make_custom_buf(126, num_records);
        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("scan", format!("{buf_kb}KB")),
            &buf,
            |b, buf| {
                b.iter(|| {
                    let mut scanner = DelimiterScanner::new(&Delimiter::Custom("||".into()));
                    scan_all(&mut scanner, buf)
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_newline, bench_csv, bench_json, bench_custom);
criterion_main!(benches);
