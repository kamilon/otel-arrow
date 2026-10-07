// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Isolated allocator probe for Arrow builder growth and finish overlap.

// SAFETY: This isolated test allocator delegates every memory operation to
// `System`; the atomics only observe requested allocation sizes.
#![allow(unsafe_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use arrow::datatypes::DataType;
use otap_df_pdata::encode::record::{
    attributes::AttributesRecordBatchBuilder, metrics::NumberDataPointsRecordBatchBuilder,
};

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static HIGH_WATER_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record_allocation(size: usize) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    let _ = HIGH_WATER_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        let _ = LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                let _ = LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        new_pointer
    }
}

#[test]
fn probe_builder_growth_dictionary_upgrade_and_finish_overlap() {
    const POINTS: u32 = 65_536;
    const BATCH_POINTS: u32 = 8_192;
    const ATTRIBUTES: u32 = 1_024;

    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    HIGH_WATER_BYTES.store(baseline, Ordering::Relaxed);

    let mut finished_batches = Vec::new();
    let mut point_bytes = 0;
    let mut max_point_finish = std::time::Duration::ZERO;
    for batch_start in (0..POINTS).step_by(BATCH_POINTS as usize) {
        let mut points = NumberDataPointsRecordBatchBuilder::new();
        for id in batch_start..batch_start + BATCH_POINTS {
            points.append_id(id);
            points.append_parent_id(0);
            points.append_start_time_unix_nano(None);
            points.append_time_unix_nano(1_700_000_000_000_000_000);
            points.append_int_value(None);
            points.append_double_value(Some(f64::from(id)));
            points.append_flags(0);
        }
        let finish_start = Instant::now();
        let point_batch = points.finish().expect("finish point batch");
        max_point_finish = max_point_finish.max(finish_start.elapsed());
        point_bytes += point_batch.get_array_memory_size();
        finished_batches.push(point_batch);
    }

    let mut attributes = AttributesRecordBatchBuilder::<u16>::new();
    for index in 0..ATTRIBUTES {
        let key = format!("attribute_key_{index:04}");
        let value = format!("attribute_value_{index:04}");
        attributes.append_parent_id(&0);
        attributes.append_key(key.as_bytes());
        attributes.any_values_builder.append_str(value.as_bytes());
    }
    let finish_start = Instant::now();
    let attribute_batch = attributes.finish().expect("finish attribute batch");
    let attribute_finish = finish_start.elapsed();
    let attribute_bytes = attribute_batch.get_array_memory_size();
    assert_eq!(
        attribute_batch.column(1).data_type(),
        &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
    );
    finished_batches.push(attribute_batch);

    let peak_bytes = HIGH_WATER_BYTES.load(Ordering::Relaxed) - baseline;
    let retained_bytes = point_bytes + attribute_bytes;
    println!(
        "points={POINTS} attributes={ATTRIBUTES} point_batch_bytes={point_bytes} \
         attribute_batch_bytes={attribute_bytes} retained_batch_bytes={retained_bytes} \
         append_finish_peak_bytes={peak_bytes} \
         max_{BATCH_POINTS}_point_finish_us={} attribute_finish_us={}",
        max_point_finish.as_micros(),
        attribute_finish.as_micros()
    );

    assert_eq!(finished_batches.len(), (POINTS / BATCH_POINTS + 1) as usize);
    assert!(point_bytes > 0);
    assert!(attribute_bytes > 0);
    assert!(peak_bytes >= retained_bytes);
}
