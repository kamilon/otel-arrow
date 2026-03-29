// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! High-performance file-tailing receiver.
//!
//! Monitors files matching configurable glob patterns, reads new data with
//! per-file growable buffers, supports log rotation detection, Docker
//! JSON-file log parsing, bookmark-based position persistence, and encoding
//! transcoding. Implements [`local::Receiver`] for thread-per-core execution
//! with round-robin file distribution across cores.

use self::bookmark::BookmarkStore;
use self::config::{Config, DockerLogFormat};
use self::delimiter::DelimiterScanner;
use self::encoding::Transcoder;
use self::metrics::FileTailMetrics;
use self::rotation::RotationOutcome;
use self::tailer::FileTailer;
use self::watcher::{FileWatcher, WatchEvent};
use async_trait::async_trait;
use chrono::Utc;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ReceiverFactory;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::node::NodeId;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{
    MessageSourceLocalEffectHandlerExtension,
    error::{Error, ReceiverErrorKind},
    local::receiver as local,
};
use otap_df_otap::OTAP_RECEIVER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::{
    encode::record::logs::LogsRecordBatchBuilder,
    otap::{Logs, OtapArrowRecords},
    proto::opentelemetry::arrow::v1::ArrowPayloadType,
};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry::{otel_info, otel_warn};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

/// Bookmark (position checkpoint) persistence.
pub mod bookmark;
/// Configuration types.
pub mod config;
/// Record delimiter strategies.
pub mod delimiter;
/// Docker container log format parsers.
pub mod docker;
/// Encoding detection and transcoding.
pub mod encoding;
/// Internal telemetry metrics.
pub mod metrics;
/// Log-rotation detection.
pub mod rotation;
/// Per-file tailing with growable buffers.
pub mod tailer;
/// File-system watching and glob-based discovery.
pub mod watcher;

/// URN for the file-tail receiver.
pub const FILE_TAIL_RECEIVER_URN: &str = "urn:otel:receiver:file_tail";

/// Register the file-tail receiver factory at compile time.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static FILE_TAIL_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: FILE_TAIL_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig| {
        Ok(ReceiverWrapper::local(
            FileTailReceiver::from_config(pipeline, &node_config.config)?,
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

/// The file-tail receiver node.
struct FileTailReceiver {
    config: Config,
    metrics: Rc<RefCell<MetricSet<FileTailMetrics>>>,
    core_id: usize,
    num_cores: usize,
}

impl FileTailReceiver {
    /// Construct from a [`PipelineContext`] and parsed config.
    fn with_pipeline(pipeline: PipelineContext, config: Config) -> Self {
        let metrics = pipeline.register_metrics::<FileTailMetrics>();
        let core_id = pipeline.core_id();
        let num_cores = pipeline.num_cores();
        Self {
            config,
            metrics: Rc::new(RefCell::new(metrics)),
            core_id,
            num_cores,
        }
    }

    /// Create from a raw JSON config value.
    fn from_config(
        pipeline: PipelineContext,
        config: &Value,
    ) -> Result<Self, otap_df_config::error::Error> {
        let cfg: Config = serde_json::from_value(config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

        // Validate docker config at creation time.
        if let Some(ref docker) = cfg.docker {
            match docker.format {
                DockerLogFormat::JsonFile => {} // supported
                DockerLogFormat::Journald | DockerLogFormat::Fluentd | DockerLogFormat::Syslog => {
                    return Err(otap_df_config::error::Error::InvalidUserConfig {
                        error: format!(
                            "docker log format {:?} is not yet implemented",
                            docker.format
                        ),
                    });
                }
            }
        }

        Ok(Self::with_pipeline(pipeline, cfg))
    }
}

/// Build an `OtapArrowRecords` log batch from a set of raw record byte slices.
fn build_log_batch(
    records: &[Vec<u8>],
    source_paths: &[String],
) -> Result<OtapArrowRecords, otap_df_pdata::encode::Error> {
    let mut logs = LogsRecordBatchBuilder::new();
    let count = records.len();
    let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);

    for (i, record) in records.iter().enumerate() {
        logs.append_id(Some(i as u16));
        logs.append_time_unix_nano(now);
        logs.append_observed_time_unix_nano(now);
        logs.body.append_str(record);
        logs.append_severity_number(None);
        logs.append_severity_text(None);
        logs.append_event_name(if source_paths.len() > i {
            Some(source_paths[i].as_bytes())
        } else {
            None
        });
    }

    // Fill resource / scope / remaining fields for the entire batch.
    logs.resource.append_id_n(0, count);
    logs.resource.append_schema_url_n(None, count);
    logs.resource.append_dropped_attributes_count_n(0, count);
    logs.scope.append_id_n(0, count);
    logs.scope.append_name_n(None, count);
    logs.scope.append_version_n(None, count);
    logs.scope.append_dropped_attributes_count_n(0, count);
    logs.append_schema_url_n(None, count);
    logs.append_dropped_attributes_count_n(0, count);
    logs.append_flags_n(None, count);
    let _ = logs.append_trace_id_n(None, count);
    let _ = logs.append_span_id_n(None, count);

    let mut batch = OtapArrowRecords::Logs(Logs::default());
    batch.set(ArrowPayloadType::Logs, logs.finish()?)?;
    Ok(batch)
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for FileTailReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        // --- initialise telemetry timer ---
        let timer_cancel_handle = effect_handler
            .start_periodic_telemetry(Duration::from_secs(1))
            .await?;

        // --- initialise transcoder ---
        let transcoder = Transcoder::new(
            self.config.source_encoding.as_deref(),
            self.config.target_encoding.as_deref(),
        )
        .map_err(|e| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Configuration,
            error: format!("encoding config error: {e}"),
            source_detail: String::new(),
        })?;

        // --- initialise file watcher ---
        let mut file_watcher = FileWatcher::new(
            &self.config.paths,
            self.core_id,
            self.num_cores,
            self.config.poll_interval_ms,
        )
        .map_err(|e| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Configuration,
            error: format!("file watcher init error: {e}"),
            source_detail: String::new(),
        })?;

        let initial_files =
            file_watcher
                .initial_scan(&self.config.paths)
                .map_err(|e| Error::ReceiverError {
                    receiver: effect_handler.receiver_id(),
                    kind: ReceiverErrorKind::Configuration,
                    error: format!("initial scan error: {e}"),
                    source_detail: String::new(),
                })?;

        otel_info!(
            "file_tail_receiver.start",
            core_id = self.core_id,
            num_cores = self.num_cores,
            files = initial_files.len(),
            message = "File-tail receiver started"
        );

        // --- initialise bookmark store ---
        // Use a temp directory for bookmarks; in production this would be a
        // node-specific data directory provided by the pipeline context.
        let bookmark_dir = std::env::temp_dir().join("otap_file_tail_bookmarks");
        std::fs::create_dir_all(&bookmark_dir).map_err(|e| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Configuration,
            error: format!("bookmark dir creation error: {e}"),
            source_detail: String::new(),
        })?;
        let mut bookmark_store =
            BookmarkStore::open(&bookmark_dir).map_err(|e| Error::ReceiverError {
                receiver: effect_handler.receiver_id(),
                kind: ReceiverErrorKind::Configuration,
                error: format!("bookmark store error: {e}"),
                source_detail: String::new(),
            })?;

        // --- open tailers for initial files ---
        let mut tailers: HashMap<PathBuf, FileTailer> = HashMap::new();
        let mut scanners: HashMap<PathBuf, DelimiterScanner> = HashMap::new();

        for path in &initial_files {
            let path_str = path.to_string_lossy().to_string();
            let bookmark_offset = if self.config.bookmark.enabled {
                if let Ok(identity) = rotation::FileIdentity::from_path(path) {
                    bookmark_store
                        .get_validated(&path_str, &identity)
                        .map(|b| b.offset)
                } else {
                    None
                }
            } else {
                None
            };

            match FileTailer::open(
                path,
                &self.config.start_position,
                bookmark_offset,
                self.config.buffer_size,
                self.config.max_buffer_size,
            )
            .await
            {
                Ok(t) => {
                    let _ = scanners
                        .insert(path.clone(), DelimiterScanner::new(&self.config.delimiter));
                    let _ = tailers.insert(path.clone(), t);
                }
                Err(e) => {
                    otel_warn!(
                        "file_tail_receiver.open_failed",
                        path = path_str,
                        error = %e,
                        message = "Failed to open file for tailing"
                    );
                    self.metrics.borrow_mut().errors.inc();
                }
            }
        }

        self.metrics
            .borrow_mut()
            .files_monitored
            .set(tailers.len() as u64);

        // --- batching state ---
        let batch_duration = Duration::from_millis(self.config.batch.max_duration_ms);
        let batch_max_size = self.config.batch.max_size;
        let mut pending_records: Vec<Vec<u8>> = Vec::new();
        let mut pending_sources: Vec<String> = Vec::new();

        // --- timers ---
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let batch_start = tokio::time::Instant::now() + batch_duration;
        let mut batch_timer = tokio::time::interval_at(batch_start, batch_duration);

        let bookmark_flush_interval = Duration::from_millis(self.config.bookmark.flush_interval_ms);
        let bookmark_start = tokio::time::Instant::now() + bookmark_flush_interval;
        let mut bookmark_timer = tokio::time::interval_at(bookmark_start, bookmark_flush_interval);

        let rotation_check_interval = Duration::from_millis(self.config.rotation.check_interval_ms);
        let rotation_start = tokio::time::Instant::now() + rotation_check_interval;
        let mut rotation_timer = tokio::time::interval_at(rotation_start, rotation_check_interval);

        let mut poll_timer =
            tokio::time::interval_at(tokio::time::Instant::now() + poll_interval, poll_interval);

        // Helper closure: flush the pending batch downstream.
        let flush_batch = |pending: &mut Vec<Vec<u8>>,
                           sources: &mut Vec<String>,
                           metrics: &Rc<RefCell<MetricSet<FileTailMetrics>>>,
                           eh: &local::EffectHandler<OtapPdata>| {
            if pending.is_empty() {
                return;
            }
            let count = pending.len() as u64;
            match build_log_batch(pending, sources) {
                Ok(batch) => {
                    let res = eh.try_send_message_with_source_node(OtapPdata::new_todo_context(
                        batch.into(),
                    ));
                    let mut m = metrics.borrow_mut();
                    match &res {
                        Ok(()) => m.records_emitted.add(count),
                        Err(_) => m.records_forward_failed.add(count),
                    }
                }
                Err(e) => {
                    otel_warn!(
                        "file_tail_receiver.batch_build_failed",
                        error = %e,
                        message = "Failed to build Arrow log batch"
                    );
                    metrics.borrow_mut().records_forward_failed.add(count);
                }
            }
            pending.clear();
            sources.clear();
        };

        // --- main event loop ---
        loop {
            tokio::select! {
                biased;

                // 1. Control messages (highest priority)
                ctrl_msg = ctrl_chan.recv() => {
                    match ctrl_msg {
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            let _ = timer_cancel_handle.cancel().await;

                            // Drain all tailers to EOF.
                            for (path, tailer) in &mut tailers {
                                if let Some(scanner) = scanners.get_mut(path) {
                                    match tailer.drain(scanner, &transcoder).await {
                                        Ok(records) => {
                                            let path_str = path.to_string_lossy().to_string();
                                            for r in records {
                                                pending_records.push(r);
                                                pending_sources.push(path_str.clone());
                                            }
                                        }
                                        Err(e) => {
                                            otel_warn!(
                                                "file_tail_receiver.drain_error",
                                                path = %path.display(),
                                                error = %e,
                                                message = "Error draining file during shutdown"
                                            );
                                        }
                                    }
                                }
                            }

                            // Flush remaining batch.
                            flush_batch(
                                &mut pending_records,
                                &mut pending_sources,
                                &self.metrics,
                                &effect_handler,
                            );

                            // Persist final bookmarks.
                            if self.config.bookmark.enabled {
                                for (path, tailer) in &tailers {
                                    bookmark_store.update(
                                        &path.to_string_lossy(),
                                        tailer.offset(),
                                        tailer.identity(),
                                    );
                                }
                                let _ = bookmark_store.flush();
                            }

                            effect_handler.notify_receiver_drained().await?;
                            let snapshot = self.metrics.borrow().snapshot();
                            return Ok(TerminalState::new(deadline, [snapshot]));
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            let _ = timer_cancel_handle.cancel().await;
                            flush_batch(
                                &mut pending_records,
                                &mut pending_sources,
                                &self.metrics,
                                &effect_handler,
                            );
                            if self.config.bookmark.enabled {
                                for (path, tailer) in &tailers {
                                    bookmark_store.update(
                                        &path.to_string_lossy(),
                                        tailer.offset(),
                                        tailer.identity(),
                                    );
                                }
                                let _ = bookmark_store.flush();
                            }
                            let snapshot = self.metrics.borrow().snapshot();
                            return Ok(TerminalState::new(deadline, [snapshot]));
                        }
                        Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                            let mut m = self.metrics.borrow_mut();
                            let _ = metrics_reporter.report(&mut m);
                        }
                        Err(e) => {
                            return Err(Error::ChannelRecvError(e));
                        }
                        _ => {
                            // Other control messages currently unhandled.
                        }
                    }
                }

                // 2. Batch flush timer
                _ = batch_timer.tick() => {
                    flush_batch(
                        &mut pending_records,
                        &mut pending_sources,
                        &self.metrics,
                        &effect_handler,
                    );
                }

                // 3. Bookmark flush timer
                _ = bookmark_timer.tick() => {
                    if self.config.bookmark.enabled {
                        for (path, tailer) in &tailers {
                            bookmark_store.update(
                                &path.to_string_lossy(),
                                tailer.offset(),
                                tailer.identity(),
                            );
                        }
                        if let Err(e) = bookmark_store.flush() {
                            otel_warn!(
                                "file_tail_receiver.bookmark_flush_error",
                                error = %e,
                                message = "Failed to flush bookmarks"
                            );
                            self.metrics.borrow_mut().errors.inc();
                        } else {
                            self.metrics.borrow_mut().bookmark_flushes.inc();
                        }
                    }
                }

                // 4. Rotation check timer
                _ = rotation_timer.tick() => {
                    if self.config.rotation.enabled {
                        let paths: Vec<PathBuf> = tailers.keys().cloned().collect();
                        for path in paths {
                            let outcome = {
                                let tailer = tailers.get(&path).expect("tailer exists");
                                tailer.check_rotation()
                            };
                            match outcome {
                                RotationOutcome::Unchanged => {}
                                RotationOutcome::Rotated => {
                                    otel_info!(
                                        "file_tail_receiver.rotation",
                                        path = %path.display(),
                                        message = "File rotation detected"
                                    );
                                    self.metrics.borrow_mut().rotation_events.inc();

                                    // Drain old file to EOF.
                                    if let Some(scanner) = scanners.get_mut(&path) {
                                        if let Some(tailer) = tailers.get_mut(&path) {
                                            let path_str = path.to_string_lossy().to_string();
                                            match tailer.drain(scanner, &transcoder).await {
                                                Ok(records) => {
                                                    for r in records {
                                                        pending_records.push(r);
                                                        pending_sources.push(path_str.clone());
                                                    }
                                                }
                                                Err(e) => {
                                                    otel_warn!(
                                                        "file_tail_receiver.rotation_drain_error",
                                                        path = %path.display(),
                                                        error = %e,
                                                        message = "Error draining rotated file"
                                                    );
                                                    self.metrics.borrow_mut().errors.inc();
                                                }
                                            }
                                            // Reopen from offset 0.
                                            if let Err(e) = tailer.reopen().await {
                                                otel_warn!(
                                                    "file_tail_receiver.rotation_reopen_error",
                                                    path = %path.display(),
                                                    error = %e,
                                                    message = "Error reopening rotated file"
                                                );
                                                self.metrics.borrow_mut().errors.inc();
                                                let _ = tailers.remove(&path);
                                                let _ = scanners.remove(&path);
                                                self.metrics
                                                    .borrow_mut()
                                                    .files_monitored
                                                    .set(tailers.len() as u64);
                                            } else {
                                                scanner.reset();
                                            }
                                        }
                                    }
                                }
                                RotationOutcome::PathGone => {
                                    otel_info!(
                                        "file_tail_receiver.path_gone",
                                        path = %path.display(),
                                        message = "Tailed file was removed"
                                    );
                                    // Drain and remove.
                                    if let Some(scanner) = scanners.get_mut(&path) {
                                        if let Some(tailer) = tailers.get_mut(&path) {
                                            let path_str = path.to_string_lossy().to_string();
                                            if let Ok(records) = tailer.drain(scanner, &transcoder).await {
                                                for r in records {
                                                    pending_records.push(r);
                                                    pending_sources.push(path_str.clone());
                                                }
                                            }
                                        }
                                    }
                                    let _ = tailers.remove(&path);
                                    let _ = scanners.remove(&path);
                                    if self.config.bookmark.enabled {
                                        bookmark_store.remove(&path.to_string_lossy());
                                    }
                                    self.metrics
                                        .borrow_mut()
                                        .files_monitored
                                        .set(tailers.len() as u64);
                                }
                            }
                        }
                    }
                }

                // 5. Poll watcher events + read data
                _ = poll_timer.tick() => {
                    // Process new/modified/removed file events.
                    let events = file_watcher.poll_events();
                    for event in events {
                        match event {
                            WatchEvent::FileModified(path) => {
                                if !tailers.contains_key(&path) {
                                    // Enforce max_open_files limit.
                                    if tailers.len() >= self.config.max_open_files {
                                        // Evict the tailer with the lowest offset (least active).
                                        let evict = tailers
                                            .iter()
                                            .min_by_key(|(_, t)| t.offset())
                                            .map(|(p, _)| p.clone());
                                        if let Some(evict_path) = evict {
                                            let _ = tailers.remove(&evict_path);
                                            let _ = scanners.remove(&evict_path);
                                        }
                                    }

                                    let path_str = path.to_string_lossy().to_string();
                                    let bookmark_offset = if self.config.bookmark.enabled {
                                        if let Ok(identity) = rotation::FileIdentity::from_path(&path) {
                                            bookmark_store
                                                .get_validated(&path_str, &identity)
                                                .map(|b| b.offset)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    };
                                    match FileTailer::open(
                                        &path,
                                        &self.config.start_position,
                                        bookmark_offset,
                                        self.config.buffer_size,
                                        self.config.max_buffer_size,
                                    )
                                    .await
                                    {
                                        Ok(t) => {
                                            let _ = scanners.insert(
                                                path.clone(),
                                                DelimiterScanner::new(&self.config.delimiter),
                                            );
                                            let _ = tailers.insert(path, t);
                                            self.metrics
                                                .borrow_mut()
                                                .files_monitored
                                                .set(tailers.len() as u64);
                                        }
                                        Err(e) => {
                                            otel_warn!(
                                                "file_tail_receiver.open_failed",
                                                path = path_str,
                                                error = %e,
                                                message = "Failed to open new file"
                                            );
                                            self.metrics.borrow_mut().errors.inc();
                                        }
                                    }
                                }
                            }
                            WatchEvent::FileRemoved(path) => {
                                let _ = tailers.remove(&path);
                                let _ = scanners.remove(&path);
                                if self.config.bookmark.enabled {
                                    bookmark_store.remove(&path.to_string_lossy());
                                }
                                self.metrics
                                    .borrow_mut()
                                    .files_monitored
                                    .set(tailers.len() as u64);
                            }
                        }
                    }

                    // Read from all active tailers.
                    let paths: Vec<PathBuf> = tailers.keys().cloned().collect();
                    for path in paths {
                        if let (Some(tailer), Some(scanner)) =
                            (tailers.get_mut(&path), scanners.get_mut(&path))
                        {
                            match tailer.read_records(scanner, &transcoder).await {
                                Ok((records, bytes_read)) => {
                                    if bytes_read > 0 {
                                        self.metrics.borrow_mut().bytes_read.add(bytes_read);
                                    }
                                    let path_str = path.to_string_lossy().to_string();
                                    for r in records {
                                        pending_records.push(r);
                                        pending_sources.push(path_str.clone());
                                    }
                                }
                                Err(e) => {
                                    otel_warn!(
                                        "file_tail_receiver.read_error",
                                        path = %path.display(),
                                        error = %e,
                                        message = "Error reading file"
                                    );
                                    self.metrics.borrow_mut().errors.inc();
                                }
                            }
                        }
                    }

                    // Flush if batch size threshold is met.
                    if pending_records.len() >= batch_max_size {
                        flush_batch(
                            &mut pending_records,
                            &mut pending_sources,
                            &self.metrics,
                            &effect_handler,
                        );
                        batch_timer.reset();
                    }
                }
            }
        }
    }
}
