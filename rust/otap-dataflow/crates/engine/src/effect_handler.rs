// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Common foundation of all effect handlers.

use crate::Interests;
use crate::completion_emission_metrics::CompletionEmissionMetricsHandle;
use crate::control::{
    AckMsg, NackMsg, PipelineCompletionMsg, PipelineCompletionMsgSender, RuntimeControlMsg,
    RuntimeCtrlMsgSender,
};
use crate::error::Error;
use crate::node::NodeId;
use otap_df_channel::error::SendError;
use otap_df_telemetry::error::Error as TelemetryError;
use otap_df_telemetry::metrics::{MetricSet, MetricSetHandler};
use otap_df_telemetry::reporter::MetricsReporter;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, UdpSocket};

// ---------------------------------------------------------------------------
// Windows shared-listener registry
// ---------------------------------------------------------------------------
//
// On Unix, `SO_REUSEPORT` allows each per-core thread to create an independent
// listening socket on the same address:port.  The kernel maintains per-socket
// accept queues and distributes incoming connections by hashing the 4-tuple.
//
// Windows has no `SO_REUSEPORT`.  Instead we create a single listening socket
// per address and hand each per-core thread a **cloned** handle obtained via
// `socket2::Socket::try_clone()` (backed by `WSADuplicateSocketW`).  When each
// clone is registered with a different Tokio runtime (each backed by its own
// IOCP), accept completions are distributed across the runtimes.
//
// The registry is process-global because:
// - Receiver addresses are not known until `start()` time, long after the
//   controller has spawned per-core threads.
// - The first thread to reach a given address creates the real socket; all
//   others (including the first) obtain clones.
// - Cleanup happens via `remove()` when a pipeline shuts down.
#[cfg(windows)]
mod shared_listener_registry {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    /// Process-global registry of shared listening sockets (Windows only).
    ///
    /// Keyed by `SocketAddr`; the value is the *original* `socket2::Socket`
    /// that was bound and put into listen mode.  Callers obtain independent
    /// handles via [`socket2::Socket::try_clone`].
    static REGISTRY: Mutex<Option<HashMap<SocketAddr, socket2::Socket>>> = Mutex::new(None);

    /// Returns a clone of the shared listening socket for `addr`, creating the
    /// original on first call.
    ///
    /// `create` is invoked at most once per address and must return a fully
    /// configured, listening `socket2::Socket`.
    pub(super) fn get_or_create_tcp_listener(
        addr: SocketAddr,
        create: impl FnOnce() -> std::io::Result<socket2::Socket>,
    ) -> std::io::Result<socket2::Socket> {
        let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let map = guard.get_or_insert_with(HashMap::new);
        if !map.contains_key(&addr) {
            let _ = map.insert(addr, create()?);
        }
        map[&addr].try_clone()
    }

    /// Returns a clone of the shared UDP socket for `addr`, creating the
    /// original on first call.
    pub(super) fn get_or_create_udp_socket(
        addr: SocketAddr,
        create: impl FnOnce() -> std::io::Result<socket2::Socket>,
    ) -> std::io::Result<socket2::Socket> {
        // Separate map for UDP to avoid collisions with TCP on the same addr.
        static UDP_REGISTRY: Mutex<Option<HashMap<SocketAddr, socket2::Socket>>> = Mutex::new(None);

        let mut guard = UDP_REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        let map = guard.get_or_insert_with(HashMap::new);
        if !map.contains_key(&addr) {
            let _ = map.insert(addr, create()?);
        }
        map[&addr].try_clone()
    }

    /// Removes the shared socket for `addr`, allowing it to be re-created on a
    /// subsequent call.  This should be called during pipeline shutdown.
    #[allow(dead_code)]
    pub(super) fn remove(addr: &SocketAddr) {
        let mut guard = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(map) = guard.as_mut() {
            let _ = map.remove(addr);
        }
    }
}

/// SourceTagging indicates whether the Context will contain empty source frames.
#[derive(Clone, Copy)]
pub enum SourceTagging {
    /// Disabled means no source node-id will be automatically
    /// inserted for nodes that do not not otherwise subscribe to
    /// Ack/Nack.
    Disabled,

    /// Enabled means a source node_id will be automatically entered
    /// by creating a new frame in as messagees are sent.
    Enabled,
}

impl SourceTagging {
    /// Indicates that source tagging is required.
    #[must_use]
    pub const fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Common implementation of all effect handlers.
///
/// Note: This implementation is `Send`.
#[derive(Clone)]
pub(crate) struct EffectHandlerCore<PData> {
    pub(crate) node_id: NodeId,
    // ToDo refactor the code to avoid using Option here.
    pub(crate) runtime_ctrl_msg_sender: Option<RuntimeCtrlMsgSender<PData>>,
    pub(crate) pipeline_completion_msg_sender: Option<PipelineCompletionMsgSender<PData>>,
    #[allow(dead_code)]
    // Will be used in the future. ToDo report metrics from channel and messages.
    pub(crate) metrics_reporter: MetricsReporter,
    /// Optional node-scoped metrics for completions routed by this effect handler.
    pub(crate) completion_emission_metrics: Option<CompletionEmissionMetricsHandle>,
    /// The outgoing message source tagging mode.
    pub(crate) source_tag: SourceTagging,
    /// Precomputed node interests derived from metric level.
    node_interests: Interests,
}

impl<PData> EffectHandlerCore<PData> {
    /// Creates a new EffectHandlerCore with node_id and a metrics reporter.
    pub(crate) const fn new(node_id: NodeId, metrics_reporter: MetricsReporter) -> Self {
        Self {
            node_id,
            runtime_ctrl_msg_sender: None,
            pipeline_completion_msg_sender: None,
            metrics_reporter,
            completion_emission_metrics: None,
            source_tag: SourceTagging::Disabled,
            node_interests: Interests::empty(),
        }
    }

    /// Sets the runtime control message sender for this effect handler.
    pub fn set_runtime_ctrl_msg_sender(
        &mut self,
        runtime_ctrl_msg_sender: RuntimeCtrlMsgSender<PData>,
    ) {
        self.runtime_ctrl_msg_sender = Some(runtime_ctrl_msg_sender);
    }

    /// Sets the pipeline result message sender for this effect handler.
    pub fn set_pipeline_completion_msg_sender(
        &mut self,
        pipeline_completion_msg_sender: PipelineCompletionMsgSender<PData>,
    ) {
        self.pipeline_completion_msg_sender = Some(pipeline_completion_msg_sender);
    }

    /// Sets whether outgoing messages need source node tagging.
    pub fn set_source_tagging(&mut self, value: SourceTagging) {
        self.source_tag = value;
    }

    /// Sets the optional node-scoped completion-emission metrics handle.
    pub fn set_completion_emission_metrics(
        &mut self,
        completion_emission_metrics: Option<CompletionEmissionMetricsHandle>,
    ) {
        self.completion_emission_metrics = completion_emission_metrics;
    }

    /// Returns outgoing messages source tagging mode.
    #[must_use]
    pub const fn source_tagging(&self) -> SourceTagging {
        self.source_tag
    }

    /// Sets the precomputed node interests for this effect handler.
    pub fn set_node_interests(&mut self, interests: Interests) {
        self.node_interests = interests;
    }

    /// Returns the precomputed node interests.
    ///
    /// Includes SOURCE_TAGGING when source tagging is enabled.
    #[must_use]
    pub fn node_interests(&self) -> Interests {
        if self.source_tag.enabled() {
            self.node_interests | Interests::SOURCE_TAGGING
        } else {
            self.node_interests
        }
    }

    /// Returns the id of the node associated with this effect handler.
    #[must_use]
    pub(crate) fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Print an info message to stdout.
    ///
    /// This method provides a standardized way for all nodes in the pipeline
    /// to output informational messages without blocking the async runtime.
    pub(crate) async fn info(&self, message: &str) {
        use tokio::io::{AsyncWriteExt, stdout};
        let mut out = stdout();
        // Ignore write errors as they're typically not recoverable for stdout
        let _ = out.write_all(message.as_bytes()).await;
        let _ = out.write_all(b"\n").await;
        let _ = out.flush().await;
    }

    /// Creates a non-blocking TCP listener on the given address with socket options defined by the
    /// pipeline engine implementation. It's important for receiver implementer to create TCP
    /// listeners via this method to ensure the scalability and the serviceability of the pipeline.
    ///
    /// # Platform behavior
    ///
    /// - **Unix**: Each per-core thread creates an independent listener with
    ///   `SO_REUSEPORT`, giving per-socket accept queues with kernel-level
    ///   connection distribution.
    /// - **Windows**: A single listening socket is created per address and
    ///   shared across per-core threads via `try_clone()`
    ///   (`WSADuplicateSocketW`).  Each clone is registered with the calling
    ///   thread's Tokio IOCP runtime, so accept completions are distributed
    ///   across cores.
    ///
    /// # Errors
    ///
    /// Returns an [`Error::IoError`] if any step in the process fails.
    ///
    /// ToDo: return a std::net::TcpListener instead of a tokio::net::tcp::TcpListener to avoid leaking our current dependency on Tokio.
    pub(crate) fn tcp_listener(
        &self,
        addr: SocketAddr,
        receiver_id: NodeId,
    ) -> Result<TcpListener, Error> {
        // Helper closure to convert errors.
        let into_engine_error = |error: std::io::Error| Error::IoError {
            node: receiver_id.clone(),
            error,
        };

        let std_listener = self.create_tcp_socket(addr).map_err(into_engine_error)?;
        TcpListener::from_std(std_listener).map_err(into_engine_error)
    }

    /// Creates (or clones) a raw TCP listening socket for `addr`.
    ///
    /// On Unix this always creates a fresh socket with `SO_REUSEPORT`.
    /// On Windows this returns a `try_clone()` of a process-wide shared socket.
    fn create_tcp_socket(&self, addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
        #[cfg(windows)]
        {
            let sock = shared_listener_registry::get_or_create_tcp_listener(addr, || {
                Self::new_tcp_socket(addr)
            })?;
            Ok(sock.into())
        }

        #[cfg(not(windows))]
        {
            let sock = Self::new_tcp_socket(addr)?;
            Ok(sock.into())
        }
    }

    /// Low-level: creates a new TCP listening socket with platform-appropriate
    /// options.
    fn new_tcp_socket(addr: SocketAddr) -> std::io::Result<socket2::Socket> {
        let sock = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::STREAM,
            None,
        )?;

        // Allows rebinding while a previous socket is in TIME_WAIT.
        sock.set_reuse_address(true)?;

        // On Unix, SO_REUSEPORT gives each per-core listener its own accept
        // queue with kernel-level load balancing.  On Windows this option does
        // not exist; instead the shared-listener registry (above) distributes a
        // single socket across cores via try_clone().
        #[cfg(unix)]
        {
            sock.set_reuse_port(true)?;
        }

        sock.set_nonblocking(true)?;
        sock.bind(&addr.into())?;
        sock.listen(8192)?;

        Ok(sock)
    }

    /// Creates a non-blocking UDP socket on the given address with socket options defined by the
    /// pipeline engine implementation. It's important for receiver implementer to create UDP
    /// sockets via this method to ensure the scalability and the serviceability of the pipeline.
    ///
    /// # Platform behavior
    ///
    /// Same strategy as [`tcp_listener`](Self::tcp_listener): `SO_REUSEPORT` on
    /// Unix, shared socket via `try_clone()` on Windows.
    ///
    /// # Errors
    ///
    /// Returns an [`Error::IoError`] if any step in the process fails.
    ///
    /// ToDo: return a std::net::UdpSocket instead of a tokio::net::UdpSocket to avoid leaking our current dependency on Tokio.
    #[allow(dead_code)]
    pub(crate) fn udp_socket(
        &self,
        addr: SocketAddr,
        receiver_id: NodeId,
    ) -> Result<UdpSocket, Error> {
        // Helper closure to convert errors.
        let into_engine_error = |error: std::io::Error| Error::IoError {
            node: receiver_id.clone(),
            error,
        };

        let std_socket = self.create_udp_socket(addr).map_err(into_engine_error)?;
        UdpSocket::from_std(std_socket).map_err(into_engine_error)
    }

    /// Creates (or clones) a raw UDP socket for `addr`.
    ///
    /// On Unix this always creates a fresh socket with `SO_REUSEPORT`.
    /// On Windows this returns a `try_clone()` of a process-wide shared socket.
    fn create_udp_socket(&self, addr: SocketAddr) -> std::io::Result<std::net::UdpSocket> {
        #[cfg(windows)]
        {
            let sock = shared_listener_registry::get_or_create_udp_socket(addr, || {
                Self::new_udp_socket(addr)
            })?;
            Ok(sock.into())
        }

        #[cfg(not(windows))]
        {
            let sock = Self::new_udp_socket(addr)?;
            Ok(sock.into())
        }
    }

    /// Low-level: creates a new UDP socket with platform-appropriate options.
    fn new_udp_socket(addr: SocketAddr) -> std::io::Result<socket2::Socket> {
        let sock = socket2::Socket::new(
            match addr {
                SocketAddr::V4(_) => socket2::Domain::IPV4,
                SocketAddr::V6(_) => socket2::Domain::IPV6,
            },
            socket2::Type::DGRAM,
            None,
        )?;

        // Allows rebinding while a previous socket is in TIME_WAIT.
        sock.set_reuse_address(true)?;

        // On Unix, SO_REUSEPORT gives each per-core socket its own receive
        // queue with kernel-level packet distribution.  On Windows, the
        // shared-listener registry distributes a single socket across cores
        // via try_clone().
        #[cfg(unix)]
        {
            sock.set_reuse_port(true)?;
        }

        sock.set_nonblocking(true)?;
        sock.bind(&addr.into())?;

        Ok(sock)
    }

    /// Reports the provided metrics to the engine.
    #[allow(dead_code)] // Will be used in the future. ToDo report metrics from channel and messages.
    pub(crate) fn report_metrics<M: MetricSetHandler + 'static>(
        &mut self,
        metrics: &mut MetricSet<M>,
    ) -> Result<(), TelemetryError> {
        self.metrics_reporter.report(metrics)
    }

    /// Re-usable function to send a runtime control message. This returns a reference
    /// to the sender to place in a cancelation, for example.
    async fn send_runtime_ctrl_msg(
        &self,
        msg: RuntimeControlMsg<PData>,
    ) -> Result<RuntimeCtrlMsgSender<PData>, SendError<RuntimeControlMsg<PData>>> {
        let runtime_ctrl_msg_sender = self.runtime_ctrl_msg_sender.clone()
            .expect("[Internal Error] Node request sender not set. This is a bug in the pipeline engine implementation.");
        runtime_ctrl_msg_sender.send(msg).await?;
        Ok(runtime_ctrl_msg_sender)
    }

    /// Re-usable function to send a pipeline result message.
    async fn send_pipeline_completion_msg(
        &self,
        msg: PipelineCompletionMsg<PData>,
    ) -> Result<PipelineCompletionMsgSender<PData>, SendError<PipelineCompletionMsg<PData>>> {
        let pipeline_completion_msg_sender = self.pipeline_completion_msg_sender.clone()
            .expect("[Internal Error] Node return sender not set. This is a bug in the pipeline engine implementation.");
        pipeline_completion_msg_sender.send(msg).await?;
        Ok(pipeline_completion_msg_sender)
    }

    /// Starts a cancellable periodic timer that emits TimerTick on the control channel.
    /// Returns a handle that can be used to cancel the timer.
    ///
    /// Current limitation: The timer can only be started once per node.
    pub async fn start_periodic_timer(
        &self,
        duration: Duration,
    ) -> Result<TimerCancelHandle<PData>, Error> {
        let runtime_ctrl_msg_sender = self
            .send_runtime_ctrl_msg(RuntimeControlMsg::StartTimer {
                node_id: self.node_id.index,
                duration,
            })
            .await
            .map_err(|e| Error::RuntimeMsgError {
                error: e.to_string(),
            })?;

        Ok(TimerCancelHandle {
            node_id: self.node_id.index,
            runtime_ctrl_msg_sender,
        })
    }

    /// Starts a cancellable periodic telemetry collection timer that emits CollectTelemetry on the control channel.
    /// Returns a handle that can be used to cancel the telemetry timer.
    pub async fn start_periodic_telemetry(
        &self,
        duration: Duration,
    ) -> Result<TelemetryTimerCancelHandle<PData>, Error> {
        let runtime_ctrl_msg_sender = self
            .send_runtime_ctrl_msg(RuntimeControlMsg::StartTelemetryTimer {
                node_id: self.node_id.index,
                duration,
            })
            .await
            .map_err(|e| Error::RuntimeMsgError {
                error: e.to_string(),
            })?;

        Ok(TelemetryTimerCancelHandle {
            node_id: self.node_id.clone(),
            runtime_ctrl_msg_sender,
        })
    }

    /// Send an AckMsg to the runtime control manager for context unwinding.
    /// This will skip if there are no frames.
    ///
    /// TODO: Note that callers are able to directly invoke route_ack() and route_nack()
    /// but it means skipping certain frame-related business and they have the same
    /// signature, so it is easy for callers to do. Find something safer.
    pub async fn route_ack(&self, ack: AckMsg<PData>) -> Result<(), Error>
    where
        PData: crate::Unwindable,
    {
        if ack.accepted.has_frames() {
            self.send_pipeline_completion_msg(PipelineCompletionMsg::DeliverAck { ack })
                .await
                .map(|_| {
                    if let Some(metrics) = &self.completion_emission_metrics {
                        let mut metrics = metrics
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        metrics.record_notify_ack_routed();
                    }
                })
                .map_err(|e| Error::RuntimeMsgError {
                    error: e.to_string(),
                })
        } else {
            Ok(())
        }
    }

    /// Send a NackMsg to the runtime control manager for context unwinding.
    /// Same semantics as `route_ack()`.
    pub async fn route_nack(&self, nack: NackMsg<PData>) -> Result<(), Error>
    where
        PData: crate::Unwindable,
    {
        if nack.refused.has_frames() {
            self.send_pipeline_completion_msg(PipelineCompletionMsg::DeliverNack { nack })
                .await
                .map(|_| {
                    if let Some(metrics) = &self.completion_emission_metrics {
                        let mut metrics = metrics
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        metrics.record_notify_nack_routed();
                    }
                })
                .map_err(|e| Error::RuntimeMsgError {
                    error: e.to_string(),
                })
        } else {
            Ok(())
        }
    }

    /// Delay a message.
    pub async fn delay_data(&self, when: Instant, data: Box<PData>) -> Result<(), PData> {
        self.send_runtime_ctrl_msg(RuntimeControlMsg::DelayData {
            node_id: self.node_id().index,
            when,
            data,
        })
        .await
        .map(|_| ())
        .map_err(|e| -> PData {
            match e.inner() {
                RuntimeControlMsg::DelayData { data, .. } => *data,
                _ => unreachable!(),
            }
        })
    }

    /// Notifies the runtime control manager that this receiver has completed
    /// ingress drain.
    pub async fn notify_receiver_drained(&self) -> Result<(), Error> {
        self.send_runtime_ctrl_msg(RuntimeControlMsg::ReceiverDrained {
            node_id: self.node_id().index,
        })
        .await
        .map(|_| ())
        .map_err(|e| Error::RuntimeMsgError {
            error: e.to_string(),
        })
    }
}

/// Handle to cancel a running timer.
pub struct TimerCancelHandle<PData> {
    node_id: usize,
    runtime_ctrl_msg_sender: RuntimeCtrlMsgSender<PData>,
}

impl<PData> TimerCancelHandle<PData> {
    /// Cancels the timer.
    pub async fn cancel(self) -> Result<(), SendError<RuntimeControlMsg<PData>>> {
        self.runtime_ctrl_msg_sender
            .send(RuntimeControlMsg::CancelTimer {
                node_id: self.node_id,
            })
            .await
    }
}

/// Handle to cancel a running telemetry timer.
pub struct TelemetryTimerCancelHandle<PData> {
    node_id: NodeId,
    runtime_ctrl_msg_sender: RuntimeCtrlMsgSender<PData>,
}

impl<PData> TelemetryTimerCancelHandle<PData> {
    /// Cancels the telemetry collection timer.
    pub async fn cancel(self) -> Result<(), SendError<RuntimeControlMsg<PData>>> {
        self.runtime_ctrl_msg_sender
            .send(RuntimeControlMsg::CancelTelemetryTimer {
                node_id: self.node_id.index,
                _temp: std::marker::PhantomData,
            })
            .await
    }
}
