//! CatgaRaftRuntime: implements `ConsensusRuntime` for the TiKV-style Raft backend.
//!
//! This runtime coordinates proposals through the PipelineManager,
//! applies entries via ApplyThread, and provides access to the coordinator.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use catga_core::{
    CatgaError, CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine,
    ErrorCode,
};
use tracing::{debug, info};

use crate::apply::ApplyThread;
use crate::config::CatgaRaftConfig;
use crate::coordinator::CatgaRaftCoordinator;
use crate::error::CatgaRaftError;
use crate::pipeline::PipelineManager;

/// One linearizable-read request: unique context bytes plus the reply channel
/// carrying the committed index the read was served against.
pub(crate) type ReadRequest = (
    Vec<u8>,
    tokio::sync::oneshot::Sender<crate::CatgaRaftResult<u64>>,
);

/// One attributed propose: unique context bytes plus the reply channel that
/// receives the committed entry's index (or Timeout).
pub(crate) type ProposeWait = (
    Vec<u8>,
    tokio::sync::oneshot::Sender<crate::CatgaRaftResult<u64>>,
);

/// One membership operation handed to the owner loop, which proposes it as a
/// raft conf change when this node leads the group.
#[derive(Debug, Clone)]
pub(crate) enum ConfChangeOp {
    /// Add `node_id` as a voter reachable at `endpoint`.
    Add { node_id: u64, endpoint: String },
    /// Remove `node_id` from the group.
    Remove { node_id: u64 },
}

/// One membership-change request: the operation plus the reply channel,
/// resolved when the corresponding conf-change entry is applied (or fails).
pub(crate) type ConfChangeRequest = (
    ConfChangeOp,
    tokio::sync::oneshot::Sender<crate::CatgaRaftResult<()>>,
);

/// CatgaRaftRuntime is the main handle to a running Raft consensus group.
///
/// It combines:
/// - [`PipelineManager`] for high-throughput async proposal batching
/// - [`ApplyThread`] for applying committed entries to the state machine
/// - [`CatgaRaftCoordinator`] for leadership and membership information
///
/// All consensus mutations are serialized through this runtime, so it is
/// safe to share across multiple tasks.
pub struct CatgaRaftRuntime<S: ConsensusStateMachine> {
    /// Pipeline manager for batching proposals.
    pipeline: Arc<PipelineManager>,
    /// Apply thread for committing entries to the state machine.
    apply: Arc<ApplyThread<S>>,
    /// Coordinator for leadership and membership view.
    coordinator: Arc<CatgaRaftCoordinator>,
    /// Configuration.
    config: CatgaRaftConfig,
    /// Whether the runtime is still alive (owner task running).
    alive: AtomicBool,
    /// Shutdown signal sender.
    shutdown_tx: tokio::sync::watch::Sender<()>,
    /// Shutdown signal receiver cloned into the owner loop.
    shutdown_rx: tokio::sync::watch::Receiver<()>,
    /// Flag indicating shutdown has been requested.
    shutdown_requested: AtomicBool,
    /// Background tasks owned by the runtime (owner loop, gRPC server).
    tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Channel into the owner loop for ReadIndex requests.
    read_tx: Option<tokio::sync::mpsc::UnboundedSender<ReadRequest>>,
    /// Channel into the owner loop for propose_and_wait attributions.
    prop_wait_tx: Option<tokio::sync::mpsc::UnboundedSender<ProposeWait>>,
    /// Context counter making every ReadIndex request unique.
    read_ctx: std::sync::atomic::AtomicU64,
    /// Channel into the owner loop for membership-change requests.
    conf_tx: Option<tokio::sync::mpsc::UnboundedSender<ConfChangeRequest>>,
}

impl<S: ConsensusStateMachine> CatgaRaftRuntime<S> {
    /// Creates a new CatgaRaftRuntime with the given components.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pipeline: PipelineManager,
        apply: ApplyThread<S>,
        coordinator: CatgaRaftCoordinator,
        config: CatgaRaftConfig,
    ) -> Self {
        Self::with_read_channel(pipeline, apply, coordinator, config, None, None, None)
    }

    /// Creates a runtime, optionally wired to an owner loop that answers
    /// ReadIndex and membership-change requests.
    #[allow(private_interfaces, clippy::too_many_arguments)]
    pub fn with_read_channel(
        pipeline: PipelineManager,
        apply: ApplyThread<S>,
        coordinator: CatgaRaftCoordinator,
        config: CatgaRaftConfig,
        read_tx: Option<tokio::sync::mpsc::UnboundedSender<ReadRequest>>,
        conf_tx: Option<tokio::sync::mpsc::UnboundedSender<ConfChangeRequest>>,
        prop_wait_tx: Option<tokio::sync::mpsc::UnboundedSender<ProposeWait>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        Self {
            pipeline: Arc::new(pipeline),
            apply: Arc::new(apply),
            coordinator: Arc::new(coordinator),
            config,
            alive: AtomicBool::new(true),
            shutdown_tx,
            shutdown_rx,
            shutdown_requested: AtomicBool::new(false),
            tasks: parking_lot::Mutex::new(Vec::new()),
            read_tx,
            prop_wait_tx,
            read_ctx: std::sync::atomic::AtomicU64::new(1),
            conf_tx,
        }
    }

    /// Creates a new runtime with minimal configuration for testing.
    pub fn new_for_test(node_id: u64) -> Self
    where
        S: Default,
    {
        let config = CatgaRaftConfig {
            node_id,
            ..Default::default()
        };

        let coordinator = CatgaRaftCoordinator::new(format!("node-{}", node_id));

        Self::new(
            PipelineManager::default(),
            ApplyThread::new(S::default()),
            coordinator,
            config,
        )
    }

    /// Returns a reference to the pipeline manager.
    pub fn pipeline(&self) -> &Arc<PipelineManager> {
        &self.pipeline
    }

    /// Returns a reference to the apply thread.
    pub fn apply(&self) -> &Arc<ApplyThread<S>> {
        &self.apply
    }

    /// Returns a reference to the coordinator.
    pub fn coordinator(&self) -> &Arc<CatgaRaftCoordinator> {
        &self.coordinator
    }

    /// Returns the configuration.
    pub fn config(&self) -> &CatgaRaftConfig {
        &self.config
    }

    /// Proposes data through the Raft pipeline.
    ///
    /// This is a synchronous wrapper around the async `propose` method.
    fn propose_sync(&self, data: Vec<u8>) -> CatgaResult<()> {
        // Check if this node is the leader
        if !self.coordinator.is_leader() {
            return Err(CatgaError::new(ErrorCode::Unavailable, "not leader"));
        }

        // Propose through the pipeline (fire-and-forget)
        self.pipeline
            .propose(data)
            .map_err(|e| CatgaError::new(ErrorCode::TransportFailed, e.to_string()))
    }

    /// Sends one membership operation to the owner loop and waits for the
    /// corresponding conf-change entry to be applied (or for the request to
    /// fail: not leader, dropped proposal, expiry).
    ///
    /// # Errors
    ///
    /// `Transport` when no owner loop is attached, otherwise whatever the
    /// owner loop reports over the reply channel (`NotLeader`, `Raft`,
    /// `Timeout`).
    async fn change_membership(&self, op: ConfChangeOp) -> crate::CatgaRaftResult<()> {
        let tx = self.conf_tx.as_ref().ok_or_else(|| {
            CatgaRaftError::Transport("membership change requires a running owner loop".into())
        })?;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        tx.send((op, reply_tx))
            .map_err(|_| CatgaRaftError::Transport("raft owner loop is gone".into()))?;
        reply_rx.await.map_err(|_| CatgaRaftError::Timeout)?
    }

    /// Gets the current applied index.
    fn applied_index_sync(&self) -> CatgaResult<u64> {
        Ok(self.apply.applied_index())
    }

    /// Updates the applied index (used internally).
    #[allow(dead_code)]
    pub fn set_applied_index(&self, _index: u64) {
        // This would be called by the apply thread when it advances
        // In the current design, ApplyThread maintains its own applied_index
    }

    /// Updates the leader information.
    pub fn set_leader(&self, endpoint: Option<String>) {
        self.coordinator.set_leader(endpoint);
    }

    /// Returns whether the runtime has been requested to shut down.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    /// Registers a background task so `join` can await it after shutdown.
    pub fn register_task(&self, handle: tokio::task::JoinHandle<()>) {
        self.tasks.lock().push(handle);
    }

    /// Clones the shutdown watch receiver for the owner loop.
    pub fn shutdown_receiver(&self) -> tokio::sync::watch::Receiver<()> {
        self.shutdown_rx.clone()
    }

    /// Raft ReadIndex: resolves to the committed log index that quorum has
    /// confirmed as at least as fresh as the current leader. Once the state
    /// machine has applied up to that index, a local read is linearizable.
    ///
    /// # Errors
    ///
    /// `Transport` when no owner loop is attached, `Timeout` when the read
    /// cannot be confirmed (e.g. no leader).
    pub async fn read_index(&self) -> crate::CatgaRaftResult<u64> {
        let tx = self.read_tx.as_ref().ok_or_else(|| {
            CatgaRaftError::Transport("read index requires a running owner loop".into())
        })?;
        let ctx = self
            .read_ctx
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes()
            .to_vec();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        tx.send((ctx, reply_tx))
            .map_err(|_| CatgaRaftError::Transport("raft owner loop is gone".into()))?;
        reply_rx.await.map_err(|_| CatgaRaftError::Timeout)?
    }

    /// Linearizable read barrier: confirms a quorum-checked commit index via
    /// ReadIndex, then waits until the local state machine has applied it.
    /// After this returns, a local read reflects every write committed before
    /// the call. Returns the index the read was served against.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::read_index`] errors; `Timeout` when the state
    /// machine does not catch up within `timeout`.
    pub async fn read_barrier(&self, timeout: std::time::Duration) -> crate::CatgaRaftResult<u64> {
        let index = self.read_index().await?;
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.apply.applied_index() >= index {
                return Ok(index);
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(CatgaRaftError::Timeout);
            }
            tokio::time::sleep(remaining.min(std::time::Duration::from_millis(1))).await;
        }
    }

    /// Attributed propose: registers a unique context, proposes, and waits
    /// for this entry's commit+apply. Fire-and-forget variants stay on
    /// [`Self::propose`].
    async fn propose_attributed(
        &self,
        data: Vec<u8>,
        timeout: Duration,
    ) -> crate::CatgaRaftResult<u64> {
        let tx = self.prop_wait_tx.as_ref().ok_or_else(|| {
            crate::CatgaRaftError::Transport(
                "propose_and_wait requires a running owner loop".into(),
            )
        })?;
        let ctx = self
            .read_ctx
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes()
            .to_vec();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        // Register the attribution BEFORE proposing so the commit can never
        // race past an unregistered waiter.
        tx.send((ctx.clone(), reply_tx))
            .map_err(|_| crate::CatgaRaftError::Transport("raft owner loop is gone".into()))?;
        self.pipeline.propose_with_context(data, ctx)?;
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_reply_dropped)) => Err(crate::CatgaRaftError::Timeout),
            Err(_elapsed) => Err(crate::CatgaRaftError::Timeout),
        }
    }

    /// Serializes `value` with bincode and proposes it fire-and-forget.
    ///
    /// # Errors
    ///
    /// `Codec` when serialization fails; otherwise the usual `propose`
    /// admission errors.
    pub fn propose_serializable<T: serde::Serialize>(
        &self,
        value: &T,
    ) -> crate::CatgaRaftResult<()> {
        let data = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| crate::CatgaRaftError::Codec(format!("serialize proposal: {e}")))?;
        self.pipeline.propose(data)
    }

    /// Serializes `value` with bincode, proposes it, and waits until THIS
    /// entry is committed and applied (attributed; no polling). Returns the
    /// applied index.
    ///
    /// # Errors
    ///
    /// `Codec` on serialization failure; `Timeout` when the entry is not
    /// confirmed within `timeout`; `NotLeader`/`Backpressure` per admission.
    pub async fn propose_and_wait_serializable<T: serde::Serialize>(
        &self,
        value: &T,
        timeout: Duration,
    ) -> crate::CatgaRaftResult<u64> {
        let data = bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| crate::CatgaRaftError::Codec(format!("serialize proposal: {e}")))?;
        self.propose_attributed(data, timeout).await
    }

    /// Shuts down and awaits every owned background task without consuming
    /// the handle (usable through `Arc`). Safe to call once; later calls are
    /// no-ops.
    pub async fn shutdown_and_join(&self) -> crate::CatgaRaftResult<()> {
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.shutdown_tx.send(());
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
        self.pipeline.stop();
        self.alive.store(false, Ordering::Release);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<S: ConsensusStateMachine + 'static> ConsensusRuntime for CatgaRaftRuntime<S> {
    /// Proposes one application command through the currently elected leader.
    ///
    /// This is fire-and-forget: `Ok(())` means the entry was locally accepted
    /// by the leader, **not** that it was committed or applied.
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()> {
        debug!(target: "catga_raft::runtime", data_len = data.len(), "proposing entry");
        self.propose_sync(data)
    }

    /// Propose and wait until THIS entry is committed and applied, resolved
    /// via a unique attribution context carried through the raft log
    /// (push-based; no polling). Returns the applied index.
    async fn propose_and_wait(&self, data: Vec<u8>, timeout: Duration) -> CatgaResult<u64> {
        if self.prop_wait_tx.is_none() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "propose_and_wait requires a running owner loop".to_string(),
            ));
        }
        self.propose_attributed(data, timeout)
            .await
            .map_err(CatgaError::from)
    }

    /// Proposes adding one member with its externally reachable `endpoint`.
    ///
    /// Resolves `Ok(())` only after the conf-change entry is committed and
    /// applied on this node; `Err` when this node is not the leader, the
    /// proposal is dropped, or the request expires.
    async fn add_member(&self, id: u64, endpoint: String) -> CatgaResult<()> {
        debug!(target: "catga_raft::runtime", id, endpoint = %endpoint, "add_member");
        Ok(self
            .change_membership(ConfChangeOp::Add {
                node_id: id,
                endpoint,
            })
            .await?)
    }

    /// Proposes removing one member from the group.
    ///
    /// Same resolution semantics as [`Self::add_member`].
    async fn remove_member(&self, id: u64) -> CatgaResult<()> {
        debug!(target: "catga_raft::runtime", id, "remove_member");
        Ok(self
            .change_membership(ConfChangeOp::Remove { node_id: id })
            .await?)
    }

    /// Returns the greatest log index applied to the application state machine.
    async fn applied_index(&self) -> CatgaResult<u64> {
        self.applied_index_sync()
    }

    /// Returns whether the backend's owner task is still running.
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Returns the leadership and membership view for this node.
    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        Arc::clone(&self.coordinator) as Arc<dyn ConsensusCoordinator>
    }

    /// Requests a graceful stop of the owner task.
    fn shutdown(&self) {
        debug!(target: "catga_raft::runtime", "shutdown requested");
        self.shutdown_requested.store(true, Ordering::Release);

        // Signal the shutdown
        let _ = self.shutdown_tx.send(());
    }

    /// Waits for the owner task and returns its terminal status.
    async fn join(self: Box<Self>) -> CatgaResult<()> {
        info!(target: "catga_raft::runtime", "joining runtime");

        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.shutdown_tx.send(());

        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
        self.pipeline.stop();
        self.alive.store(false, Ordering::Release);

        Ok(())
    }
}

impl<S: ConsensusStateMachine> Drop for CatgaRaftRuntime<S> {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
        debug!(target: "catga_raft::runtime", "runtime dropped");
    }
}
