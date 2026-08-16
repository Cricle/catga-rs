//! CatgaRaftRuntimeBuilder: programmatic and CLI-friendly builder for CatgaRaftRuntime.
//!
//! This module provides a builder pattern for constructing a `CatgaRaftRuntime` with
//! all necessary components. It supports both CLI-style initialization (with base_port,
//! node index, and total nodes) and programmatic configuration.

use std::sync::Arc;
use std::time::Duration;

use catga_core::ConsensusStateMachine;
use tracing::info;

use crate::apply::ApplyThread;
use crate::config::{CatgaRaftConfig, PipelineConfig};
use crate::coordinator::CatgaRaftCoordinator;
use crate::error::CatgaRaftResult;
use crate::pipeline::PipelineManager;
use crate::runtime::CatgaRaftRuntime;

/// Builder for constructing a `CatgaRaftRuntime`.
///
/// # Example (CLI-style)
///
/// ```ignore
/// let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
///     .start(my_state_machine)
///     .await
///     .unwrap();
/// ```
///
/// # Example (programmatic)
///
/// ```ignore
/// let config = CatgaRaftConfig {
///     node_id: 1,
///     cluster_id: 1,
///     election_tick: 10,
///     heartbeat_tick: 3,
///     max_size_per_msg: 64 * 1024 * 1024,
///     max_inflight_msgs: 256,
/// };
///
/// let runtime = CatgaRaftRuntimeBuilder::new()
///     .with_config(config)
///     .with_pipeline_config(PipelineConfig::default())
///     .with_members(vec![(2, "http://127.0.0.1:9200".to_string())])
///     .start(my_state_machine)
///     .await
///     .unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct CatgaRaftRuntimeBuilder {
    /// Raft configuration.
    config: CatgaRaftConfig,
    /// Pipeline configuration for batching proposals.
    pipeline_config: PipelineConfig,
    /// Cluster members (node_id, endpoint).
    members: Vec<(u64, String)>,
    /// This node's own raft endpoint; when set, the gRPC server binds here.
    self_endpoint: Option<String>,
    /// Directory for persistent raft state (raft-engine log + hard state).
    /// None keeps the in-memory storage, whose log is lost on restart.
    data_dir: Option<std::path::PathBuf>,
}

impl CatgaRaftRuntimeBuilder {
    /// Creates a builder from CLI-style arguments.
    ///
    /// For clusters larger than ~10 nodes, prefer starting from
    /// [`CatgaRaftConfig::for_cluster_size`] via `with_config` so the
    /// election timeout window is widened for the cluster size.
    ///
    /// # Arguments
    ///
    /// * `base_port` - Base port number for the cluster. Each node uses `base_port + |node_index - this_index| * 100`.
    /// * `node` - Zero-based index of this node in the cluster.
    /// * `nodes` - Total number of nodes in the cluster.
    ///
    /// # Example
    ///
    /// For `from_cli(9100, 0, 3)`:
    /// - Node 0: `http://127.0.0.1:9100`
    /// - Node 1: `http://127.0.0.1:9200`
    /// - Node 2: `http://127.0.0.1:9300`
    pub fn from_cli(base_port: u16, node: u64, nodes: u64) -> CatgaRaftResult<Self> {
        if nodes == 0 {
            return Err(crate::CatgaRaftError::Raft(
                "at least one node is required".into(),
            ));
        }

        // Node IDs are 1-indexed internally
        let node_id = node + 1;

        let config = CatgaRaftConfig {
            node_id,
            cluster_id: 1,
            ..Default::default()
        };

        // Build member list (excluding this node)
        // Port formula: base_port + i * 100, where i is the peer's node index
        let mut members = Vec::with_capacity((nodes - 1) as usize);
        for i in 0..nodes {
            if i == node {
                continue;
            }
            let peer_id = i + 1;
            let port = base_port + (i as u16) * 100;
            let endpoint = format!("http://127.0.0.1:{}", port);
            members.push((peer_id, endpoint));
        }

        info!(
            node_id,
            base_port,
            member_count = members.len(),
            "created builder from CLI args"
        );

        Ok(Self {
            config,
            pipeline_config: PipelineConfig::default(),
            members,
            self_endpoint: Some(format!(
                "http://127.0.0.1:{}",
                base_port + (node as u16) * 100
            )),
            data_dir: None,
        })
    }

    /// Creates a new builder with default settings.
    ///
    /// Use this for programmatic configuration without CLI convenience.
    pub fn new() -> Self {
        Self {
            config: CatgaRaftConfig::default(),
            pipeline_config: PipelineConfig::default(),
            members: Vec::new(),
            self_endpoint: None,
            data_dir: None,
        }
    }

    /// Sets the Raft configuration.
    ///
    /// This allows fine-grained control over Raft parameters like election timeout,
    /// heartbeat interval, and message sizes.
    pub fn with_config(mut self, config: CatgaRaftConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the pipeline configuration for batch replication.
    ///
    /// The pipeline config controls batching behavior:
    /// - `batch_size`: Maximum entries per batch
    /// - `flush_interval`: Maximum time before flushing an incomplete batch
    /// - `max_inflight`: Maximum concurrent uncommitted entries
    pub fn with_pipeline_config(mut self, pipeline_config: PipelineConfig) -> Self {
        self.pipeline_config = pipeline_config;
        self
    }

    /// Sets the batch size for the pipeline.
    ///
    /// This is a convenience method that only changes the batch size
    /// while keeping other pipeline settings at their defaults.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.pipeline_config.batch_size = batch_size;
        self
    }

    /// Sets the flush interval for the pipeline.
    ///
    /// The pipeline will flush after this duration even if the batch
    /// is not full. Shorter intervals mean lower latency but less batching.
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.pipeline_config.flush_interval = interval;
        self
    }

    /// Sets the maximum in-flight entries for the pipeline.
    ///
    /// This limits how many proposals can be waiting for replication.
    /// Higher values increase throughput but use more memory.
    pub fn with_max_inflight(mut self, max_inflight: usize) -> Self {
        self.pipeline_config.max_inflight = max_inflight;
        self
    }

    /// Adds a cluster member.
    ///
    /// # Arguments
    ///
    /// * `id` - The node's Raft ID (must be unique in the cluster)
    /// * `endpoint` - The node's HTTP endpoint (e.g., "http://127.0.0.1:9200")
    pub fn with_member(mut self, id: u64, endpoint: impl Into<String>) -> Self {
        self.members.push((id, endpoint.into()));
        self
    }

    /// Sets the cluster members.
    ///
    /// This replaces any existing members. Each member is a tuple of
    /// (node_id, endpoint).
    pub fn with_members(mut self, members: Vec<(u64, String)>) -> Self {
        self.members = members;
        self
    }

    /// Sets this node's own raft endpoint; the gRPC server binds here.
    pub fn with_self_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.self_endpoint = Some(endpoint.into());
        self
    }

    /// Sets the data directory for persistent raft state.
    ///
    /// When set, raft log entries are persisted via raft-engine and the hard
    /// state via a companion file, so the raft log survives restarts. When
    /// unset (the default), storage stays in memory.
    pub fn with_data_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.data_dir = Some(path.into());
        self
    }

    /// Returns this node's own raft endpoint, if configured.
    pub fn self_endpoint(&self) -> Option<&str> {
        self.self_endpoint.as_deref()
    }

    /// Wires the whole cluster topology in one call: this node's own endpoint
    /// plus the peer member list.
    ///
    /// Replaces any previously configured self endpoint and members. `members`
    /// must list only peers: it must not contain this node's own id, and ids
    /// must be unique (both are enforced by [`start`](Self::start)).
    ///
    /// # Example (k8s-style DNS endpoints)
    ///
    /// ```ignore
    /// let runtime = CatgaRaftRuntimeBuilder::new()
    ///     .with_config(CatgaRaftConfig {
    ///         node_id: 1,
    ///         cluster_id: 1,
    ///         ..Default::default()
    ///     })
    ///     .with_topology(
    ///         "http://catga-raft-0.catga-raft-headless.default.svc.cluster.local:9100",
    ///         vec![
    ///             (2, "http://catga-raft-1.catga-raft-headless.default.svc.cluster.local:9100".to_string()),
    ///             (3, "http://catga-raft-2.catga-raft-headless.default.svc.cluster.local:9100".to_string()),
    ///         ],
    ///     )
    ///     .start(my_state_machine)
    ///     .await?;
    /// ```
    pub fn with_topology(
        mut self,
        self_endpoint: impl Into<String>,
        members: Vec<(u64, String)>,
    ) -> Self {
        self.self_endpoint = Some(self_endpoint.into());
        self.members = members;
        self
    }

    /// Sets the cluster ID.
    ///
    /// All nodes in a Raft cluster must share the same cluster ID.
    pub fn with_cluster_id(mut self, cluster_id: u64) -> Self {
        self.config.cluster_id = cluster_id;
        self
    }

    /// Sets the election tick count.
    ///
    /// This is the number of ticks between elections. The actual timeout
    /// depends on the underlying timer tick interval.
    pub fn with_election_tick(mut self, election_tick: usize) -> Self {
        self.config.election_tick = election_tick;
        self
    }

    /// Sets the heartbeat tick count.
    ///
    /// This is the number of ticks between heartbeats when this node is leader.
    pub fn with_heartbeat_tick(mut self, heartbeat_tick: usize) -> Self {
        self.config.heartbeat_tick = heartbeat_tick;
        self
    }

    /// Sets the maximum size per message.
    ///
    /// Raft messages larger than this will be chunked.
    pub fn with_max_size_per_msg(mut self, max_size: u64) -> Self {
        self.config.max_size_per_msg = max_size;
        self
    }

    /// Sets the maximum in-flight messages.
    ///
    /// This limits how many messages can be pending for a single peer.
    pub fn with_max_inflight_msgs(mut self, max_inflight: usize) -> Self {
        self.config.max_inflight_msgs = max_inflight;
        self
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &CatgaRaftConfig {
        &self.config
    }

    /// Returns the current members.
    pub fn members(&self) -> &[(u64, String)] {
        &self.members
    }

    /// Validates the builder state before any component is started.
    ///
    /// `CatgaRaftConfig::default()` leaves `node_id` at 0, so a default
    /// builder would otherwise silently boot a broken raft node.
    fn validate(&self) -> CatgaRaftResult<()> {
        let node_id = self.config.node_id;
        if node_id == 0 {
            return Err(crate::CatgaRaftError::Raft(
                "node_id must be non-zero: CatgaRaftConfig::default() leaves it at 0; \
                 set a valid node_id via with_config() or from_cli()"
                    .into(),
            ));
        }
        if let Some(endpoint) = &self.self_endpoint {
            Self::parse_endpoint_port(endpoint)?;
        }
        if self.members.iter().any(|(id, _)| *id == node_id) {
            return Err(crate::CatgaRaftError::Raft(format!(
                "members must not contain this node's own id {node_id}; list only peers"
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for (id, _) in &self.members {
            if !seen.insert(id) {
                return Err(crate::CatgaRaftError::Raft(format!(
                    "duplicate member id {id} in members list"
                )));
            }
        }
        Ok(())
    }

    /// Extracts the port from an endpoint like `http://host:9100`.
    fn parse_endpoint_port(endpoint: &str) -> CatgaRaftResult<u16> {
        endpoint
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| {
                crate::CatgaRaftError::Raft(format!(
                    "self_endpoint {endpoint:?} has no parseable port; \
                     expected a URL like \"http://host:9100\""
                ))
            })
    }

    /// Starts the runtime with the given state machine.
    ///
    /// For clusters larger than ~10 nodes, supply a config built with
    /// [`CatgaRaftConfig::for_cluster_size`] (via `with_config`) to widen
    /// the randomized election timeout window.
    ///
    /// This initializes all components:
    /// - PipelineManager for batching proposals
    /// - ApplyThread for applying committed entries
    /// - CatgaRaftCoordinator for leadership tracking
    /// - RaftTransport for inter-node communication
    ///
    /// # Runtime requirements
    ///
    /// Must be called from within a multi-threaded Tokio runtime (e.g. under
    /// `#[tokio::main]`): `start` spawns the raft owner loop and, when a self
    /// endpoint is configured, the gRPC server task, and wires them to the
    /// runtime via tokio mpsc/watch channels. A current-thread runtime cannot
    /// drive these background tasks correctly.
    ///
    /// # Arguments
    ///
    /// * `state_machine` - The application's state machine implementing `ConsensusStateMachine`
    ///
    /// # Returns
    ///
    /// A running `CatgaRaftRuntime` that can be used to propose entries.
    ///
    /// # Errors
    ///
    /// Returns `CatgaRaftError::Raft` before starting any component when:
    /// - `config.node_id` is 0 (the `CatgaRaftConfig::default()` value),
    /// - the self endpoint is set but has no parseable port,
    /// - the member list contains this node's own id,
    /// - the member list contains duplicate ids.
    ///
    /// Storage or transport setup failures from the underlying components are
    /// propagated as well.
    pub async fn start<S>(self, state_machine: S) -> CatgaRaftResult<CatgaRaftRuntime<S>>
    where
        S: ConsensusStateMachine + 'static,
    {
        self.validate()?;

        let node_id = self.config.node_id;
        let member_endpoints: Vec<String> = self.members.iter().map(|(_, ep)| ep.clone()).collect();

        info!(
            node_id,
            cluster_id = self.config.cluster_id,
            member_count = self.members.len(),
            "starting CatgaRaftRuntime"
        );

        // Create the pipeline manager
        let pipeline = PipelineManager::new(self.pipeline_config.clone());
        pipeline.start();
        let batch_rx = pipeline.batch_receiver();
        let flush_notify = pipeline.flush_notify();

        // Create the apply thread
        let apply = ApplyThread::new(state_machine);

        // Create the coordinator
        let coordinator = CatgaRaftCoordinator::new(format!("node-{}", node_id));
        coordinator.set_members(member_endpoints.clone());

        // Bootstrap the raft group with all voters known up front.
        let mut voters: Vec<u64> = self.members.iter().map(|(id, _)| *id).collect();
        voters.push(node_id);
        voters.sort_unstable();
        voters.dedup();
        let storage = match &self.data_dir {
            Some(dir) => {
                let conf_state =
                    raft::prelude::ConfState::from((voters.clone(), Vec::<u64>::new()));
                info!(
                    node_id,
                    data_dir = %dir.display(),
                    "using persistent raft-engine storage"
                );
                crate::storage::CatgaStorage::engine(dir, node_id, Some(conf_state))?
            }
            None => crate::storage::CatgaStorage::memory_with_conf_state((voters, vec![])),
        };

        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raft_node = crate::node::RaftNode::new(self.config.clone(), storage.clone(), &logger)?;
        let raw_node = raft_node.raw_node();

        // Wire the transport: dial peers, serve our own endpoint.
        let transport = std::sync::Arc::new(crate::transport::GrpcTransport::new(node_id));
        for (peer_id, endpoint) in &self.members {
            transport.add_peer(*peer_id, endpoint.clone()).await?;
        }

        // Build the runtime first so the gRPC server can watch its shutdown
        // signal and drain instead of blocking `join` forever.
        let (read_tx, read_rx) = tokio::sync::mpsc::unbounded_channel();
        let (conf_tx, conf_rx) = tokio::sync::mpsc::unbounded_channel();
        let (prop_wait_tx, prop_wait_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = CatgaRaftRuntime::with_read_channel(
            pipeline,
            apply,
            coordinator,
            self.config.clone(),
            Some(read_tx),
            Some(conf_tx),
            Some(prop_wait_tx),
        );

        // Wire the snapshot provider: when raft needs a snapshot for a
        // far-behind follower (`Storage::snapshot`), storage pulls the state
        // machine bytes plus the applied index they reflect from the apply
        // thread. The receive half (installing a snapshot and restoring the
        // machine) lives in the owner loop. Holding the machine lock across
        // `applied_index` + `snapshot()` keeps the pair consistent against
        // concurrent applies or restores.
        {
            let apply = Arc::clone(runtime.apply());
            storage.set_snapshot_provider(std::sync::Arc::new(move || {
                let machine = apply.state_machine().lock();
                let applied = apply.applied_index();
                let bytes = machine.snapshot().map_err(|e| {
                    crate::CatgaRaftError::Storage(format!("state machine snapshot: {e}"))
                })?;
                Ok((bytes, applied))
            }));
        }

        // Peer messages are queued and stepped by the owner loop only;
        // RawNode must be driven by a single thread.
        let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut server_task = None;
        if let Some(endpoint) = &self.self_endpoint {
            // Listen on all interfaces: the self endpoint's hostname is what
            // peers dial, which may resolve differently inside this host
            // (e.g. container DNS aliases).
            let port: u16 = Self::parse_endpoint_port(endpoint)?;
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            let step_tx = msg_tx.clone();
            let service = crate::transport::server::RaftGrpcService::new(move |msg| {
                step_tx
                    .send(msg)
                    .map_err(|_| crate::CatgaRaftError::Transport("raft owner loop is gone".into()))
            });
            let mut shutdown_rx = runtime.shutdown_receiver();
            let shutdown = async move {
                let _ = shutdown_rx.changed().await;
            };
            match crate::transport::server::serve_with_shutdown(addr, service, shutdown).await {
                Ok(task) => server_task = Some(task),
                // Keep the node usable (outbound-only) instead of failing startup,
                // e.g. when tests run several runtimes on the same derived port.
                Err(e) => tracing::warn!(error = %e, %endpoint, "raft gRPC server not started"),
            }
        }

        if let Some(task) = server_task {
            runtime.register_task(task);
        }

        let peers: std::collections::HashMap<u64, String> = self.members.iter().cloned().collect();
        let owner = tokio::spawn(crate::owner::run(
            runtime.shutdown_receiver(),
            raw_node,
            storage,
            transport,
            batch_rx,
            Arc::clone(runtime.pipeline()),
            msg_rx,
            read_rx,
            conf_rx,
            prop_wait_rx,
            flush_notify,
            Arc::clone(runtime.apply()),
            Arc::clone(runtime.coordinator()),
            node_id,
            self.self_endpoint.clone(),
            peers,
        ));
        runtime.register_task(owner);
        drop(msg_tx);

        info!(node_id, "CatgaRaftRuntime started successfully");

        Ok(runtime)
    }

    /// Starts the runtime with a default state machine.
    ///
    /// This is only available when `S: Default`.
    #[allow(dead_code)]
    pub async fn start_default<S>(&self) -> CatgaRaftResult<CatgaRaftRuntime<S>>
    where
        S: ConsensusStateMachine + Default + 'static,
    {
        self.clone().start(S::default()).await
    }
}

impl Default for CatgaRaftRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CatgaRaftRuntimeBuilder {
    /// Shorthand for `Self::new()` to enable builder pattern chaining.
    #[allow(dead_code)]
    fn builder() -> Self {
        Self::new()
    }
}
