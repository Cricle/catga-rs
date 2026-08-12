//! CLI-shaped one-call bootstrap for a single-shard sorock node.
//!
//! [`SorockRuntimeBuilder`] collapses the port/storage/member derivation and
//! the imperative group formation every CLI- or StatefulSet-launched sorock
//! application rewrites by hand: bind and advertise the gRPC port derived
//! from a base port, place the redb file, seed the member view, start the
//! [`SorockRuntime`], and — on the designated bootstrap node — add this node
//! to the empty membership (which bootstraps the group) before joining every
//! peer, one membership change at a time.

use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
};

use catga_core::{CatgaError, CatgaResult, ConsensusRuntime, ConsensusStateMachine, ErrorCode};

use crate::{SorockNodeConfig, SorockRuntime, SorockStorage};

/// Offset from a node's API port to its sorock gRPC port in the
/// [`SorockRuntimeBuilder::from_cli`] layout.
pub const CLI_GRPC_PORT_OFFSET: u16 = 1000;

/// Default group-formation budget of [`SorockRuntimeBuilder::start`]: peers
/// may still be starting when the bootstrap node comes up, so membership
/// changes retry until this expires and process start order does not matter.
const DEFAULT_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(120);

/// Default pause between membership-call retries during group formation.
const DEFAULT_BOOTSTRAP_RETRY_DELAY: Duration = Duration::from_millis(500);

fn validation_error(message: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, message.to_string())
}

fn internal_error(message: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, message.to_string())
}

/// Builds one sorock node from CLI-shaped arguments and forms the group.
///
/// Created by [`Self::from_cli`], which derives the full
/// [`SorockNodeConfig`] from `(base_port, node, nodes)`; the `with_*`
/// methods override individual derivations for deployments whose topology
/// does not follow the CLI layout (for example orchestrator-assigned fixed
/// ports). [`Self::start`] then starts the runtime and drives the group
/// formation documented at the crate level.
pub struct SorockRuntimeBuilder {
    config: SorockNodeConfig,
    member_id: u64,
    peers: Vec<(u64, String)>,
    api_host: String,
    api_port: u16,
    bootstrap: bool,
    bootstrap_timeout: Duration,
    bootstrap_retry_delay: Duration,
}

impl SorockRuntimeBuilder {
    /// Derives one node's configuration from CLI-shaped arguments.
    ///
    /// `node` is this node's zero-based index among the `nodes` members. The
    /// derivation matches across members:
    ///
    /// - gRPC bind: `127.0.0.1:(base_port + {CLI_GRPC_PORT_OFFSET} + node)`,
    ///   advertised as `http://127.0.0.1:<same port>`;
    /// - application API address: `127.0.0.1:(base_port + node)`, reported by
    ///   [`Self::api_addr`] — sorock does not serve it; the application binds
    ///   its own listener there;
    /// - storage: a redb file at `./sorock-state-node{node}-p{base_port}`,
    ///   keyed by base port as well as node index so two local clusters on
    ///   different port ranges never pick up each other's state;
    /// - seed members and bootstrap peers: the gRPC URIs of every other node,
    ///   numbered `index + 1`.
    ///
    /// Node `0` becomes the group's bootstrap node (see [`Self::start`]).
    ///
    /// # Errors
    ///
    /// Fails validation when `nodes` is zero, `node >= nodes`, or the port
    /// ranges leave no room inside `u16`.
    pub fn from_cli(base_port: u16, node: u64, nodes: u64) -> CatgaResult<Self> {
        if nodes == 0 || node >= nodes {
            return Err(validation_error("sorock node index must be in [0, nodes)"));
        }
        let api_port = cli_port(base_port, 0, node, nodes, "api")?;
        let grpc_port = cli_port(base_port, CLI_GRPC_PORT_OFFSET, node, nodes, "gRPC")?;
        let peers: Vec<(u64, String)> = (0..nodes)
            .filter(|index| *index != node)
            .map(|index| {
                (
                    index + 1,
                    format!(
                        "http://127.0.0.1:{}",
                        base_port + CLI_GRPC_PORT_OFFSET + index as u16
                    ),
                )
            })
            .collect();
        let mut config = SorockNodeConfig::new(
            format!("node-{}", node + 1),
            SocketAddr::from(([127, 0, 0, 1], grpc_port)),
        );
        config.public_uri = Some(format!("http://127.0.0.1:{grpc_port}"));
        config.storage = SorockStorage::RedbFile(PathBuf::from(format!(
            "./sorock-state-node{node}-p{base_port}/raft.redb"
        )));
        config.members = peers.iter().map(|(_, uri)| uri.clone()).collect();
        Ok(Self {
            config,
            member_id: node + 1,
            peers,
            api_host: "127.0.0.1".to_owned(),
            api_port,
            bootstrap: node == 0,
            bootstrap_timeout: DEFAULT_BOOTSTRAP_TIMEOUT,
            bootstrap_retry_delay: DEFAULT_BOOTSTRAP_RETRY_DELAY,
        })
    }

    /// Adjusts the derived node configuration before startup.
    ///
    /// Deployments whose topology does not follow the CLI layout — fixed
    /// orchestrator ports, externally reachable advertise URIs, mounted
    /// storage — rewrite the derived fields here (bind address,
    /// [`SorockNodeConfig::public_uri`], [`SorockNodeConfig::storage`],
    /// [`SorockNodeConfig::members`]).
    pub fn with_node_config(mut self, configure: impl FnOnce(&mut SorockNodeConfig)) -> Self {
        configure(&mut self.config);
        self
    }

    /// Replaces the peer list used for group formation and re-seeds the
    /// coordinator's member view from it.
    pub fn with_peers(mut self, peers: impl IntoIterator<Item = (u64, String)>) -> Self {
        self.peers = peers.into_iter().collect();
        self.config.members = self.peers.iter().map(|(_, uri)| uri.clone()).collect();
        self
    }

    /// Overrides the host of the application API address
    /// (for example `0.0.0.0` under an orchestrator).
    pub fn with_api_host(mut self, host: impl Into<String>) -> Self {
        self.api_host = host.into();
        self
    }

    /// Overrides the port of the application API address
    /// (for example a fixed container port shared by every replica).
    pub fn with_api_port(mut self, port: u16) -> Self {
        self.api_port = port;
        self
    }

    /// Overrides whether this node drives the initial group formation.
    ///
    /// Defaults to the node at index `0`; every other node starts with an
    /// empty membership and is joined by the bootstrap node.
    pub fn with_bootstrap(mut self, bootstrap: bool) -> Self {
        self.bootstrap = bootstrap;
        self
    }

    /// Overrides the group-formation budget: the total time membership
    /// changes may retry and the pause between attempts.
    pub fn with_bootstrap_budget(mut self, timeout: Duration, retry_delay: Duration) -> Self {
        self.bootstrap_timeout = timeout;
        self.bootstrap_retry_delay = retry_delay;
        self
    }

    /// Returns the application API address derived for this node
    /// (`host:port`), where the application binds its own HTTP listener.
    pub fn api_addr(&self) -> String {
        format!("{}:{}", self.api_host, self.api_port)
    }

    /// Starts the sorock runtime and forms the group.
    ///
    /// The node starts with [`SorockRuntime::start`] (the storage directory
    /// is created when missing), wrapping `machine` in a [`crate::SorockApp`].
    /// The bootstrap node then drives sorock's imperative formation on the
    /// shard: it adds this node's own advertised URI to the empty membership
    /// — a self-add bootstraps the group and elects this node leader — then
    /// joins every peer, one membership change at a time. Every change
    /// retries until the formation budget ([`Self::with_bootstrap_budget`])
    /// expires, so peers may start in any order. Non-bootstrap nodes skip
    /// formation and are joined by the bootstrap node; their runtime serves
    /// once they appear in the group's membership.
    ///
    /// # Errors
    ///
    /// Returns an error when the storage directory cannot be created, the
    /// node cannot start, or the formation budget expires before the group
    /// is formed.
    pub async fn start<M>(self, machine: M) -> CatgaResult<SorockRuntime>
    where
        M: ConsensusStateMachine + 'static,
    {
        if let SorockStorage::RedbFile(path) = &self.config.storage
            && let Some(parent) = path.parent()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                internal_error(format!(
                    "sorock storage directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let runtime = SorockRuntime::start(self.config, machine).await?;
        if self.bootstrap {
            let deadline = Instant::now() + self.bootstrap_timeout;
            let self_uri = runtime.advertised_uri().to_owned();
            add_with_retry(
                &runtime,
                self.member_id,
                &self_uri,
                deadline,
                self.bootstrap_retry_delay,
            )
            .await?;
            for (member_id, uri) in &self.peers {
                add_with_retry(
                    &runtime,
                    *member_id,
                    uri,
                    deadline,
                    self.bootstrap_retry_delay,
                )
                .await?;
            }
        }
        Ok(runtime)
    }
}

/// Validates that `nodes` ports fit in `u16` starting at `base_port + offset`
/// and returns this node's port.
fn cli_port(base_port: u16, offset: u16, node: u64, nodes: u64, kind: &str) -> CatgaResult<u16> {
    if u64::from(base_port) + u64::from(offset) + nodes > u64::from(u16::MAX) + 1 {
        return Err(validation_error(format!(
            "base port {base_port} leaves no room for {nodes} {kind} ports \
             (offset {offset} from the api port)"
        )));
    }
    Ok(base_port + offset + node as u16)
}

/// Issues one membership change, retrying until `deadline` so a peer that is
/// still starting does not break formation.
async fn add_with_retry(
    runtime: &SorockRuntime,
    member_id: u64,
    uri: &str,
    deadline: Instant,
    retry_delay: Duration,
) -> CatgaResult<()> {
    loop {
        match ConsensusRuntime::add_member(runtime, member_id, uri.to_owned()).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(
                    retry_delay.min(deadline.saturating_duration_since(Instant::now())),
                )
                .await;
            }
        }
    }
}
