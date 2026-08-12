//! One-call bootstrap for a Raft state-machine node served over HTTP.
//!
//! [`RaftHttpCluster`] collapses the generic wiring every HTTP-hosted Raft
//! application rewrites by hand: opening the configured node, spawning the
//! [`RaftStateMachineRuntime`] with an [`HttpRaftTransport`] that carries a
//! bounded request timeout and peer identity, mounting the inbound Raft route
//! behind the peer-identity middleware and a static inbound policy, and
//! exposing `/healthz` and `/status` probes.
//!
//! Topology discovery is intentionally out of scope: the builder consumes an
//! already-resolved [`RaftClusterConfig`] and never reads the environment.
//! Applications keep their own routes, mediator pipelines, and leader duties,
//! and merge them into [`RaftHttpCluster::serve`] (or serve
//! [`RaftHttpCluster::router`] themselves for full lifecycle control).

use std::{future::Future, sync::Arc, time::Duration};

use axum::{Json, Router, middleware, routing::get};
use catga_cluster::{
    ClusterCoordinator, RaftClusterConfig, RaftClusterNode, RaftCommittedEntry, RaftStateMachine,
    RaftStateMachineDriver, RaftStateMachineRuntime, StaticRaftInboundPolicy,
};
use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use http::StatusCode;
use serde::Serialize;
use tokio::net::TcpListener;

use crate::{HttpRaftTransport, raft_message_route, raft_peer_identity_middleware};

/// Default per-request timeout for outbound Raft protocol frames.
///
/// The Raft owner task awaits each send, so a peer that accepts TCP but never
/// responds would stall the whole Raft loop without this bound. Two seconds is
/// generous for a healthy peer and short enough to report a stalled one
/// unreachable well inside an election timeout.
pub const DEFAULT_RAFT_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Liveness probe path mounted by [`RaftHttpCluster`].
///
/// Returns `200` while the Raft owner task runs and `503` once it has stopped
/// (terminal failure or shutdown), so an orchestrator can restart a zombie
/// process whose HTTP stack is still alive.
pub const RAFT_HTTP_HEALTH_PATH: &str = "/healthz";

/// Readiness/status probe path mounted by [`RaftHttpCluster`].
///
/// Reports this node's Raft member id, leadership view, leader endpoint,
/// liveness, and latest applied index as JSON.
pub const RAFT_HTTP_STATUS_PATH: &str = "/status";

/// Maps a Raft member id to the self-asserted peer identity carried by
/// [`crate::RAFT_PEER_IDENTITY_HEADER`].
type PeerNaming = Arc<dyn Fn(u64) -> String + Send + Sync>;

fn default_peer_naming() -> PeerNaming {
    Arc::new(|id| format!("node-{id}"))
}

fn config_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}

fn bootstrap_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}

/// A running Raft state-machine node with its HTTP ingress already wired.
///
/// Construct one through [`RaftHttpCluster::builder`]. The handle exposes the
/// runtime, the leadership coordinator, and the pre-wired router so
/// applications compose their own routes and behaviors on top before serving.
///
/// # Startup ordering
///
/// [`serve`](Self::serve) binds the application listener first and only then
/// starts the Raft election: the Raft owner task awaits transport sends, so
/// campaigning before peers can be reached deadlocks startup. The same
/// ordering must be preserved by applications that serve [`Self::router`]
/// manually — call [`RaftStateMachineRuntime::campaign`] only after the
/// listener is accepting.
pub struct RaftHttpCluster {
    runtime: Arc<RaftStateMachineRuntime>,
    coordinator: Arc<RaftClusterNode>,
    router: Router,
}

impl RaftHttpCluster {
    /// Starts building one cluster node from an already-resolved configuration.
    ///
    /// The configuration supplies the local endpoint, the remote members, the
    /// validated Raft timing, and the optional durable state path; how those
    /// values were discovered (static config, DNS, an orchestrator) is the
    /// application's concern.
    ///
    /// ```no_run
    /// # use catga_axum::{RaftHttpCluster, axum::Router};
    /// # use catga_cluster::{RaftClusterConfig, RaftCommittedEntry, RaftStateMachine};
    /// # use catga_core::CatgaResult;
    /// # struct Machine;
    /// # impl RaftStateMachine for Machine {
    /// #     fn apply(&mut self, _: &RaftCommittedEntry) -> CatgaResult<()> { Ok(()) }
    /// #     fn snapshot(&self) -> CatgaResult<Vec<u8>> { Ok(Vec::new()) }
    /// #     fn restore(&mut self, _: &[u8]) -> CatgaResult<()> { Ok(()) }
    /// # }
    /// # async fn run(config: RaftClusterConfig) -> CatgaResult<()> {
    /// let cluster = RaftHttpCluster::builder(config).state_machine(Machine).build()?;
    /// let listener = tokio::net::TcpListener::bind("0.0.0.0:9100")
    ///     .await
    ///     .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error.to_string()))?;
    /// cluster.serve(listener, Router::new()).await
    /// # }
    /// ```
    pub fn builder(config: RaftClusterConfig) -> RaftHttpClusterBuilder<()> {
        RaftHttpClusterBuilder {
            config,
            machine: (),
            request_timeout: DEFAULT_RAFT_HTTP_REQUEST_TIMEOUT,
            peer_naming: default_peer_naming(),
            client: reqwest::Client::new(),
        }
    }

    /// Starts building one cluster node that replicates a backend-agnostic
    /// [`ConsensusStateMachine`].
    ///
    /// This is the counterpart of [`Self::builder`] for applications written
    /// against the `catga-core` consensus contract instead of the raft-rs
    /// [`RaftStateMachine`] driver contract: `machine` is wrapped in a bridge
    /// adapter that re-exposes each committed entry as an `(index, data)`
    /// apply, so callers never name the Raft backend or write the adapter
    /// themselves. Configuration, timing, transport, and probes are otherwise
    /// identical to [`Self::builder`]; the returned builder accepts the same
    /// `with_*` overrides and finishes with [`RaftHttpClusterBuilder::build`].
    ///
    /// ```no_run
    /// # use catga_axum::{RaftHttpCluster, axum::Router};
    /// # use catga_cluster::RaftClusterConfig;
    /// # use catga_core::{CatgaResult, ConsensusStateMachine};
    /// # struct Machine;
    /// # impl ConsensusStateMachine for Machine {
    /// #     fn apply(&mut self, _: u64, _: &[u8]) -> CatgaResult<()> { Ok(()) }
    /// #     fn snapshot(&self) -> CatgaResult<Vec<u8>> { Ok(Vec::new()) }
    /// #     fn restore(&mut self, _: &[u8]) -> CatgaResult<()> { Ok(()) }
    /// # }
    /// # async fn run(config: RaftClusterConfig) -> CatgaResult<()> {
    /// let cluster = RaftHttpCluster::builder_with_core_sm(config, Machine).build()?;
    /// let listener = tokio::net::TcpListener::bind("0.0.0.0:9100")
    ///     .await
    ///     .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error.to_string()))?;
    /// cluster.serve(listener, Router::new()).await
    /// # }
    /// ```
    pub fn builder_with_core_sm<M>(
        config: RaftClusterConfig,
        machine: M,
    ) -> RaftHttpClusterBuilder<ConsensusMachineBridge<M>>
    where
        M: ConsensusStateMachine + 'static,
    {
        Self::builder(config).state_machine(ConsensusMachineBridge::new(machine))
    }

    /// Returns the spawned state-machine runtime handle.
    ///
    /// Applications use it to propose commands, await the applied index, or
    /// drive checkpoints from a leader-only background task.
    pub fn runtime(&self) -> &Arc<RaftStateMachineRuntime> {
        &self.runtime
    }

    /// Returns the lock-free leadership view for this node.
    ///
    /// Reads such as `is_leader()` are only meaningful while
    /// [`RaftStateMachineRuntime::is_alive`] is true.
    pub fn coordinator(&self) -> &Arc<RaftClusterNode> {
        &self.coordinator
    }

    /// Returns a clone of the framework router for application-owned serving.
    ///
    /// The router mounts the inbound Raft route
    /// ([`crate::RAFT_MESSAGE_PATH`], guarded by the peer-identity middleware
    /// and the static inbound policy), [`RAFT_HTTP_HEALTH_PATH`], and
    /// [`RAFT_HTTP_STATUS_PATH`]. Merging application routes that reuse these
    /// paths panics, as with any Axum route conflict.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Serves until SIGINT or SIGTERM, then shuts the Raft runtime down.
    ///
    /// Equivalent to [`Self::serve_until`] with [`shutdown_signal`].
    ///
    /// # Errors
    ///
    /// Returns an error when the election cannot start or the HTTP server
    /// fails while accepting or draining connections.
    pub async fn serve(self, listener: TcpListener, routes: Router) -> CatgaResult<()> {
        self.serve_until(listener, routes, shutdown_signal()).await
    }

    /// Serves the framework router merged with application `routes`.
    ///
    /// The server starts accepting before the Raft election begins (see the
    /// type-level startup-ordering note), drains gracefully once `shutdown`
    /// resolves, and then requests the Raft runtime to stop. The runtime owner
    /// task is joined only when the caller holds no other clones of
    /// [`Self::runtime`]; with outstanding clones the stop is still requested
    /// but not awaited, since the owner task exits on its own once cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error when the election cannot start — the just-spawned
    /// server is aborted in that case — or when the HTTP server fails while
    /// accepting or draining connections.
    pub async fn serve_until(
        self,
        listener: TcpListener,
        routes: Router,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> CatgaResult<()> {
        let app = self.router.merge(routes);
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown)
                .await
                .map_err(|error| bootstrap_error(format!("raft http server: {error}")))
        });
        if let Err(error) = self.runtime.campaign().await {
            server.abort();
            let _ = server.await;
            return Err(bootstrap_error(format!("raft campaign: {error}")));
        }
        server
            .await
            .map_err(|error| bootstrap_error(format!("raft http server task: {error}")))??;
        self.runtime.shutdown();
        if let Ok(runtime) = Arc::try_unwrap(self.runtime) {
            runtime
                .join()
                .await
                .map_err(|error| bootstrap_error(format!("raft runtime join: {error}")))?;
        }
        Ok(())
    }
}

/// Builds a [`RaftHttpCluster`] from a resolved configuration.
///
/// Created by [`RaftHttpCluster::builder`]; the application state machine is
/// supplied through [`RaftHttpClusterBuilder::state_machine`], after which
/// [`RaftHttpClusterBuilder::build`] performs the wiring.
pub struct RaftHttpClusterBuilder<M> {
    config: RaftClusterConfig,
    machine: M,
    request_timeout: Duration,
    peer_naming: PeerNaming,
    client: reqwest::Client,
}

impl RaftHttpClusterBuilder<()> {
    /// Supplies the application state machine replicated by this node.
    ///
    /// The machine is owned by the runtime's single driver task, so it needs
    /// no internal locking; shared read models still synchronize with the
    /// application as usual.
    pub fn state_machine<M>(self, machine: M) -> RaftHttpClusterBuilder<M>
    where
        M: RaftStateMachine + Send + 'static,
    {
        RaftHttpClusterBuilder {
            config: self.config,
            machine,
            request_timeout: self.request_timeout,
            peer_naming: self.peer_naming,
            client: self.client,
        }
    }
}

impl<M> RaftHttpClusterBuilder<M> {
    /// Overrides the per-request timeout for outbound Raft protocol frames.
    ///
    /// Defaults to [`DEFAULT_RAFT_HTTP_REQUEST_TIMEOUT`]. See
    /// [`HttpRaftTransport::with_request_timeout`] for why this bound matters.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Overrides how a Raft member id maps to its self-asserted peer identity.
    ///
    /// The same naming produces this node's outbound identity header and the
    /// inbound policy's expected identity per peer, so it must be consistent
    /// across the cluster. The default is `node-{id}`.
    ///
    /// A self-asserted header is only safe on trusted networks or demos;
    /// production deployments must authenticate peers at the transport layer
    /// (see the crate's mTLS helpers) instead of relying on this naming.
    pub fn with_peer_naming(
        mut self,
        naming: impl Fn(u64) -> String + Send + Sync + 'static,
    ) -> Self {
        self.peer_naming = Arc::new(naming);
        self
    }

    /// Supplies the reusable HTTP client used for outbound Raft frames.
    ///
    /// Defaults to a plain [`reqwest::Client`]. Deployments with mutual TLS
    /// pass the client built by [`crate::mtls_reqwest_client`].
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = client;
        self
    }
}

impl<M> RaftHttpClusterBuilder<M>
where
    M: RaftStateMachine + Send + 'static,
{
    /// Opens the configured node, spawns its runtime, and wires HTTP ingress.
    ///
    /// The returned cluster has not campaigned yet; the election starts inside
    /// [`RaftHttpCluster::serve`] once the listener accepts, or through a
    /// manual [`RaftStateMachineRuntime::campaign`] after the application
    /// starts serving [`RaftHttpCluster::router`].
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration fails validation, the durable
    /// node cannot be opened or recovered, the inbound policy rejects the
    /// derived peer identities, or the runtime cannot start.
    pub fn build(self) -> CatgaResult<RaftHttpCluster> {
        let members = self.config.members().map_err(config_error)?;
        let timing = self.config.raft_timing().map_err(config_error)?;
        let node = self.config.open_node().map_err(config_error)?;
        let node_id = node.id();
        let driver = RaftStateMachineDriver::new(node, self.machine)
            .map_err(|error| bootstrap_error(format!("raft state-machine driver: {error}")))?;

        let peers = members
            .iter()
            .filter(|member| member.id() != node_id)
            .map(|member| (member.id(), (self.peer_naming)(member.id())));
        let policy = StaticRaftInboundPolicy::new(node_id, peers)
            .map_err(|error| config_error(format!("raft inbound policy: {error}")))?;

        let transport = HttpRaftTransport::new(self.client, members)
            .with_request_timeout(self.request_timeout)
            .with_peer_identity((self.peer_naming)(node_id));
        let runtime = Arc::new(
            RaftStateMachineRuntime::spawn(driver, Arc::new(transport), timing.tick_interval())
                .map_err(|error| bootstrap_error(format!("raft runtime: {error}")))?,
        );
        let coordinator = runtime.coordinator();
        let router = cluster_router(&runtime, &coordinator, policy);
        Ok(RaftHttpCluster {
            runtime,
            coordinator,
            router,
        })
    }
}

/// Bridges a backend-agnostic [`ConsensusStateMachine`] into the raft-rs
/// [`RaftStateMachine`] driver contract.
///
/// Applications written against the `catga-core` consensus contract are
/// replicated by [`RaftHttpCluster::builder_with_core_sm`] through this
/// adapter without naming the Raft backend: each committed
/// [`RaftCommittedEntry`] is re-exposed as an `(index, data)` apply, and
/// snapshot and restore delegate unchanged. It is the inverse of
/// [`catga_cluster::CoreStateMachine`], which exposes a [`RaftStateMachine`]
/// through the `catga-core` contract.
pub struct ConsensusMachineBridge<M>(M);

impl<M> ConsensusMachineBridge<M> {
    /// Wraps one backend-agnostic state machine.
    pub const fn new(machine: M) -> Self {
        Self(machine)
    }

    /// Returns a shared reference to the wrapped state machine.
    pub const fn inner(&self) -> &M {
        &self.0
    }

    /// Returns the wrapped state machine.
    pub fn into_inner(self) -> M {
        self.0
    }
}

impl<M: ConsensusStateMachine> RaftStateMachine for ConsensusMachineBridge<M> {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        self.0.apply(entry.index, &entry.data)
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        self.0.snapshot()
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        self.0.restore(bytes)
    }
}

/// JSON body reported by [`RAFT_HTTP_STATUS_PATH`].
#[derive(Serialize)]
struct RaftHttpStatus {
    raft_node_id: u64,
    is_leader: bool,
    leader_endpoint: Option<String>,
    raft_alive: bool,
    applied_index: Option<u64>,
}

fn cluster_router(
    runtime: &Arc<RaftStateMachineRuntime>,
    coordinator: &Arc<RaftClusterNode>,
    policy: StaticRaftInboundPolicy,
) -> Router {
    let node_id = runtime.id();
    let healthz = {
        let runtime = Arc::clone(runtime);
        move || {
            let runtime = Arc::clone(&runtime);
            async move {
                if runtime.is_alive() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }
    };
    let status = {
        let runtime = Arc::clone(runtime);
        let coordinator = Arc::clone(coordinator);
        move || {
            let runtime = Arc::clone(&runtime);
            let coordinator = Arc::clone(&coordinator);
            async move {
                Json(RaftHttpStatus {
                    raft_node_id: node_id,
                    is_leader: coordinator.is_leader(),
                    leader_endpoint: coordinator
                        .leader_endpoint()
                        .map(|leader| leader.to_string()),
                    raft_alive: runtime.is_alive(),
                    applied_index: runtime.applied_index().await.ok(),
                })
            }
        }
    };
    Router::new()
        .route(RAFT_HTTP_HEALTH_PATH, get(healthz))
        .route(RAFT_HTTP_STATUS_PATH, get(status))
        .merge(
            raft_message_route(runtime.inbox(), policy)
                .layer(middleware::from_fn(raft_peer_identity_middleware)),
        )
}

/// Resolves on SIGINT (Ctrl-C) and, on Unix, on SIGTERM.
///
/// Use it directly as an application's graceful-shutdown trigger, or rely on
/// [`RaftHttpCluster::serve`], which already wires it. If the SIGTERM handler
/// cannot be installed, only SIGINT is honored.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
