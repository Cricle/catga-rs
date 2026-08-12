//! Owns the gRPC server, sorock `RaftNode`, and redb storage of one node.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use sorock::{NodeAddress, node::RaftNode, process::RaftStorage};
use tokio::{net::TcpListener, sync::watch, task::JoinHandle};
use tonic::transport::{Server, server::TcpIncoming};

use crate::{SorockApp, SorockNodeConfig, SorockStorage, error};

/// Upper bound [`SorockNode::join`] waits for the gRPC server to drain after
/// a shutdown request before aborting the server task.
///
/// tonic's graceful shutdown waits for every accepted connection to close,
/// and a connection that never completes the HTTP/2 preface (for example a
/// lazily connecting client channel whose TCP handshake finished after its
/// only RPC was already cancelled) keeps the server waiting forever. The
/// bound turns that wedge into a forced close.
const JOIN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// A running sorock node: one gRPC server and one redb database fronting a
/// sorock `RaftNode` with one Raft process per attached shard.
///
/// sorock is a multi-Raft engine: a single node process can host many
/// independent Raft groups (shards). The configured
/// [`SorockNodeConfig::shard`] — the *primary* shard — is attached by
/// [`Self::start`]; additional shards are attached with
/// [`Self::attach_shard`]. Every shard keeps its log and ballot in its own
/// redb tables (`log-{shard}` / `ballot-{shard}`) inside the node's single
/// database, so a file-backed node holds all its shards in one file.
///
/// Dropping the node aborts the server task and stops every background thread
/// of the attached Raft processes. Prefer [`Self::request_shutdown`] plus
/// [`Self::join`] for a graceful stop.
pub struct SorockNode {
    raft_node: Arc<RaftNode>,
    storage: RaftStorage,
    attached: Mutex<BTreeSet<u32>>,
    local_addr: SocketAddr,
    advertised_uri: String,
    connect_uri: String,
    shard: u32,
    shutdown_tx: watch::Sender<bool>,
    server_task: Mutex<Option<JoinHandle<CatgaResult<()>>>>,
}

impl SorockNode {
    /// Binds the gRPC listener, builds the sorock node and storage, attaches
    /// the Raft process for `config.shard`, and spawns the server task.
    ///
    /// Must be called from within a Tokio runtime: sorock spawns its Raft
    /// threads with `tokio::spawn`.
    pub async fn start<M>(config: &SorockNodeConfig, app: SorockApp<M>) -> CatgaResult<Self>
    where
        M: ConsensusStateMachine + 'static,
    {
        config.validate()?;

        let listener = TcpListener::bind(config.bind_addr).await.map_err(|e| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("sorock node cannot bind {}: {e}", config.bind_addr),
            )
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("sorock node cannot read its bound address: {e}"),
            )
        })?;
        let connect_uri = format!("http://{}", dial_addr(local_addr));
        let advertised_uri = config
            .public_uri
            .clone()
            .unwrap_or_else(|| connect_uri.clone());
        let node_address: NodeAddress = advertised_uri.parse().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                format!("sorock advertised uri is not a valid URI: {advertised_uri}"),
            )
        })?;

        let database = match &config.storage {
            SorockStorage::InMemory => redb::Database::builder()
                .create_with_backend(redb::backends::InMemoryBackend::new()),
            SorockStorage::RedbFile(path) => redb::Database::create(path),
        }
        .map_err(|e| error::database_to_catga(&e))?;
        let storage = RaftStorage::new(database);

        let node = Self {
            raft_node: Arc::new(RaftNode::new(node_address)),
            storage,
            attached: Mutex::new(BTreeSet::new()),
            local_addr,
            advertised_uri,
            connect_uri,
            shard: config.shard,
            shutdown_tx: watch::channel(false).0,
            server_task: Mutex::new(None),
        };
        // Attach the primary shard before the server starts accepting
        // requests, so a freshly started node never answers RPCs for a shard
        // it does not serve yet.
        node.attach_shard(config.shard, app).await?;

        let service = sorock::service::raft::new(Arc::clone(&node.raft_node));
        let mut shutdown_rx = node.shutdown_tx.subscribe();
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpIncoming::from(listener), async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
                .map_err(|e| error::transport_to_catga(&e))
        });
        *node.lock_server_task() = Some(server_task);

        Ok(node)
    }

    /// Attaches a Raft process for `shard`, backed by `app` and this node's
    /// shared storage.
    ///
    /// The shard must not be attached already; attaching the same shard twice
    /// fails with [`ErrorCode::Validation`]. Each shard is an independent
    /// Raft group: its membership, log, and state machine never mix with
    /// other shards of this node.
    pub async fn attach_shard<M>(&self, shard: u32, app: SorockApp<M>) -> CatgaResult<()>
    where
        M: ConsensusStateMachine + 'static,
    {
        if self.lock_attached().contains(&shard) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!("sorock shard {shard} is already attached to this node"),
            ));
        }
        let process =
            sorock::process::RaftProcess::new(app, &self.storage, self.raft_node.get_handle(shard))
                .await
                .map_err(|e| error::anyhow_to_catga(&e))?;
        self.raft_node.attach_process(shard, process);
        self.lock_attached().insert(shard);
        Ok(())
    }

    /// Detaches the Raft process of `shard`, stopping its background threads.
    ///
    /// The shard's log and ballot stay in the node's storage, so a later
    /// [`Self::attach_shard`] of the same index resumes from the persisted
    /// state. Detaching a shard that is not attached is a no-op.
    pub fn detach_shard(&self, shard: u32) {
        if self.lock_attached().remove(&shard) {
            self.raft_node.detach_process(shard);
        }
    }

    /// Returns the shard indices currently attached to this node, in
    /// ascending order.
    pub fn attached_shards(&self) -> Vec<u32> {
        self.lock_attached().iter().copied().collect()
    }

    fn lock_attached(&self) -> MutexGuard<'_, BTreeSet<u32>> {
        self.attached.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn lock_server_task(&self) -> MutexGuard<'_, Option<JoinHandle<CatgaResult<()>>>> {
        self.server_task.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Returns the underlying sorock node. Prefer [`Self::attach_shard`] over
    /// attaching processes directly, so the node can track its shards.
    pub fn raft_node(&self) -> &Arc<RaftNode> {
        &self.raft_node
    }

    /// Returns the storage backing the Raft logs, kept alive for the lifetime
    /// of this node and shared by every attached shard (each shard keeps its
    /// own tables inside the one redb database).
    pub fn storage(&self) -> &RaftStorage {
        &self.storage
    }

    /// Returns the address the gRPC server is bound to (resolved when the
    /// config used port `0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the URI this node advertises to peers; this is the server id
    /// used in `add_member` calls.
    pub fn advertised_uri(&self) -> &str {
        &self.advertised_uri
    }

    /// Returns the primary shard index: the shard [`Self::start`] attached
    /// from [`SorockNodeConfig::shard`]. Additional shards attached through
    /// [`Self::attach_shard`] are listed by [`Self::attached_shards`].
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// Returns whether the gRPC server task is still running.
    pub fn is_running(&self) -> bool {
        self.lock_server_task()
            .as_ref()
            .is_some_and(|task| !task.is_finished())
    }

    /// Requests a graceful stop of the gRPC server. In-flight requests are
    /// allowed to finish within the [`Self::join`] drain bound; the Raft
    /// threads stop when the node is dropped or its shards are detached.
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Shuts the server down gracefully and waits for the server task.
    ///
    /// The graceful drain is bounded: tonic waits for every accepted
    /// connection to close, and a connection that never finishes the HTTP/2
    /// handshake (a client whose connect raced its own cancellation) would
    /// otherwise block this method forever. If the server is still draining
    /// after the bound, the server task is aborted — closing every connection
    /// — and `join` still returns `Ok`: the node is stopped either way.
    pub async fn join(self) -> CatgaResult<()> {
        self.request_shutdown();
        self.wait_server_task().await
    }

    /// Waits for the server task within the drain bound, taking it out of the
    /// node so the wait works through a shared reference. A missing task
    /// (already joined, or never spawned) resolves immediately.
    pub(crate) async fn wait_server_task(&self) -> CatgaResult<()> {
        let task = self.lock_server_task().take();
        if let Some(mut task) = task {
            tokio::select! {
                joined = &mut task => {
                    let result = joined.map_err(|e| {
                        CatgaError::new(
                            ErrorCode::Internal,
                            format!("sorock server task failed to join: {e}"),
                        )
                    })?;
                    result?;
                }
                _ = tokio::time::sleep(JOIN_DRAIN_TIMEOUT) => {
                    task.abort();
                    // Reap the aborted task so its resources are released
                    // before the caller proceeds (e.g. rebinding the port).
                    let _ = task.await;
                }
            }
        }
        Ok(())
    }

    /// URI a client on this host can dial, with an unspecified bind IP
    /// replaced by the loopback address.
    pub(crate) fn connect_uri(&self) -> &str {
        &self.connect_uri
    }
}

impl Drop for SorockNode {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.lock_server_task().take() {
            task.abort();
        }
    }
}

/// Replaces an unspecified bind IP with loopback for dialing: a socket bound
/// to `0.0.0.0` listens on all interfaces but is not itself a dial target.
fn dial_addr(addr: SocketAddr) -> SocketAddr {
    if addr.ip().is_unspecified() {
        SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port(),
        )
    } else {
        addr
    }
}
