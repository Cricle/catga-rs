//! [`ConsensusRuntime`] implementation backed by a local sorock client.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use catga_core::{
    CatgaError, CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine,
    ErrorCode,
};
use sorock::service::raft::client::{
    AddServerRequest, RaftClient, RemoveServerRequest, TimeoutNow, WriteRequest,
};
use tonic::transport::Endpoint;

use crate::{
    SorockApp, SorockCoordinator, SorockNode, SorockNodeConfig, SorockProposeRetry, error,
};

/// Pause between membership-call retries while a previous membership change
/// is still committing.
const MEMBERSHIP_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Per-candidate budget of one failover-watchdog `TimeoutNow` RPC.
///
/// A `TimeoutNow` on a live member resolves within a round trip; anything
/// slower means the candidate is dead or partitioned and the watchdog moves
/// on to the next member. The budget is additionally clamped to the
/// remaining [`SorockNodeConfig::request_timeout`] of the triggering
/// `propose` call.
const WATCHDOG_RPC_TIMEOUT: Duration = Duration::from_millis(500);

/// A running sorock consensus group handle, scoped to one shard.
///
/// Proposals and membership changes are issued through a gRPC client
/// connected to the **local** node; sorock forwards them to the current
/// leader internally, so the runtime works identically on leaders and
/// followers.
///
/// # Multi-shard
///
/// sorock is a multi-Raft engine: one node process can host many independent
/// Raft groups (shards), and each `SorockRuntime` is scoped to exactly one of
/// them ([`Self::shard`]). [`Self::attach_shard`] attaches another shard to
/// this runtime's node and returns a new, fully independent runtime for it:
/// own state machine, own membership registry and coordinator view, own
/// request id sequence — while sharing the node's gRPC server and storage and
/// inheriting this runtime's failover tuning
/// ([`SorockNodeConfig::request_timeout`],
/// [`SorockNodeConfig::propose_retry`],
/// [`SorockNodeConfig::failover_watchdog`], and
/// [`SorockNodeConfig::snapshot_interval`]).
///
/// Lifecycle is reference-counted over the shared node: shutting down,
/// joining, or dropping one shard runtime detaches only that shard's Raft
/// process, and the gRPC server keeps serving the remaining shards until the
/// **last** shard runtime shuts down. A runtime that never attaches extra
/// shards owns its node exclusively, so the single-shard path behaves exactly
/// as before: [`ConsensusRuntime::shutdown`] requests the server stop and
/// [`ConsensusRuntime::join`] drains it.
///
/// # Semantics notes
///
/// - sorock's write RPC resolves once the entry is committed and applied on
///   the leader, so a successful [`ConsensusRuntime::propose`] here is
///   stronger than the minimal fire-and-forget contract.
/// - Writes carry a monotonically increasing, node-and-shard-unique request
///   id (`{node_id}-{shard}-{sequence}`) which sorock deduplicates on; a
///   retried proposal with the same id is applied at most once.
/// - Membership changes are one-at-a-time (see the trait docs) and scoped to
///   the runtime's shard: sorock's `AddServer`/`RemoveServer` RPCs carry the
///   shard index, so different shards of one node may have different voter
///   sets. Bootstrap order is documented at the crate level and applies per
///   shard: the first `add_member` call on a shard must add the issuing
///   node's own advertised URI.
/// - [`ConsensusRuntime::remove_member`] needs the member's endpoint, but the
///   trait only passes the numeric id. The runtime therefore resolves ids
///   through a local registry of previous `add_member` calls; removing an id
///   that was never added through this runtime fails with
///   [`ErrorCode::NotFound`]. The registry is per shard runtime: ids added
///   through another shard are not visible here.
/// - sorock activates a new membership as soon as the configuration entry is
///   appended on the leader, before it commits. `remove_member` therefore
///   succeeds without the removed member's vote: the shrunken voter set only
///   needs its own majority. `add_member` of an unreachable node, in
///   contrast, can hang until [`SorockNodeConfig::request_timeout`] expires —
///   the enlarged voter set may lack a majority able to commit the entry.
/// - `propose` retries failures classified as leader-transient by
///   [`SorockProposeRetry::retries`] within the
///   [`SorockNodeConfig::request_timeout`] budget, so a single call rides out
///   an election instead of failing immediately. Every retry reuses the same
///   request id, which sorock deduplicates on: a retried proposal is applied
///   at most once. All other classifications stay single-shot. A `propose`
///   on an already-stopped node or a detached shard fails fast with
///   [`ErrorCode::Unavailable`] instead of spending the budget on a dead
///   local connection.
/// - With [`SorockNodeConfig::failover_watchdog`] enabled, the first
///   leader-transient failure (or a full-budget attempt timeout, in which
///   case the watchdog fires with its own bounded budget past the deadline)
///   additionally sends sorock's `TimeoutNow` RPC to a live member — this
///   node first, then the other known members — to force-promote a survivor
///   in ~2 round trips instead of waiting out the failure detector. The RPC
///   carries the runtime's shard index, so it only force-promotes within this
///   shard. Best-effort: failures are swallowed and the watchdog fires at
///   most once per `propose` call. See the config field docs for the
///   term-churn caveat.
pub struct SorockRuntime {
    node: Arc<SorockNode>,
    client: RaftClient,
    node_id: String,
    shard: u32,
    snapshot_interval: u64,
    request_timeout: Duration,
    propose_retry: SorockProposeRetry,
    failover_watchdog: bool,
    request_seq: AtomicU64,
    member_ids: Mutex<HashMap<u64, String>>,
    applied: Arc<AtomicU64>,
    coordinator: Arc<SorockCoordinator>,
}

impl SorockRuntime {
    /// Starts a sorock node from `config`, wraps `machine` in a
    /// [`SorockApp`], and connects a local client. See
    /// [`SorockNode::start`] for runtime requirements.
    ///
    /// The returned runtime is scoped to [`SorockNodeConfig::shard`];
    /// additional shards of the same node are added through
    /// [`Self::attach_shard`].
    pub async fn start<M>(config: SorockNodeConfig, machine: M) -> CatgaResult<Self>
    where
        M: ConsensusStateMachine + 'static,
    {
        let app = SorockApp::new(machine, config.snapshot_interval);
        let applied = app.applied_index_handle();
        let node = Arc::new(SorockNode::start(&config, app).await?);

        let endpoint = Endpoint::from_shared(node.connect_uri().to_owned()).map_err(|e| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("sorock local endpoint is not a valid URI: {e}"),
            )
        })?;
        let client = RaftClient::new(endpoint.connect_lazy());

        let mut seed: Vec<Arc<str>> = config.members.iter().map(|m| m.as_str().into()).collect();
        let advertised: Arc<str> = node.advertised_uri().into();
        if !seed.iter().any(|m| m.as_ref() == advertised.as_ref()) {
            seed.push(advertised);
        }
        let coordinator = Arc::new(SorockCoordinator::new(config.node_id.clone(), seed));

        Ok(Self {
            node,
            client,
            node_id: config.node_id,
            shard: config.shard,
            snapshot_interval: config.snapshot_interval,
            request_timeout: config.request_timeout,
            propose_retry: config.propose_retry,
            failover_watchdog: config.failover_watchdog,
            request_seq: AtomicU64::new(0),
            member_ids: Mutex::new(HashMap::new()),
            applied,
            coordinator,
        })
    }

    /// Returns the underlying node handle, shared by every shard runtime
    /// attached through [`Self::attach_shard`].
    pub fn node(&self) -> &SorockNode {
        &self.node
    }

    /// Returns the URI this node advertises to peers.
    pub fn advertised_uri(&self) -> &str {
        self.node.advertised_uri()
    }

    /// Returns the shard index this runtime is scoped to. Every proposal and
    /// membership change issued through this runtime carries this index.
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// Attaches `shard` to this runtime's node and returns a new runtime
    /// scoped to it.
    ///
    /// `machine` becomes the state machine of the new shard, snapshotted at
    /// this runtime's [`SorockNodeConfig::snapshot_interval`]. The new
    /// runtime shares the node's gRPC server, storage, and advertised URI,
    /// and inherits this runtime's failover tuning
    /// ([`SorockNodeConfig::request_timeout`],
    /// [`SorockNodeConfig::propose_retry`], and
    /// [`SorockNodeConfig::failover_watchdog`]); everything else — the
    /// coordinator's member view, the `add_member` id registry, and the
    /// request id sequence — starts fresh, because membership is per shard.
    ///
    /// The new shard starts with an empty membership: bootstrap it by calling
    /// `add_member` with this node's own advertised URI on the returned
    /// runtime, then add the peers one at a time, exactly like the primary
    /// shard. Attaching an already-attached shard fails with
    /// [`ErrorCode::Validation`].
    pub async fn attach_shard<M>(&self, shard: u32, machine: M) -> CatgaResult<Self>
    where
        M: ConsensusStateMachine + 'static,
    {
        let app = SorockApp::new(machine, self.snapshot_interval);
        let applied = app.applied_index_handle();
        self.node.attach_shard(shard, app).await?;

        let endpoint = Endpoint::from_shared(self.node.connect_uri().to_owned()).map_err(|e| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("sorock local endpoint is not a valid URI: {e}"),
            )
        })?;
        let client = RaftClient::new(endpoint.connect_lazy());

        let advertised: Arc<str> = self.node.advertised_uri().into();
        let coordinator = Arc::new(SorockCoordinator::new(self.node_id.clone(), [advertised]));

        Ok(Self {
            node: Arc::clone(&self.node),
            client,
            node_id: self.node_id.clone(),
            shard,
            snapshot_interval: self.snapshot_interval,
            request_timeout: self.request_timeout,
            propose_retry: self.propose_retry,
            failover_watchdog: self.failover_watchdog,
            request_seq: AtomicU64::new(0),
            member_ids: Mutex::new(HashMap::new()),
            applied,
            coordinator,
        })
    }

    fn lock_member_ids(&self) -> MutexGuard<'_, HashMap<u64, String>> {
        self.member_ids.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Issues a membership RPC, retrying transient rejections until the
    /// [`SorockNodeConfig::request_timeout`] budget expires.
    ///
    /// sorock accepts only one membership change at a time: while a previous
    /// change is still committing (including the bootstrap configuration), it
    /// rejects a new one and the client observes a dropped stream
    /// (`Cancelled`/`Unknown`, or `Unavailable` while the group has no
    /// leader). Membership changes are idempotent — adding an existing server
    /// or removing an absent one converges to the same voter set — so
    /// retrying is safe and lets callers issue changes back to back.
    async fn call_membership<F, Fut, T>(&self, mut build: F) -> CatgaResult<T>
    where
        F: FnMut(RaftClient) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send,
    {
        let deadline = std::time::Instant::now() + self.request_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(self.timeout_error());
            }
            let status = match tokio::time::timeout(remaining, build(self.client.clone())).await {
                Err(_) => return Err(self.timeout_error()),
                Ok(Ok(response)) => return Ok(response.into_inner()),
                Ok(Err(status)) => status,
            };
            let retryable = matches!(
                status.code(),
                tonic::Code::Cancelled | tonic::Code::Unknown | tonic::Code::Unavailable
            );
            if !retryable {
                return Err(error::status_to_catga(&status));
            }
            tokio::time::sleep(
                MEMBERSHIP_RETRY_INTERVAL
                    .min(deadline.saturating_duration_since(std::time::Instant::now())),
            )
            .await;
        }
    }

    fn timeout_error(&self) -> CatgaError {
        CatgaError::new(
            ErrorCode::Timeout,
            format!(
                "sorock request exceeded the {:?} deadline",
                self.request_timeout
            ),
        )
    }

    /// Best-effort force-promotion of a survivor after a dead-leader write
    /// pattern, using sorock's `TimeoutNow` RPC.
    ///
    /// Candidates are tried in order: this node first (it just answered the
    /// failed write, so it is the most likely survivor, and promoting it
    /// skips the heartbeat wait before its ballot points at the new leader),
    /// then the other members of the local coordinator view. The first
    /// candidate that accepts the RPC wins; a dead or unreachable candidate
    /// costs at most [`WATCHDOG_RPC_TIMEOUT`]. Every failure — including a
    /// `TimeoutNow` rejection on a node whose membership does not cover it —
    /// is swallowed: the watchdog only accelerates the election that the
    /// failure detector would run anyway.
    ///
    /// The force-promoted candidate starts a pre-vote with `force_vote`,
    /// which bypasses the phi gate but not the log-freshness check, so a
    /// stale candidate simply fails its election and the next candidate is
    /// tried. `deadline` bounds the whole sweep to the remaining budget of
    /// the triggering `propose` call.
    async fn fire_failover_watchdog(&self, deadline: std::time::Instant) {
        let mut candidates: Vec<String> = vec![self.node.advertised_uri().to_owned()];
        for member in self.coordinator.member_endpoints().iter() {
            if !candidates.iter().any(|c| c == member.as_ref()) {
                candidates.push(member.to_string());
            }
        }
        for uri in candidates {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return;
            }
            let Ok(endpoint) = Endpoint::from_shared(uri) else {
                continue;
            };
            let mut client = RaftClient::new(endpoint.connect_lazy());
            let call = client.send_timeout_now(TimeoutNow {
                shard_index: self.shard,
            });
            let budget = WATCHDOG_RPC_TIMEOUT.min(remaining);
            if let Ok(Ok(_)) = tokio::time::timeout(budget, call).await {
                return;
            }
        }
    }
}

impl ConsensusRuntime for SorockRuntime {
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()> {
        // A stopped server or a detached shard can never serve a write: fail
        // fast instead of spending the whole request budget retrying a dead
        // local connection.
        if !self.node.is_running() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "sorock node is shut down",
            ));
        }
        if !self.node.attached_shards().contains(&self.shard) {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                format!("sorock shard {} is detached", self.shard),
            ));
        }
        let seq = self.request_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let request = WriteRequest {
            shard_index: self.shard,
            message: Bytes::from(data),
            // The shard is part of the id because every shard runtime of one
            // node runs its own sequence, and sorock deduplicates per shard.
            request_id: format!("{}-{}-{seq}", self.node_id, self.shard),
        };
        let deadline = std::time::Instant::now() + self.request_timeout;
        let mut attempts = 0_u32;
        let mut watchdog_fired = false;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(self.timeout_error());
            }
            attempts += 1;
            // Every attempt reuses the same request id, which sorock
            // deduplicates on, so a retried write is applied at most once.
            let status =
                match tokio::time::timeout(remaining, self.client.clone().write(request.clone()))
                    .await
                {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(status)) => status,
                    Err(_) => {
                        // The attempt hung (typically a write forwarded to a
                        // dead leader) and consumed the rest of the budget.
                        // This is the strongest dead-leader signal, so the
                        // watchdog still fires — with its own bounded budget,
                        // since the request deadline is already spent. In this
                        // path a `propose` call may therefore outlive its
                        // deadline by at most [`WATCHDOG_RPC_TIMEOUT`].
                        if self.failover_watchdog && !watchdog_fired {
                            let watchdog_budget = std::time::Instant::now() + WATCHDOG_RPC_TIMEOUT;
                            self.fire_failover_watchdog(watchdog_budget).await;
                        }
                        return Err(self.timeout_error());
                    }
                };
            let error = error::status_to_catga(&status);
            if !self.propose_retry.retries(error.code())
                || attempts >= self.propose_retry.max_attempts
            {
                return Err(error);
            }
            if self.failover_watchdog && !watchdog_fired {
                watchdog_fired = true;
                self.fire_failover_watchdog(deadline).await;
            }
            let backoff = self
                .propose_retry
                .backoff
                .min(deadline.saturating_duration_since(std::time::Instant::now()));
            tokio::time::sleep(backoff).await;
        }
    }

    async fn add_member(&self, id: u64, endpoint: String) -> CatgaResult<()> {
        let shard = self.shard;
        let server_id = endpoint.clone();
        self.call_membership(move |mut client| {
            let server_id = server_id.clone();
            async move {
                client
                    .add_server(AddServerRequest {
                        shard_index: shard,
                        server_id,
                    })
                    .await
            }
        })
        .await?;
        self.lock_member_ids().insert(id, endpoint.clone());
        self.coordinator.add_endpoint(&endpoint);
        Ok(())
    }

    async fn remove_member(&self, id: u64) -> CatgaResult<()> {
        // Quorum nuance: sorock activates a new membership as soon as the
        // configuration entry is *appended* on the leader, before it is
        // committed. A removal therefore only needs the *remaining* members
        // to commit — removing a dead member succeeds because the shrunken
        // voter set keeps quorum. An *addition* of an unreachable member can
        // instead hang until the request deadline: the enlarged voter set may
        // no longer have a majority available to commit the entry.
        let endpoint = self.lock_member_ids().get(&id).cloned().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                format!(
                    "sorock member id {id} is unknown to this runtime; \
                     only ids previously passed to add_member can be removed"
                ),
            )
        })?;
        let shard = self.shard;
        let server_id = endpoint.clone();
        self.call_membership(move |mut client| {
            let server_id = server_id.clone();
            async move {
                client
                    .remove_server(RemoveServerRequest {
                        shard_index: shard,
                        server_id,
                    })
                    .await
            }
        })
        .await?;
        self.lock_member_ids().remove(&id);
        self.coordinator.remove_endpoint(&endpoint);
        Ok(())
    }

    async fn applied_index(&self) -> CatgaResult<u64> {
        Ok(self.applied.load(Ordering::Acquire))
    }

    fn is_alive(&self) -> bool {
        self.node.is_running() && self.node.attached_shards().contains(&self.shard)
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        Arc::clone(&self.coordinator) as Arc<dyn ConsensusCoordinator>
    }

    fn shutdown(&self) {
        if Arc::strong_count(&self.node) == 1 {
            // The last shard runtime owns the node exclusively: stop the
            // shared server gracefully.
            self.node.request_shutdown();
        } else {
            // Other shards still need the server: retire only this shard's
            // Raft process, so its group stops hearing from this member.
            self.node.detach_shard(self.shard);
        }
    }

    async fn join(self) -> CatgaResult<()> {
        if Arc::strong_count(&self.node) == 1 {
            self.node.request_shutdown();
            self.node.wait_server_task().await?;
        } else {
            self.node.detach_shard(self.shard);
        }
        Ok(())
    }
}

impl Drop for SorockRuntime {
    fn drop(&mut self) {
        // Idempotent: shutdown/join may have detached the shard already. When
        // this was the last shard runtime, the node itself drops right after,
        // which aborts the server task.
        self.node.detach_shard(self.shard);
    }
}
