//! Configuration for one sorock cluster node.

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Default shard a [`crate::SorockRuntime`] attaches to.
pub const DEFAULT_SHARD_INDEX: u32 = 0;

/// Default deadline applied to client-side gRPC calls.
///
/// Tuned down from 10s to 2s for failover: when the leader dies, followers
/// keep forwarding writes to it until their failure detector fires, and a
/// forwarded write only resolves once this budget expires. 2s keeps the
/// balanced failover profile in the ~4.2s band (see the crate-level docs)
/// while leaving ample headroom for healthy writes, which complete in
/// milliseconds on any sane network.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Default total number of write attempts a single
/// [`ConsensusRuntime::propose`](catga_core::ConsensusRuntime::propose) call
/// makes before giving up.
///
/// One attempt is always made; the remaining `max_attempts - 1` are retries
/// of failures classified as retryable by [`SorockProposeRetry::retries`],
/// bounded additionally by the [`SorockNodeConfig::request_timeout`]
/// deadline. With the default 200ms backoff and 2s deadline the cap and the
/// deadline expire at roughly the same time for instantly-failing attempts.
pub const DEFAULT_PROPOSE_MAX_ATTEMPTS: u32 = 10;

/// Default pause between write attempts inside one
/// [`ConsensusRuntime::propose`](catga_core::ConsensusRuntime::propose)
/// call.
pub const DEFAULT_PROPOSE_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Retry policy of
/// [`ConsensusRuntime::propose`](catga_core::ConsensusRuntime::propose) for
/// failures that typically resolve on their own once the group re-elects.
///
/// A proposal carries a node-unique request id which sorock deduplicates on,
/// so retrying the same proposal within one `propose` call cannot apply the
/// entry twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SorockProposeRetry {
    /// Total number of write attempts, including the first one. `1` disables
    /// retries (single-shot). Must be at least `1`; validated at startup.
    pub max_attempts: u32,
    /// Pause between attempts. The pause is clamped to the remaining
    /// [`SorockNodeConfig::request_timeout`] budget, so a `propose` call
    /// never outlives its deadline by more than one in-flight attempt.
    pub backoff: Duration,
}

impl Default for SorockProposeRetry {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_PROPOSE_MAX_ATTEMPTS,
            backoff: DEFAULT_PROPOSE_RETRY_BACKOFF,
        }
    }
}

impl SorockProposeRetry {
    /// Returns whether a failed attempt classified as `code` is retried.
    ///
    /// Retryable are exactly the leader-transient classifications:
    /// [`ErrorCode::Transient`] and [`ErrorCode::Cancelled`] (sorock aborts
    /// in-flight writes while no leader is known, or when its forwarding
    /// handler tears the connection down) and [`ErrorCode::Unavailable`]
    /// (the forwarded-to leader is unreachable). Every other classification
    /// — validation, conflicts, internal errors, and client-side deadline
    /// exhaustion — stays single-shot.
    pub fn retries(&self, code: ErrorCode) -> bool {
        matches!(
            code,
            ErrorCode::Transient | ErrorCode::Unavailable | ErrorCode::Cancelled
        )
    }
}

/// Storage backend for the Raft log and ballot of a node.
///
/// sorock 0.12 is redb-only; this selects where the redb database lives.
#[derive(Debug, Clone, Default)]
pub enum SorockStorage {
    /// Volatile in-memory storage (redb `InMemoryBackend`). Suited for tests
    /// and ephemeral nodes; all Raft state is lost on shutdown.
    #[default]
    InMemory,
    /// A redb database file at the given path, created when missing.
    RedbFile(PathBuf),
}

/// Configuration of one sorock node.
///
/// Construct with [`Self::new`] and adjust the public fields before passing
/// the config to [`crate::SorockRuntime::start`] or [`crate::SorockNode::start`].
#[derive(Debug, Clone)]
pub struct SorockNodeConfig {
    /// Stable, human-readable identifier of this node, reported through the
    /// coordinator's `node_id` and used as the prefix of proposal request ids
    /// (which sorock deduplicates on).
    pub node_id: String,
    /// Address the gRPC server binds to. Port `0` asks the OS for a free port;
    /// the effective address is available through
    /// [`crate::SorockNode::local_addr`] after startup.
    pub bind_addr: SocketAddr,
    /// URI this node advertises to peers (scheme included, for example
    /// `http://raft-1.internal:7000`). Defaults to `http://{bound address}`,
    /// with an unspecified bind IP replaced by `127.0.0.1`.
    pub public_uri: Option<String>,
    /// Member endpoints this node knows about at startup. The list only seeds
    /// the coordinator's member view; actual membership is established through
    /// `add_member` calls (see the crate-level formation order).
    pub members: Vec<String>,
    /// Primary shard this node attaches its first Raft process to. sorock is
    /// multi-Raft; every node of a group must use the same shard index, and
    /// additional shards of the same node are attached later through
    /// [`crate::SorockRuntime::attach_shard`] (or
    /// [`crate::SorockNode::attach_shard`]). Defaults to
    /// [`DEFAULT_SHARD_INDEX`].
    pub shard: u32,
    /// Storage backend for the Raft log and ballot.
    pub storage: SorockStorage,
    /// Number of applied user entries between application-driven snapshots.
    ///
    /// `0` (the default) disables snapshotting entirely. Keep it disabled for
    /// [`SorockStorage::RedbFile`] nodes: snapshots are held in memory, so a
    /// restarted node whose log was compacted cannot restore them.
    pub snapshot_interval: u64,
    /// Deadline applied to each client-side gRPC call issued by
    /// [`crate::SorockRuntime`], and the total budget of one `propose` call
    /// including its retries (see [`Self::propose_retry`]). sorock's client
    /// API has no built-in timeout and a write on a leaderless group would
    /// otherwise wait indefinitely.
    pub request_timeout: Duration,
    /// Retry policy for `propose` failures that typically resolve once the
    /// group re-elects a leader. Defaults to [`SorockProposeRetry::default`]
    /// (retry within the request deadline); set `max_attempts` to `1` for
    /// single-shot proposals.
    pub propose_retry: SorockProposeRetry,
    /// Enables the failover watchdog: when a `propose` attempt fails with a
    /// dead-leader pattern, the runtime sends sorock's `TimeoutNow` RPC to a
    /// live member (this node first, then the other known members) to
    /// force-promote a survivor instead of waiting out the phi-accrual
    /// failure detector. This cuts failover to roughly one request budget
    /// plus an election round trip (~2.1s locally with the 2s default
    /// timeout, versus ~4.2s without; see the crate-level docs): the first
    /// post-kill attempt typically hangs until the client deadline, and the
    /// watchdog fires by then at the latest.
    ///
    /// Off by default. **Caveat:** the trigger cannot distinguish a dead
    /// leader from a slow or network-partitioned one. Firing on a
    /// partitioned-but-alive leader force-starts a new term while the old
    /// leader still serves its partition; sorock's pre-vote bounds the
    /// disruption to a term bump, but clients that cannot tolerate that
    /// churn should keep the watchdog disabled.
    pub failover_watchdog: bool,
}

impl SorockNodeConfig {
    /// Creates a config with defaults: shard 0, in-memory storage, snapshots
    /// disabled, [`DEFAULT_REQUEST_TIMEOUT`],
    /// [`SorockProposeRetry::default`], and the failover watchdog disabled.
    pub fn new(node_id: impl Into<String>, bind_addr: SocketAddr) -> Self {
        Self {
            node_id: node_id.into(),
            bind_addr,
            public_uri: None,
            members: Vec::new(),
            shard: DEFAULT_SHARD_INDEX,
            storage: SorockStorage::InMemory,
            snapshot_interval: 0,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            propose_retry: SorockProposeRetry::default(),
            failover_watchdog: false,
        }
    }

    /// Validates the fields that cannot be checked lazily.
    pub(crate) fn validate(&self) -> CatgaResult<()> {
        if self.node_id.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "sorock node id must not be empty",
            ));
        }
        if let Some(uri) = &self.public_uri
            && uri.parse::<tonic::transport::Uri>().is_err()
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!("sorock public uri is not a valid URI: {uri}"),
            ));
        }
        if self.propose_retry.max_attempts == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "sorock propose retry max_attempts must be at least 1",
            ));
        }
        Ok(())
    }
}
