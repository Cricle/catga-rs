//! Multi-Raft consensus backend for `catga-core` built on [sorock](https://crates.io/crates/sorock) 0.12.
//!
//! This crate adapts sorock Raft processes (shards of a sorock multi-Raft
//! node) to the backend-agnostic contracts in `catga_core`:
//!
//! - [`SorockApp`] adapts an application [`ConsensusStateMachine`] to sorock's
//!   `RaftApp` trait, including its byte-stream snapshot protocol.
//! - [`SorockNode`] owns the gRPC server, the sorock `RaftNode`, and the redb
//!   storage of one cluster node.
//! - [`SorockRuntime`] implements [`ConsensusRuntime`] on top of a local sorock
//!   client, scoped to one shard, and [`SorockCoordinator`] implements
//!   [`ConsensusCoordinator`].
//!
//! # Cluster formation order
//!
//! sorock forms groups imperatively; there is no static membership list:
//!
//! 1. Start every node with [`SorockRuntime::start`]. Each node only serves its
//!    own shard at this point and knows no peers.
//! 2. Bootstrap the group by calling `add_member` with the **first node's own
//!    advertised URI** on the first node's runtime. sorock turns a self-add on
//!    an empty membership into a single-node bootstrap and immediately elects
//!    itself leader.
//! 3. Add every remaining node with one `add_member` call per node, issued in
//!    sequence. Membership changes are one-at-a-time: wait until a change is
//!    observable (for example through the coordinator's member view) before
//!    issuing the next one.
//!
//! Any node may issue membership changes after bootstrap; sorock forwards them
//! to the current leader internally.
//!
//! [`SorockRuntimeBuilder`] automates exactly this sequence for CLI-shaped
//! topologies (base port plus node index): it derives the node configuration
//! and, on the bootstrap node, performs the self-add and peer joins with
//! retries so process start order does not matter.
//!
//! # Multi-shard model
//!
//! sorock 0.12 is a true multi-Raft engine: one node process can host many
//! independent Raft groups, called *shards*. This crate exposes that through
//! [`SorockRuntime::attach_shard`] (and the lower-level
//! [`SorockNode::attach_shard`]); [`SorockRuntime::start`] alone keeps the
//! node single-shard, exactly as before.
//!
//! - **Every shard is an independent Raft group** with its own log, ballot,
//!   membership, leader election, and state machine. A write proposed on one
//!   shard is never visible on another, and every shard elects and fails over
//!   on its own.
//! - **Membership is per shard.** `add_member`/`remove_member` carry the
//!   runtime's shard index (sorock's `AddServer`/`RemoveServer` RPCs take a
//!   `shard_index`), so different shards of the same process may have
//!   different voter sets. The formation order above applies to each shard
//!   independently: bootstrap every shard with a self-add on that shard's
//!   runtime.
//! - **Storage is per node, namespaced per shard.** sorock keeps each shard's
//!   log and ballot in dedicated redb tables (`log-{shard}`, `ballot-{shard}`)
//!   inside the node's single database, so a [`SorockStorage::RedbFile`] node
//!   holds all its shards in one file.
//! - **The coordinator view is per runtime**, hence per shard: each shard
//!   runtime tracks only the members added through it.
//! - **Failover tuning is inherited per shard.** A runtime produced by
//!   `attach_shard` keeps the originating runtime's
//!   [`SorockNodeConfig::request_timeout`],
//!   [`SorockNodeConfig::propose_retry`], and
//!   [`SorockNodeConfig::failover_watchdog`]; the watchdog's `TimeoutNow` RPC
//!   carries the shard index and only force-promotes within that shard.
//! - **Lifecycle is reference-counted over the shared node.** Shutting down,
//!   joining, or dropping one shard runtime detaches only that shard's Raft
//!   process; the gRPC server keeps serving the remaining shards and stops
//!   when the last shard runtime shuts down.
//!
//! # Known limitations of sorock 0.12
//!
//! - **Leadership is not observable.** sorock 0.12 exposes no public API to
//!   query the election state or the current leader, and the gRPC request type
//!   needed to poll membership is not exported. `SorockCoordinator::is_leader`
//!   therefore always returns `false` and
//!   `SorockCoordinator::leader_endpoint` always returns `None`. Applications
//!   that need leader-only work must fence it externally.
//! - **No learners or joint consensus.** Membership changes replace the voter
//!   set in a single log entry; a brand-new node must catch up through log
//!   replication (or a snapshot) before it can vote.
//! - **Snapshots are kept in memory** by [`SorockApp`]. Keep
//!   [`SorockNodeConfig::snapshot_interval`] at `0` (disabled) for file-backed
//!   nodes: a restarted node whose log was compacted past its in-memory
//!   snapshots cannot restore them.
//! - **Follower apply visibility lags by the heartbeat cadence.** A
//!   successful `propose` resolves once the entry is committed and applied on
//!   the leader (a few milliseconds on loopback; see
//!   `tests/write_perf.rs`). Followers, however, learn the leader's commit
//!   index only from heartbeats: sorock 0.12 queues one heartbeat per
//!   follower every 300ms, the per-peer heartbeat multiplexer drains its
//!   buffer every 300ms, and a follower's commit/apply threads poll on a
//!   100ms fallback because heartbeat receipt pushes no event. A write
//!   observed through a *follower's* state machine (or through
//!   `applied_index` on that node) therefore lags by roughly 300–800ms.
//!   sorock 0.12 exposes no cadence configuration, so this is a hard floor of
//!   the dependency, not of this adapter; applications that need
//!   read-your-writes must pin the read to the proposing node.
//!
//! # Failover behavior and tuning
//!
//! When the leader dies, sorock's own machinery is fast: the phi-accrual
//! failure detector (threshold 12.0 against 300ms heartbeats) plus the
//! election tick lands a new leader in roughly 1.5–3s. The dominant term is
//! client-side: followers keep forwarding writes to the dead leader
//! (`ballot.voted_for`), and a forwarded write only resolves when the
//! transport gives up (sorock's internal h2 pong watchdog defaults to 20s) or
//! the client deadline fires. This crate tunes that client side:
//!
//! - [`SorockNodeConfig::request_timeout`] defaults to 2s (was 10s), capping
//!   every client-visible attempt, and
//! - [`SorockNodeConfig::propose_retry`] retries leader-transient failures
//!   inside that budget, so one `propose` call rides out the election instead
//!   of failing immediately.
//!
//! Expected kill-leader failover (write succeeds again) on loopback,
//! exercised by `tests/failover_tuning.rs`:
//!
//! - **Balanced defaults: ~4.2s.** The first post-kill attempt typically
//!   hangs until the client deadline — the follower forwards the write to
//!   the dead leader and its handler dies mid-stream, so the client stream
//!   only resolves at the 2s timeout. The next `propose` call's retry loop
//!   then rides out the phi-gated election (~1.5–3s of heartbeat silence
//!   plus the election round trip), landing the first successful write at
//!   roughly two request budgets.
//! - **With [`SorockNodeConfig::failover_watchdog`]: ~2.1s**, i.e. one
//!   request budget plus an election round trip. When the hung first attempt
//!   hits the deadline, the watchdog force-promotes a survivor through
//!   sorock's `TimeoutNow` RPC (~2 round trips instead of the
//!   failure-detector window), so the immediately following retry succeeds.
//!
//! The watchdog is off by default: it cannot distinguish a dead leader from
//! a partitioned one, and force-promoting on a partitioned-but-alive leader
//! costs an avoidable term bump (sorock's pre-vote bounds the disruption).
//!
//! [`ConsensusStateMachine`]: catga_core::ConsensusStateMachine
//! [`ConsensusRuntime`]: catga_core::ConsensusRuntime
//! [`ConsensusCoordinator`]: catga_core::ConsensusCoordinator

mod app;
mod builder;
mod config;
mod coordinator;
pub mod error;
mod node;
mod runtime;

pub use app::SorockApp;
pub use builder::{CLI_GRPC_PORT_OFFSET, SorockRuntimeBuilder};
pub use config::{
    DEFAULT_PROPOSE_MAX_ATTEMPTS, DEFAULT_PROPOSE_RETRY_BACKOFF, DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_SHARD_INDEX, SorockNodeConfig, SorockProposeRetry, SorockStorage,
};
pub use coordinator::SorockCoordinator;
pub use node::SorockNode;
pub use runtime::SorockRuntime;
