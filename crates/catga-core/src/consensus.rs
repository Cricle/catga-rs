//! Backend-agnostic consensus contracts.
//!
//! These traits describe the minimal surface a replicated-consensus backend
//! (single Raft group, multi-Raft, or otherwise) exposes so that application
//! code — such as a replicated key/value store — depends only on `catga-core`
//! contracts and never on a concrete backend crate. A thin bridge in each
//! backend crate adapts its runtime to these traits.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Poll cadence of the default [`ConsensusRuntime::propose_and_wait`]
/// implementation between [`ConsensusRuntime::applied_index`] reads.
const PROPOSE_AND_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Backend-agnostic replicated state machine.
///
/// A consensus backend feeds committed log entries to [`Self::apply`] in
/// strictly increasing `index` order, exactly once per committed entry.
/// Implementations must be deterministic: applying the same sequence of
/// `(index, data)` pairs to a fresh instance always yields the same state.
pub trait ConsensusStateMachine: Send {
    /// Applies one newly committed entry at log position `index`.
    ///
    /// Returning an error is terminal for the owning runtime: backends stop
    /// before acknowledging the failed entry so a durable node can recover
    /// and replay it on restart.
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()>;

    /// Encodes the complete state after the last successfully applied entry.
    fn snapshot(&self) -> CatgaResult<Vec<u8>>;

    /// Replaces the entire state from `data` produced by [`Self::snapshot`],
    /// before any subsequent log replay.
    fn restore(&mut self, data: &[u8]) -> CatgaResult<()>;
}

/// Cluster readiness and leadership view of one node.
///
/// Reads are cheap, lock-free snapshots of the node's latest known cluster
/// state; they are only meaningful while the owning
/// [`ConsensusRuntime`] is alive.
pub trait ConsensusCoordinator: Send + Sync {
    /// Returns this node's stable identifier.
    fn node_id(&self) -> &str;

    /// Returns whether this node currently holds leadership.
    fn is_leader(&self) -> bool;

    /// Returns the endpoint of the elected leader when it is known.
    fn leader_endpoint(&self) -> Option<Arc<str>>;

    /// Returns a compact snapshot of known member endpoints.
    fn member_endpoints(&self) -> Arc<[Arc<str>]>;
}

/// A running consensus group handle: proposals, membership, progress, and
/// lifecycle.
///
/// All consensus mutations are serialized by the backend's owner task, so the
/// handle itself is `Sync` and cheap to share. The trait is object-safe, so a
/// backend can be erased behind `Arc<dyn ConsensusRuntime>`.
#[async_trait]
pub trait ConsensusRuntime: Send + Sync {
    /// Proposes one application command through the currently elected leader.
    ///
    /// This is fire-and-forget: `Ok(())` means the entry was locally accepted
    /// by the leader, **not** that it was committed or applied. Callers
    /// observe durable progress through [`Self::applied_index`], typically
    /// combined with an application-level operation id inside `data`.
    /// Proposing on a node without leadership returns an error.
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()>;

    /// Proposes one application command and resolves with the applied index
    /// once the entry is committed and applied by the local state machine.
    ///
    /// Unlike fire-and-forget [`Self::propose`], the returned index is at
    /// least the index of the proposed entry, so the entry's effects are
    /// observable in the state machine when the call resolves. A node without
    /// leadership fails fast with the same routine error as [`Self::propose`];
    /// a stopped runtime fails with [`ErrorCode::Unavailable`]; expiry of
    /// `timeout` fails with [`ErrorCode::Timeout`] — the proposal may still be
    /// applied later.
    ///
    /// If leadership changes while the entry is in flight, its log slot can be
    /// overwritten by another entry; the resolved index then reflects that
    /// slot rather than necessarily this proposal. Callers needing
    /// exactly-once semantics must embed an application-level operation id in
    /// `data` and verify it against the state machine.
    ///
    /// The default implementation polls [`Self::applied_index`] every 10 ms
    /// once [`Self::propose`] accepts the entry; backends with an
    /// applied-notification path override it with a push-based wait.
    async fn propose_and_wait(&self, data: Vec<u8>, timeout: Duration) -> CatgaResult<u64> {
        let baseline = self.applied_index().await?;
        self.propose(data).await?;
        let started = std::time::Instant::now();
        loop {
            let applied = self.applied_index().await?;
            if applied > baseline {
                return Ok(applied);
            }
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return Err(CatgaError::new(
                    ErrorCode::Timeout,
                    "consensus proposal was not applied before the deadline",
                ));
            }
            tokio::time::sleep(PROPOSE_AND_WAIT_POLL_INTERVAL.min(timeout - elapsed)).await;
        }
    }

    /// Proposes adding one member with its externally reachable `endpoint`.
    ///
    /// Membership changes are applied one at a time: wait until a change is
    /// observable through the coordinator before issuing the next one,
    /// because backends may silently drop a change proposed while another is
    /// pending. Learners, leadership transfer, and multi-member changes in a
    /// single step are intentionally not part of this contract.
    async fn add_member(&self, id: u64, endpoint: String) -> CatgaResult<()>;

    /// Proposes removing one member from the group.
    ///
    /// The same one-change-at-a-time discipline as [`Self::add_member`]
    /// applies. Removing the last member is rejected; removing this node
    /// itself is permitted, after which it can no longer campaign.
    async fn remove_member(&self, id: u64) -> CatgaResult<()>;

    /// Returns the greatest log index applied to the application state
    /// machine.
    async fn applied_index(&self) -> CatgaResult<u64>;

    /// Returns whether the backend's owner task is still running.
    fn is_alive(&self) -> bool;

    /// Returns the leadership and membership view for this node.
    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator>;

    /// Requests a graceful stop of the owner task.
    fn shutdown(&self);

    /// Requests shutdown and awaits the owner task without consuming the
    /// handle (usable through `Arc`). The default implementation only
    /// requests shutdown; backends with background tasks should override it.
    async fn shutdown_and_join(&self) -> CatgaResult<()> {
        self.shutdown();
        Ok(())
    }

    /// Waits for the owner task and returns its terminal status.
    ///
    /// Call [`Self::shutdown`] first for a graceful stop; `join` then
    /// resolves once the owner task has finished draining.
    ///
    /// The receiver is `Box<Self>` rather than a bare `self` so the trait
    /// stays object-safe: concrete runtimes join through
    /// `Box::new(runtime).join().await`, and a boxed trait object
    /// (`Box<dyn ConsensusRuntime>`) joins through the same call. Shared
    /// `Arc` handles cannot be joined directly; call [`Self::shutdown`] and
    /// drop them instead.
    async fn join(self: Box<Self>) -> CatgaResult<()>;
}
