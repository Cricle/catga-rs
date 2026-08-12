//! Bridge adapting the `raft-rs` runtime to the backend-agnostic `catga-core`
//! consensus contracts.
//!
//! Generic application code — such as a replicated key/value store — depends
//! only on the `catga-core` consensus traits and never on a concrete backend
//! crate. This module wires this crate's single-group Raft runtime
//! ([`RaftStateMachineRuntime`]), its lock-free coordinator
//! ([`RaftClusterNode`]), and application state machines ([`RaftStateMachine`])
//! into [`ConsensusRuntime`], [`ConsensusCoordinator`], and
//! [`ConsensusStateMachine`] so such applications can run on this backend
//! without naming it.

use std::{sync::Arc, time::Duration};

use catga_core::{
    CatgaError, CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine,
    ErrorCode,
};

use crate::{
    ClusterCoordinator, MemoryClusterNode, RaftClusterNode, RaftCommittedEntry, RaftNodeError,
    RaftStateMachine, RaftStateMachineError, RaftStateMachineRuntime, RaftStateMachineRuntimeError,
};

impl ConsensusCoordinator for RaftClusterNode {
    fn node_id(&self) -> &str {
        ClusterCoordinator::node_id(self)
    }

    fn is_leader(&self) -> bool {
        ClusterCoordinator::is_leader(self)
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        ClusterCoordinator::leader_endpoint(self)
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        ClusterCoordinator::member_endpoints(self)
    }
}

impl ConsensusCoordinator for MemoryClusterNode {
    fn node_id(&self) -> &str {
        ClusterCoordinator::node_id(self)
    }

    fn is_leader(&self) -> bool {
        ClusterCoordinator::is_leader(self)
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        ClusterCoordinator::leader_endpoint(self)
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        ClusterCoordinator::member_endpoints(self)
    }
}

/// Adapts an application [`RaftStateMachine`] to the backend-agnostic
/// [`ConsensusStateMachine`] contract.
///
/// This adapter exists so generic applications can drive a
/// [`RaftStateMachine`] through the `catga-core` consensus traits without
/// naming the `raft-rs` backend: [`ConsensusStateMachine::apply`] receives a
/// bare `(index, data)` pair, which the adapter wraps into the
/// [`RaftCommittedEntry`] the machine expects before delegating. Snapshot and
/// restore delegate unchanged.
///
/// ```
/// use catga_cluster::{CoreStateMachine, RaftCommittedEntry, RaftStateMachine};
/// use catga_core::{CatgaResult, ConsensusStateMachine};
///
/// #[derive(Default)]
/// struct LastIndex(u64);
///
/// impl RaftStateMachine for LastIndex {
///     fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
///         self.0 = entry.index;
///         Ok(())
///     }
///     fn snapshot(&self) -> CatgaResult<Vec<u8>> {
///         Ok(self.0.to_le_bytes().to_vec())
///     }
///     fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
///         let raw: [u8; 8] = bytes.try_into().unwrap_or_default();
///         self.0 = u64::from_le_bytes(raw);
///         Ok(())
///     }
/// }
///
/// # fn run() -> CatgaResult<()> {
/// // Generic code only names the `catga-core` trait, never the Raft backend.
/// let mut machine: CoreStateMachine<LastIndex> = CoreStateMachine::new(LastIndex::default());
/// ConsensusStateMachine::apply(&mut machine, 7, b"set a=1")?;
/// assert_eq!(machine.inner().0, 7);
///
/// let snapshot = ConsensusStateMachine::snapshot(&machine)?;
/// ConsensusStateMachine::restore(&mut machine, &snapshot)?;
/// assert_eq!(machine.inner().0, 7);
/// # Ok(())
/// # }
/// # run().expect("bridge example");
/// ```
pub struct CoreStateMachine<M>(M);

impl<M> CoreStateMachine<M> {
    /// Wraps one application state machine.
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

impl<M> ConsensusStateMachine for CoreStateMachine<M>
where
    M: RaftStateMachine + Send,
{
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let entry = RaftCommittedEntry {
            index,
            data: data.to_vec(),
        };
        self.0.apply(&entry)
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        self.0.snapshot()
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        self.0.restore(data)
    }
}

impl ConsensusRuntime for RaftStateMachineRuntime {
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()> {
        RaftStateMachineRuntime::propose(self, data)
            .await
            .map_err(map_runtime_error)
    }

    async fn propose_and_wait(&self, data: Vec<u8>, timeout: Duration) -> CatgaResult<u64> {
        RaftStateMachineRuntime::propose_and_wait(self, data, timeout)
            .await
            .map_err(map_runtime_error)
    }

    async fn add_member(&self, id: u64, endpoint: String) -> CatgaResult<()> {
        RaftStateMachineRuntime::add_voter(self, id, endpoint)
            .await
            .map_err(map_runtime_error)
    }

    async fn remove_member(&self, id: u64) -> CatgaResult<()> {
        RaftStateMachineRuntime::remove_voter(self, id)
            .await
            .map_err(map_runtime_error)
    }

    async fn applied_index(&self) -> CatgaResult<u64> {
        RaftStateMachineRuntime::applied_index(self)
            .await
            .map_err(map_runtime_error)
    }

    fn is_alive(&self) -> bool {
        RaftStateMachineRuntime::is_alive(self)
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        RaftStateMachineRuntime::coordinator(self)
    }

    fn shutdown(&self) {
        RaftStateMachineRuntime::shutdown(self);
    }

    async fn join(self) -> CatgaResult<()> {
        RaftStateMachineRuntime::join(self)
            .await
            .map_err(map_runtime_error)
    }
}

/// Maps a runtime failure onto the stable `catga-core` error categories.
///
/// Routine caller errors keep their semantics: a dropped proposal (no elected
/// leader) and a full pending-commit queue are [`ErrorCode::Unavailable`] and
/// therefore retryable, an expired propose-and-wait deadline is
/// [`ErrorCode::Timeout`], a membership change rejected by pre-validation is
/// [`ErrorCode::Conflict`], and a failure already classified by the
/// application passes through unchanged.
fn map_runtime_error(error: RaftStateMachineRuntimeError) -> CatgaError {
    let code = match &error {
        RaftStateMachineRuntimeError::Stopped => ErrorCode::Unavailable,
        RaftStateMachineRuntimeError::Timeout => ErrorCode::Timeout,
        RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped) => ErrorCode::Unavailable,
        RaftStateMachineRuntimeError::Raft(raft::Error::ConfChangeError(_)) => ErrorCode::Conflict,
        RaftStateMachineRuntimeError::Node(RaftNodeError::ZeroMemberId) => ErrorCode::Validation,
        RaftStateMachineRuntimeError::Node(RaftNodeError::PendingCommitCapacity { .. }) => {
            ErrorCode::Unavailable
        }
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::Application(
            application,
        )) => return application.clone(),
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::NothingApplied) => {
            ErrorCode::Conflict
        }
        RaftStateMachineRuntimeError::Transport(_) => ErrorCode::TransportFailed,
        RaftStateMachineRuntimeError::InvalidTickInterval
        | RaftStateMachineRuntimeError::Raft(_)
        | RaftStateMachineRuntimeError::Node(_)
        | RaftStateMachineRuntimeError::StateMachine(_)
        | RaftStateMachineRuntimeError::Task(_) => ErrorCode::Internal,
    };
    CatgaError::new(code, error.to_string())
}
