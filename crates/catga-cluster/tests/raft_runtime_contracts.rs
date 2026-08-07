//! Strict public contracts for Raft ownership, backpressure, and runtime recovery.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    RaftCommittedEntry, RaftMember, RaftMessage, RaftNode, RaftNodeError, RaftRuntime,
    RaftRuntimeError, RaftStateMachine, RaftStateMachineDriver, RaftStateMachineError,
    RaftStateMachineRuntime, RaftStateMachineRuntimeError, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

fn single_member() -> Vec<RaftMember> {
    vec![RaftMember::new(1, "http://node-1")]
}

fn two_members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ]
}

#[derive(Default)]
struct RecordingMachine {
    applied: Arc<AtomicU64>,
    snapshot_calls: Arc<AtomicUsize>,
}

impl RaftStateMachine for RecordingMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine commands must contain eight bytes",
            )
        })?;
        self.applied
            .fetch_add(u64::from_le_bytes(bytes), Ordering::AcqRel);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        self.snapshot_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self.applied.load(Ordering::Acquire).to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine snapshots must contain eight bytes",
            )
        })?;
        self.applied
            .store(u64::from_le_bytes(bytes), Ordering::Release);
        Ok(())
    }
}

#[derive(Default)]
struct RetryOnceTransport {
    attempts: AtomicUsize,
}

#[async_trait]
impl RaftTransport for RetryOnceTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            return Err(RaftTransportError::retryable(io::Error::other(
                "temporary peer backpressure",
            )));
        }
        Ok(())
    }
}

struct FatalTransport;

#[async_trait]
impl RaftTransport for FatalTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Err(RaftTransportError::fatal(io::Error::other(
            "invalid peer transport configuration",
        )))
    }
}

struct SinkTransport;

#[async_trait]
impl RaftTransport for SinkTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Ok(())
    }
}

#[test]
fn raft_node_bounds_unapplied_commits_and_retains_the_queued_command() {
    let mut node =
        RaftNode::new_with_pending_commit_capacity(1, "http://node-1", single_member(), 1)
            .expect("single-node configuration must be valid");

    node.campaign().expect("single node must elect itself");
    node.try_propose(b"first".to_vec())
        .expect("first command must fit the bounded queue");
    assert_eq!(node.pending_commit_count(), 1);

    assert!(matches!(
        node.try_propose(b"second".to_vec()),
        Err(RaftNodeError::PendingCommitCapacity { capacity: 1 })
    ));
    assert_eq!(node.pending_commit_count(), 1);
    assert_eq!(
        node.next_committed()
            .expect("queued command must remain available")
            .data,
        b"first"
    );
    assert!(
        node.try_next_committed()
            .expect("refilling an empty queue must succeed")
            .is_none()
    );
}

#[test]
fn raft_node_checkpoint_persists_exact_application_bytes_at_the_applied_index() {
    let mut node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    node.campaign().expect("single node must elect itself");
    node.propose(b"write".to_vec())
        .expect("leader must accept a proposal");
    let committed = node
        .next_committed()
        .expect("proposal must commit on a single node");

    node.checkpoint(committed.index, b"application-state".to_vec())
        .expect("the committed log tip can be checkpointed");
    assert_eq!(
        node.application_snapshot()
            .expect("snapshot reads must succeed"),
        Some(catga_cluster::RaftApplicationSnapshot {
            index: committed.index,
            data: b"application-state".to_vec(),
        })
    );
}

#[test]
fn state_machine_checkpoint_requires_an_applied_command_and_serializes_the_current_state() {
    let applied = Arc::new(AtomicU64::new(0));
    let snapshot_calls = Arc::new(AtomicUsize::new(0));
    let machine = RecordingMachine {
        applied: Arc::clone(&applied),
        snapshot_calls: Arc::clone(&snapshot_calls),
    };
    let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    let mut driver = RaftStateMachineDriver::new(node, machine).expect("driver must construct");

    assert!(matches!(
        driver.checkpoint(),
        Err(RaftStateMachineError::NothingApplied)
    ));
    driver.campaign().expect("single node must elect itself");
    driver
        .propose(9_u64.to_le_bytes())
        .expect("leader must accept a proposal");
    assert_eq!(
        driver.apply_committed().expect("application must succeed"),
        1
    );
    assert_eq!(driver.applied_index(), 2);
    assert_eq!(applied.load(Ordering::Acquire), 9);

    driver
        .checkpoint()
        .expect("applied state can be checkpointed");
    assert_eq!(snapshot_calls.load(Ordering::Acquire), 1);
}

#[test]
fn runtimes_reject_zero_tick_intervals_without_starting_owner_tasks() {
    let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    assert!(matches!(
        RaftRuntime::spawn(node, Arc::new(SinkTransport), Duration::ZERO),
        Err(RaftRuntimeError::InvalidTickInterval)
    ));

    let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    let driver =
        RaftStateMachineDriver::new(node, RecordingMachine::default()).expect("driver is valid");
    assert!(matches!(
        RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::ZERO),
        Err(RaftStateMachineRuntimeError::InvalidTickInterval)
    ));
}

#[test]
fn raft_runtime_continues_after_retryable_peer_backpressure() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    runtime.block_on(async {
        let transport = Arc::new(RetryOnceTransport::default());
        let runtime = RaftRuntime::spawn(
            RaftNode::new(1, "http://node-1", two_members()).expect("node must construct"),
            Arc::clone(&transport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        runtime
            .campaign()
            .await
            .expect("retryable peer backpressure must not terminate the owner");
        assert!(transport.attempts.load(Ordering::Acquire) > 0);
        runtime
            .campaign()
            .await
            .expect("owner must still accept commands after a temporary error");

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn raft_runtime_surfaces_fatal_transport_errors_only_when_its_owner_stops() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    runtime.block_on(async {
        let runtime = RaftRuntime::spawn(
            RaftNode::new(1, "http://node-1", two_members()).expect("node must construct"),
            Arc::new(FatalTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        assert!(matches!(
            runtime.campaign().await,
            Err(RaftRuntimeError::Transport(_))
        ));
        assert!(matches!(
            runtime.join().await,
            Err(RaftRuntimeError::Transport(_))
        ));
    });
}

#[test]
fn state_machine_runtime_applies_then_checkpoints_without_shared_state_locks() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    runtime.block_on(async {
        let applied = Arc::new(AtomicU64::new(0));
        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let driver = RaftStateMachineDriver::new(
            RaftNode::new(1, "http://node-1", single_member()).expect("node must construct"),
            RecordingMachine {
                applied: Arc::clone(&applied),
                snapshot_calls: Arc::clone(&snapshot_calls),
            },
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(SinkTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(4_u64.to_le_bytes())
            .await
            .expect("proposal must apply before its response completes");
        assert_eq!(applied.load(Ordering::Acquire), 4);
        runtime
            .checkpoint()
            .await
            .expect("applied state must checkpoint through the owner");
        assert_eq!(snapshot_calls.load(Ordering::Acquire), 1);

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}
