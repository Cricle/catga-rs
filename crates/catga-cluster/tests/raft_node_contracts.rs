//! Contract tests for node-level Raft behavior that the runtime suites do not
//! exercise: member validation, bounded commit queues, coordinator views
//! before leadership exists, committed-entry draining, and checkpoints.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use catga_cluster::{
    ClusterCoordinator, RaftClusterConfig, RaftMember, RaftNode, RaftNodeError,
    RaftStateMachineDriver, RaftStateMachineError,
};
use members::single_member;
use recording_machine::RecordingMachine;

fn default_timing() -> catga_cluster::RaftTiming {
    RaftClusterConfig::local(0, 1, 9000)
        .expect("valid dimensions")
        .raft_timing()
        .expect("default timing is valid")
}

#[test]
fn node_construction_validates_the_member_list() {
    assert!(matches!(
        RaftNode::new(1, "http://node-1", Vec::new()),
        Err(RaftNodeError::EmptyMembers)
    ));
    assert!(matches!(
        RaftNode::new(
            0,
            "http://node-0",
            vec![RaftMember::new(0, "http://node-0")]
        ),
        Err(RaftNodeError::ZeroMemberId)
    ));
    assert!(matches!(
        RaftNode::new(
            1,
            "http://node-1",
            vec![
                RaftMember::new(1, "http://node-1"),
                RaftMember::new(1, "http://node-1-copy"),
            ],
        ),
        Err(RaftNodeError::DuplicateMemberId(1))
    ));
    assert!(matches!(
        RaftNode::new(
            1,
            "http://node-1",
            vec![RaftMember::new(2, "http://node-2")]
        ),
        Err(RaftNodeError::LocalMemberMissing(1))
    ));
    assert!(matches!(
        RaftNode::new(1, "http://elsewhere", single_member()),
        Err(RaftNodeError::LocalEndpointMismatch { .. })
    ));
}

#[test]
fn node_construction_rejects_a_zero_pending_commit_capacity() {
    assert!(matches!(
        RaftNode::new_with_timing_and_pending_commit_capacity(
            1,
            "http://node-1",
            single_member(),
            default_timing(),
            0,
        ),
        Err(RaftNodeError::ZeroPendingCommitCapacity)
    ));
}

#[test]
fn new_with_timing_builds_a_working_node() {
    let mut node = RaftNode::new_with_timing(1, "http://node-1", single_member(), default_timing())
        .expect("node must construct with explicit timing");
    node.campaign().expect("single node must elect itself");
    node.propose(1_u64.to_le_bytes()).expect("proposal commits");
    assert_eq!(node.pending_commit_count(), 1);
}

#[test]
fn persisted_committed_entries_reads_the_durable_log_without_queuing() {
    let mut node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    assert!(
        node.persisted_committed_entries()
            .expect("read succeeds")
            .is_empty(),
        "a fresh node has no committed entries"
    );

    node.campaign().expect("single node must elect itself");
    node.propose(9_u64.to_le_bytes()).expect("proposal commits");

    let entries = node.persisted_committed_entries().expect("read succeeds");
    assert_eq!(entries.len(), 1, "empty protocol entries are excluded");
    assert_eq!(entries[0].data, 9_u64.to_le_bytes());
    assert_eq!(
        node.pending_commit_count(),
        1,
        "the durable read leaves the in-memory queue untouched"
    );
}

#[test]
fn a_full_pending_commit_queue_applies_backpressure_until_drained() {
    let mut node =
        RaftNode::new_with_pending_commit_capacity(1, "http://node-1", single_member(), 1)
            .expect("node must construct");
    node.campaign().expect("single node must elect itself");

    node.propose(1_u64.to_le_bytes())
        .expect("the first proposal fits the queue");
    assert_eq!(node.pending_commit_count(), 1);

    let result = node.propose(2_u64.to_le_bytes());
    assert!(matches!(
        result,
        Err(RaftNodeError::PendingCommitCapacity { capacity: 1 })
    ));

    let drained = node.try_drain_committed().expect("drain must succeed");
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].data, 1_u64.to_le_bytes());
    assert_eq!(node.pending_commit_count(), 0);

    node.propose(2_u64.to_le_bytes())
        .expect("the queue accepts new work after draining");
}

#[test]
fn a_follower_proposal_is_a_routine_raft_error() {
    let mut node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    assert!(matches!(
        node.propose(b"set a=1".to_vec()),
        Err(RaftNodeError::Raft(raft::Error::ProposalDropped))
    ));
}

#[test]
fn reporting_an_unknown_peer_unreachable_is_a_noop() {
    let mut node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    node.report_unreachable(2)
        .expect("an unknown peer report is ignored");
}

#[test]
fn a_forwarded_proposal_while_leaderless_is_dropped_routinely() {
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ];
    let mut node = RaftNode::new(1, "http://node-1", members).expect("node must construct");

    let mut forwarded = raft::prelude::Message::default();
    forwarded.set_msg_type(raft::prelude::MessageType::MsgPropose);
    forwarded.from = 2;
    forwarded.to = 1;
    forwarded.mut_entries().push(raft::prelude::Entry {
        data: b"command".to_vec().into(),
        ..Default::default()
    });

    node.step(forwarded)
        .expect("a declined forwarded proposal must not fail the node");
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

#[test]
fn the_coordinator_reports_no_leader_before_any_election() {
    current_thread_runtime().block_on(async {
        let mut node =
            RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
        let coordinator = node.coordinator();

        assert_eq!(coordinator.node_id(), "1");
        assert!(!coordinator.is_leader());
        assert_eq!(coordinator.leader_endpoint(), None);
        assert_eq!(coordinator.member_endpoints().len(), 1);
        let snapshot = coordinator.leadership_snapshot();
        assert!(snapshot.leader_node_id.is_none());

        let mut subscription = coordinator.subscribe_leadership();
        assert!(subscription.snapshot().leader_node_id.is_none());

        // Waiting for leadership without an election times out with `false`.
        assert!(
            !coordinator
                .wait_for_leadership(Duration::from_millis(20))
                .await
        );

        let waiter = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.wait_for_leadership_change(false).await }
        });
        tokio::task::yield_now().await;
        node.campaign().expect("single node must elect itself");

        assert!(
            tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("the waiter must observe the election")
                .expect("the waiter task must not panic"),
            "the waiter must observe this node leading"
        );
        assert!(coordinator.is_leader());
        assert_eq!(
            coordinator.leader_endpoint().as_deref(),
            Some("http://node-1")
        );
        assert!(coordinator.wait_for_leadership(Duration::ZERO).await);
        // The state already changed, so the waiter returns without suspending.
        assert!(coordinator.wait_for_leadership_change(false).await);

        let transition = tokio::time::timeout(Duration::from_secs(2), subscription.recv())
            .await
            .expect("the subscription must deliver the election")
            .expect("the subscription stays open");
        assert_eq!(transition.leader_node_id.as_deref(), Some("1"));
    });
}

#[test]
fn committed_entries_drain_in_log_order_through_every_accessor() {
    current_thread_runtime().block_on(async {
        let mut node =
            RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
        node.campaign().expect("single node must elect itself");
        node.propose(1_u64.to_le_bytes()).expect("proposal one");
        node.propose(2_u64.to_le_bytes()).expect("proposal two");

        let first = node
            .try_next_committed()
            .expect("refill must succeed")
            .expect("one committed entry is pending");
        assert_eq!(first.data, 1_u64.to_le_bytes());

        let second = node.next_committed().expect("the second entry was queued");
        assert_eq!(second.data, 2_u64.to_le_bytes());
        assert!(node.next_committed().is_none());

        node.propose(3_u64.to_le_bytes()).expect("proposal three");
        let rest = node.try_drain_committed().expect("drain must succeed");
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].data, 3_u64.to_le_bytes());

        assert!(
            node.drain_messages().is_empty(),
            "a single node has no peers"
        );
        assert!(node.drain_installed_snapshots().is_empty());
    });
}

#[test]
fn checkpoints_reject_uncommitted_indexes_and_then_persist_at_the_tip() {
    let mut node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");

    assert!(
        node.checkpoint(0, Vec::new()).is_err(),
        "index zero is never a valid checkpoint"
    );
    assert!(
        node.checkpoint(1, Vec::new()).is_err(),
        "an index beyond the durable commit is rejected"
    );
    assert!(
        node.application_snapshot()
            .expect("snapshot read")
            .is_none()
    );

    node.campaign().expect("single node must elect itself");
    node.propose(1_u64.to_le_bytes()).expect("proposal commits");

    // Index 2 is the proposal; the election no-op occupies index 1.
    node.checkpoint(2, b"snapshot".to_vec())
        .expect("the committed tip checkpoints");
    let snapshot = node
        .application_snapshot()
        .expect("snapshot read")
        .expect("a durable snapshot exists");
    assert_eq!(snapshot.index, 2);
    assert_eq!(snapshot.data, b"snapshot");
}

#[test]
fn the_driver_exposes_machine_id_and_coordinator_views() {
    let applied = Arc::new(AtomicU64::new(0));
    let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
    let mut driver = RaftStateMachineDriver::new(
        node,
        RecordingMachine::new(Arc::clone(&applied), Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");

    assert_eq!(driver.id(), 1);
    assert_eq!(driver.applied_index(), 0);
    assert_eq!(driver.machine().applied.load(Ordering::Acquire), 0);
    assert_eq!(driver.coordinator().node_id(), "1");

    assert!(matches!(
        driver.checkpoint(),
        Err(RaftStateMachineError::NothingApplied)
    ));

    driver.campaign().expect("single node must elect itself");
    driver
        .propose(4_u64.to_le_bytes())
        .expect("proposal commits");
    assert_eq!(driver.apply_committed().expect("apply succeeds"), 1);
    assert_eq!(driver.applied_index(), 2);
    assert_eq!(applied.load(Ordering::Acquire), 4);

    driver.report_unreachable(2).expect("unknown peer report");
    driver.checkpoint().expect("applied tip checkpoints");
    assert_eq!(driver.machine().snapshot_calls.load(Ordering::Acquire), 1);
}

#[test]
fn a_persistent_node_reopens_with_its_persisted_membership() {
    current_thread_runtime().block_on(async {
        let directory = tempfile::tempdir().expect("temporary raft directory");
        let timing = default_timing();

        {
            let mut node = RaftNode::open_persistent_with_timing(
                1,
                "http://node-1",
                single_member(),
                directory.path(),
                timing,
            )
            .expect("persistent node must open");
            node.campaign().expect("single node must elect itself");
            node.propose(1_u64.to_le_bytes()).expect("proposal commits");
            node.checkpoint(2, 1_u64.to_le_bytes().to_vec())
                .expect("the committed tip checkpoints");
        }

        {
            let node = RaftNode::open_persistent_with_timing(
                1,
                "http://node-1",
                single_member(),
                directory.path(),
                timing,
            )
            .expect("persistent node must reopen");
            let applied = Arc::new(AtomicU64::new(0));
            let driver = RaftStateMachineDriver::new(
                node,
                RecordingMachine::new(Arc::clone(&applied), Arc::new(AtomicUsize::new(0))),
            )
            .expect("driver must recover the committed suffix");
            assert_eq!(driver.applied_index(), 2, "recovery replays the proposal");
            assert_eq!(applied.load(Ordering::Acquire), 1);
        }
    });
}
