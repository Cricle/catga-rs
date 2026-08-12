//! Liveness contracts for Raft runtimes: routine caller errors must not stop the
//! owner task, and terminal stops must be observable through the health surface.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    RaftMessage, RaftNode, RaftRuntime, RaftStateMachineDriver, RaftStateMachineError,
    RaftStateMachineRuntime, RaftStateMachineRuntimeError, RaftStopKind, RaftTransport,
    RaftTransportError, RaftTransportResult,
};

use members::single_member;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

fn two_members() -> Vec<catga_cluster::RaftMember> {
    vec![
        catga_cluster::RaftMember::new(1, "http://node-1"),
        catga_cluster::RaftMember::new(2, "http://node-2"),
    ]
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

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

fn spawn_state_machine_runtime(applied: Arc<AtomicU64>) -> RaftStateMachineRuntime {
    let driver = RaftStateMachineDriver::new(
        RaftNode::new(1, "http://node-1", single_member()).expect("node must construct"),
        RecordingMachine::new(applied, Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::from_millis(1))
        .expect("runtime must start")
}

#[test]
fn checkpoint_before_first_apply_is_a_caller_error_and_keeps_the_runtime_alive() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_state_machine_runtime(Arc::new(AtomicU64::new(0)));

        let result = runtime.checkpoint().await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::StateMachine(
                    RaftStateMachineError::NothingApplied
                ))
            ),
            "checkpoint before any apply must be rejected as a caller error, got {result:?}"
        );
        assert!(
            runtime.is_alive(),
            "a routine caller error must not stop the owner task"
        );
        assert!(runtime.stop_reason().is_none());

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(7_u64.to_le_bytes())
            .await
            .expect("proposals keep working after the caller error");
        runtime
            .checkpoint()
            .await
            .expect("checkpoint succeeds once a command is applied");

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn propose_without_leadership_is_a_caller_error_and_keeps_the_runtime_alive() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_state_machine_runtime(Arc::new(AtomicU64::new(0)));

        let result = runtime.propose(1_u64.to_le_bytes()).await;
        assert!(result.is_err(), "a follower cannot accept proposals");
        assert!(
            runtime.is_alive(),
            "a dropped proposal must not stop the owner task"
        );

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(1_u64.to_le_bytes())
            .await
            .expect("proposal succeeds once the node leads");

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn plain_runtime_propose_without_leadership_keeps_the_runtime_alive() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(SinkTransport), Duration::from_millis(1))
            .expect("runtime must start");

        let result = runtime.propose(b"command".to_vec()).await;
        assert!(result.is_err(), "a follower cannot accept proposals");
        assert!(
            runtime.is_alive(),
            "a dropped proposal must not stop the plain runtime"
        );
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn forwarded_proposal_while_leaderless_keeps_the_runtime_alive() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let driver = RaftStateMachineDriver::new(
            node,
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(SinkTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        // A peer that still believes this node leads forwards its client proposal
        // here. With no known leader raft-rs declines the frame with
        // ProposalDropped; that routine refusal must not stop the owner task.
        let mut forwarded = RaftMessage::default();
        forwarded.set_msg_type(raft::eraftpb::MessageType::MsgPropose);
        forwarded.from = 2;
        forwarded.to = 1;
        let entry = raft::eraftpb::Entry {
            data: b"command".to_vec().into(),
            ..Default::default()
        };
        forwarded.mut_entries().push(entry);
        runtime
            .inbox()
            .send(forwarded)
            .await
            .expect("inbox accepts the forwarded proposal");

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            runtime.is_alive(),
            "a declined forwarded proposal must not stop the owner task"
        );
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn applied_index_reports_application_progress() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_state_machine_runtime(Arc::new(AtomicU64::new(0)));
        assert_eq!(
            runtime
                .applied_index()
                .await
                .expect("applied index is available"),
            0
        );

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(9_u64.to_le_bytes())
            .await
            .expect("proposal must succeed");

        let applied = runtime
            .applied_index()
            .await
            .expect("applied index is available");
        assert!(
            applied >= 2,
            "the applied index must cover the proposal, got {applied}"
        );

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn fatal_transport_stops_the_runtime_observably() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(FatalTransport), Duration::from_millis(1))
            .expect("runtime must start");

        // With non-blocking dispatch the campaign may return Ok: sends only queue
        // frames, and the fatal worker failure surfaces on a later send.
        let _ = runtime.campaign().await;

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while runtime.is_alive() {
            assert!(
                std::time::Instant::now() < deadline,
                "terminal transport failure must stop the owner task"
            );
            tokio::task::yield_now().await;
        }
        let reason = runtime
            .stop_reason()
            .expect("a terminal stop must record its reason");
        assert_eq!(reason.kind(), RaftStopKind::Transport);

        runtime
            .join()
            .await
            .expect_err("join must return the terminal transport error");
    });
}
