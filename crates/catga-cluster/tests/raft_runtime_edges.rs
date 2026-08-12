//! Contract tests for runtime error edges: bounded-queue backpressure through
//! the command channel, terminal failures reported to the command caller, and
//! owner-task panics surfacing through `join`.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use async_trait::async_trait;
use catga_cluster::{
    RaftCommittedEntry, RaftMember, RaftMessage, RaftNode, RaftNodeError, RaftRuntime,
    RaftRuntimeError, RaftStateMachine, RaftStateMachineDriver, RaftStateMachineRuntime,
    RaftStateMachineRuntimeError, RaftTransport, RaftTransportError, RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use members::single_member;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

fn two_members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ]
}

struct FatalTransport;

#[async_trait]
impl RaftTransport for FatalTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Err(RaftTransportError::fatal(io::Error::other("link down")))
    }
}

/// A transport whose sends never complete, so per-peer dispatch queues fill.
struct BlockingTransport;

#[async_trait]
impl RaftTransport for BlockingTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        std::future::pending().await
    }
}

#[test]
fn a_full_pending_commit_queue_is_a_routine_caller_error_for_the_plain_runtime() {
    current_thread_runtime().block_on(async {
        let node =
            RaftNode::new_with_pending_commit_capacity(1, "http://node-1", single_member(), 1)
                .expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(SinkTransport), Duration::from_millis(1))
            .expect("runtime must start");

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(1_u64.to_le_bytes())
            .await
            .expect("the first proposal fits the queue");

        let result = runtime.propose(2_u64.to_le_bytes()).await;
        assert!(matches!(
            result,
            Err(RaftRuntimeError::Node(
                RaftNodeError::PendingCommitCapacity { capacity: 1 }
            ))
        ));
        assert!(runtime.is_alive(), "backpressure must not stop the owner");
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn a_terminal_transport_failure_is_reported_to_the_command_caller() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(FatalTransport), Duration::from_millis(1))
            .expect("runtime must start");

        // The first campaign only queues frames; the worker then records the fatal
        // failure, so the next command's inline send observes it and the caller
        // learns the owner stopped.
        let _ = runtime.campaign().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let result = runtime.campaign().await;
        assert!(matches!(result, Err(RaftRuntimeError::Stopped)));

        runtime
            .join()
            .await
            .expect_err("join returns the terminal error");
    });
}

#[test]
fn a_full_pending_commit_queue_is_a_routine_caller_error_for_the_state_machine_driver() {
    // The runtime applies (and therefore drains) the bounded queue after every
    // drive, so the routine capacity error is observed deterministically one
    // level down, on the driver's propose path the runtime delegates to.
    current_thread_runtime().block_on(async {
        let node =
            RaftNode::new_with_pending_commit_capacity(1, "http://node-1", single_member(), 1)
                .expect("node must construct");
        let mut driver = RaftStateMachineDriver::new(
            node,
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");

        driver.campaign().expect("single node must elect itself");
        driver
            .propose(1_u64.to_le_bytes())
            .expect("the first proposal fits the queue");

        let result = driver.propose(2_u64.to_le_bytes());
        assert!(matches!(
            result,
            Err(RaftNodeError::PendingCommitCapacity { capacity: 1 })
        ));

        // Applying the queued entry drains the queue, so the next proposal is
        // accepted again: backpressure stays a routine caller error.
        driver.apply_committed().expect("apply must succeed");
        driver
            .propose(2_u64.to_le_bytes())
            .expect("the drained queue accepts proposals again");
    });
}

#[test]
fn a_terminal_transport_failure_is_reported_to_the_state_machine_command_caller() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let driver = RaftStateMachineDriver::new(
            node,
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(FatalTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        let _ = runtime.campaign().await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let result = runtime.campaign().await;
        assert!(matches!(result, Err(RaftStateMachineRuntimeError::Stopped)));

        runtime
            .join()
            .await
            .expect_err("join returns the terminal error");
    });
}

#[test]
fn a_full_dispatch_queue_reports_the_peer_unreachable_for_the_state_machine_runtime() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let driver = RaftStateMachineDriver::new(
            node,
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(BlockingTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        // The blocked worker holds one frame; 64 queued frames fill the per-peer
        // capacity, and further campaign frames overflow it, exercising the
        // retryable report-unreachable path inside the owner loop.
        for _ in 0..70 {
            runtime
                .campaign()
                .await
                .expect("queue overflow must surface as routine backpressure");
        }
        assert!(runtime.is_alive());
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn a_panicking_state_machine_stops_the_owner_and_surfaces_through_join() {
    struct PanickingMachine;

    impl RaftStateMachine for PanickingMachine {
        fn apply(&mut self, _entry: &RaftCommittedEntry) -> CatgaResult<()> {
            panic!("application state machine panicked");
        }

        fn snapshot(&self) -> CatgaResult<Vec<u8>> {
            Err(CatgaError::new(ErrorCode::Internal, "unreachable"))
        }

        fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::Internal, "unreachable"))
        }
    }

    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
        let driver =
            RaftStateMachineDriver::new(node, PanickingMachine).expect("driver must construct");
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
        // The proposal commits; the apply panic takes the owner task down.
        let _ = runtime.propose(1_u64.to_le_bytes()).await;

        let result = runtime.join().await;
        let Err(RaftStateMachineRuntimeError::Task(join_error)) = result else {
            panic!("a panicking owner must surface as a task error, got {result:?}");
        };
        assert!(join_error.is_panic());
        assert!(join_error.to_string().contains("panic"));
    });
}
