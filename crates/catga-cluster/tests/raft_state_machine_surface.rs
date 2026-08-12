//! Contract tests for `RaftStateMachineRuntime` transport-failure handling and
//! health accessors that the liveness and bridge suites do not cover.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftStateMachineDriver,
    RaftStateMachineRuntime, RaftStopKind, RaftTransport, RaftTransportError, RaftTransportResult,
};
use members::single_member;
use recording_machine::RecordingMachine;

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

fn spawn_runtime<T>(members: Vec<RaftMember>, transport: Arc<T>) -> RaftStateMachineRuntime
where
    T: RaftTransport + 'static,
{
    let driver = RaftStateMachineDriver::new(
        RaftNode::new(1, "http://node-1", members).expect("node must construct"),
        RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, transport, Duration::from_millis(1))
        .expect("runtime must start")
}

struct BusyTransport;

#[async_trait]
impl RaftTransport for BusyTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Err(RaftTransportError::retryable(io::Error::other(
            "peer dispatch queue is full",
        )))
    }
}

struct FatalTransport;

#[async_trait]
impl RaftTransport for FatalTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Err(RaftTransportError::fatal(io::Error::other("link down")))
    }
}

#[test]
fn state_machine_runtime_reports_unreachable_peers_and_stays_alive() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_runtime(two_members(), Arc::new(BusyTransport));

        // Campaign frames fail retryably, so the owner reports the peer
        // unreachable and keeps driving instead of stopping.
        let _ = runtime.campaign().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(runtime.is_alive());
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn state_machine_runtime_stop_reason_describes_terminal_transport_failures() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_runtime(two_members(), Arc::new(FatalTransport));

        let _ = runtime.campaign().await;

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while runtime.is_alive() {
            assert!(
                std::time::Instant::now() < deadline,
                "a fatal transport failure must stop the owner task"
            );
            tokio::task::yield_now().await;
        }

        let reason = runtime
            .stop_reason()
            .expect("terminal stops record a reason");
        assert_eq!(reason.kind(), RaftStopKind::Transport);
        assert!(reason.detail().contains("link down"));
        assert!(reason.to_string().contains("Transport"));

        runtime
            .join()
            .await
            .expect_err("join returns the terminal error");
    });
}

#[test]
fn state_machine_runtime_exposes_id_coordinator_and_inbox() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_runtime(single_member(), Arc::new(BusyTransport));

        assert_eq!(runtime.id(), 1);
        assert_eq!(runtime.coordinator().node_id(), "1");
        assert!(!runtime.coordinator().is_leader());

        let mut heartbeat = RaftMessage::default();
        heartbeat.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
        heartbeat.from = 2;
        heartbeat.to = 1;
        runtime
            .inbox()
            .try_send(heartbeat)
            .expect("the inbox accepts frames while running");

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}
