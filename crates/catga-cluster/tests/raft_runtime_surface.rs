//! Contract tests for the plain `RaftRuntime` surface not exercised by the
//! liveness suites: coordinator/inbox accessors, committed-entry draining,
//! retryable transport backpressure, and stop-reason reporting.

#[path = "common/members.rs"]
mod members;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftRuntime, RaftRuntimeError,
    RaftStopKind, RaftTransport, RaftTransportError, RaftTransportResult,
};
use members::single_member;

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
fn runtime_exposes_its_accessors_and_drains_committed_entries() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", single_member()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(BusyTransport), Duration::from_millis(1))
            .expect("runtime must start");

        assert_eq!(runtime.id(), 1);
        assert_eq!(runtime.coordinator().node_id(), "1");
        assert!(runtime.stop_reason().is_none());

        // An inbound frame addressed to this node is accepted by the inbox.
        let mut heartbeat = RaftMessage::default();
        heartbeat.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
        heartbeat.from = 2;
        heartbeat.to = 1;
        runtime
            .inbox()
            .try_send(heartbeat)
            .expect("the inbox accepts frames while running");

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        runtime
            .propose(7_u64.to_le_bytes())
            .await
            .expect("proposal commits");

        let committed = runtime.drain_committed().await.expect("drain succeeds");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].data, 7_u64.to_le_bytes());
        assert!(
            runtime
                .drain_committed()
                .await
                .expect("drain succeeds")
                .is_empty()
        );

        runtime.shutdown();
        let drained = runtime.drain_committed().await;
        assert!(matches!(drained, Err(RaftRuntimeError::Stopped)));
        assert!(
            runtime.stop_reason().is_none(),
            "graceful stops record nothing"
        );
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn retryable_transport_failures_report_the_peer_and_keep_the_runtime_alive() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(BusyTransport), Duration::from_millis(1))
            .expect("runtime must start");

        // The campaign's outbound frames fail retryably; the owner reports the peer
        // unreachable and keeps running instead of stopping.
        let _ = runtime.campaign().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(runtime.is_alive());
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn a_terminal_stop_reason_reports_kind_detail_and_display() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let runtime = RaftRuntime::spawn(node, Arc::new(FatalTransport), Duration::from_millis(1))
            .expect("runtime must start");

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
        let rendered = reason.to_string();
        assert!(rendered.contains("Transport"));
        assert!(rendered.contains("link down"));

        runtime
            .join()
            .await
            .expect_err("join returns the terminal error");
    });
}
