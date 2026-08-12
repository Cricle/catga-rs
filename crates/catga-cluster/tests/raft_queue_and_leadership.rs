//! Contract tests for dispatch-queue backpressure and leadership transitions:
//! a full per-peer dispatch queue fails sends retryably without stopping the
//! runtime, and a partitioned leader steps down once it cannot confirm quorum.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftClusterNode, RaftMember, RaftMessage, RaftNode, RaftRuntime,
    RaftStateMachineDriver, RaftStateMachineRuntime, RaftTransport, RaftTransportError,
    RaftTransportResult,
};

#[path = "common/recording_machine.rs"]
mod recording_machine;

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

/// A transport whose first send never completes, so the per-peer worker holds
/// one in-flight frame while the dispatch queue behind it fills up.
struct BlockingTransport {
    started: AtomicUsize,
}

#[async_trait]
impl RaftTransport for BlockingTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        self.started.fetch_add(1, Ordering::AcqRel);
        std::future::pending().await
    }
}

#[test]
fn a_full_peer_dispatch_queue_fails_sends_retryably_without_stopping_the_runtime() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let transport = Arc::new(BlockingTransport {
            started: AtomicUsize::new(0),
        });
        let runtime = RaftRuntime::spawn(node, transport.clone(), Duration::from_millis(1))
            .expect("runtime must start");

        // Every campaign emits one pre-vote frame for the single peer. The worker
        // holds the first frame forever, so after 64 queued frames (the per-peer
        // capacity) further sends overflow the queue and fail retryably.
        for _ in 0..70 {
            runtime
                .campaign()
                .await
                .expect("queue overflow must surface as routine backpressure");
        }
        assert_eq!(
            transport.started.load(Ordering::Acquire),
            1,
            "the blocked worker never takes a second frame"
        );
        assert!(runtime.is_alive());
        assert!(runtime.stop_reason().is_none());

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

/// An in-process hub that routes frames between runtimes until severed.
#[derive(Clone, Default)]
struct GatedTransport {
    routes: Arc<RwLock<HashMap<u64, tokio::sync::mpsc::Sender<RaftMessage>>>>,
    severed: Arc<RwLock<bool>>,
    severed_messages: Arc<AtomicU64>,
}

impl GatedTransport {
    async fn register(&self, runtime: &RaftStateMachineRuntime) {
        self.routes
            .write()
            .expect("route table poisoned")
            .insert(runtime.id(), runtime.inbox());
    }

    fn sever(&self) {
        *self.severed.write().expect("gate poisoned") = true;
    }

    fn severed_messages(&self) -> u64 {
        self.severed_messages.load(Ordering::Acquire)
    }
}

#[async_trait]
impl RaftTransport for GatedTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        if *self.severed.read().expect("gate poisoned") {
            self.severed_messages.fetch_add(1, Ordering::AcqRel);
            return Err(RaftTransportError::retryable(std::io::Error::other(
                "partition severs all traffic",
            )));
        }
        let route = self
            .routes
            .read()
            .expect("route table poisoned")
            .get(&message.to)
            .cloned();
        let Some(route) = route else {
            return Err(RaftTransportError::retryable(std::io::Error::other(
                "peer never registered",
            )));
        };
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(std::io::Error::other("peer stopped")))?;
        Ok(())
    }
}

fn spawn_state_machine_runtime(
    id: u64,
    members: &[RaftMember],
    transport: &GatedTransport,
) -> RaftStateMachineRuntime {
    let driver = RaftStateMachineDriver::new(
        RaftNode::new(id, format!("http://node-{id}"), members.to_vec())
            .expect("node must construct"),
        RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(
        driver,
        Arc::new(transport.clone()),
        Duration::from_millis(1),
    )
    .expect("runtime must start")
}

async fn wait_for_leader(coordinator: &RaftClusterNode) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !coordinator.is_leader() {
        assert!(
            std::time::Instant::now() < deadline,
            "the node must win the election"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn a_partitioned_leader_steps_down_when_it_cannot_confirm_quorum() {
    current_thread_runtime().block_on(async {
        let transport = GatedTransport::default();
        let members = two_members();

        let one = spawn_state_machine_runtime(1, &members, &transport);
        let two = spawn_state_machine_runtime(2, &members, &transport);
        transport.register(&one).await;
        transport.register(&two).await;

        one.campaign().await.expect("campaign must start");
        wait_for_leader(&one.coordinator()).await;

        transport.sever();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while one.coordinator().is_leader() {
            assert!(
                std::time::Instant::now() < deadline,
                "the partitioned leader must step down"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            transport.severed_messages() > 0,
            "the partition must actually sever traffic"
        );

        one.shutdown();
        two.shutdown();
        one.join().await.expect("node one must stop cleanly");
        two.join().await.expect("node two must stop cleanly");
    });
}
