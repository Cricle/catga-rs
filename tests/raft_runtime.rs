//! Raft transport runtime integration tests.

use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftRuntime, RaftRuntimeError,
    RaftTransport, RaftTransportError, RaftTransportResult,
};
use tokio::sync::{RwLock, mpsc};

#[derive(Default)]
struct BlockingTransport {
    entered_send: AtomicBool,
}

#[async_trait]
impl RaftTransport for BlockingTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        self.entered_send.store(true, Ordering::Release);
        std::future::pending().await
    }
}

#[derive(Clone, Default)]
struct ChannelTransport {
    routes: Arc<RwLock<HashMap<u64, mpsc::Sender<RaftMessage>>>>,
}

impl ChannelTransport {
    async fn register(&self, node: &RaftRuntime) {
        self.routes.write().await.insert(node.id(), node.inbox());
    }
}

#[async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self
            .routes
            .read()
            .await
            .get(&message.to)
            .cloned()
            .ok_or_else(|| RaftTransportError::fatal(io::Error::other("unknown Raft peer")))?;
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(io::Error::other("Raft peer stopped")))?;
        Ok(())
    }
}

#[derive(Default)]
struct RetryOnceTransport {
    attempts: AtomicU64,
}

#[async_trait]
impl RaftTransport for RetryOnceTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            return Err(RaftTransportError::retryable(io::Error::other(
                "peer inbox is temporarily full",
            )));
        }
        Ok(())
    }
}

fn members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ]
}

#[test]
fn raft_runtime_rejects_a_zero_tick_interval() {
    let cluster_members = members();
    let node = RaftNode::new(1, "http://node-1", cluster_members).unwrap();

    assert!(matches!(
        RaftRuntime::spawn(node, Arc::new(ChannelTransport::default()), Duration::ZERO),
        Err(RaftRuntimeError::InvalidTickInterval)
    ));
}

#[tokio::test]
async fn raft_runtime_owns_ticks_transport_and_committed_entries_without_external_relay() {
    let transport = ChannelTransport::default();
    let cluster_members = members();
    let runtimes = cluster_members
        .iter()
        .map(|member| {
            RaftRuntime::spawn(
                RaftNode::new(member.id(), member.endpoint(), cluster_members.clone()).unwrap(),
                Arc::new(transport.clone()),
                Duration::from_millis(2),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    runtimes[0].campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtimes.iter().any(|runtime| {
            runtime.coordinator().leader_endpoint().as_deref() != Some("http://node-1")
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtimes[0].propose(b"reserve-inventory:10").await.unwrap();
    for runtime in &runtimes {
        let committed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let entries = runtime.drain_committed().await.unwrap();
                if entries
                    .iter()
                    .any(|entry| entry.data == b"reserve-inventory:10")
                {
                    return entries;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            committed
                .iter()
                .any(|entry| entry.data == b"reserve-inventory:10")
        );
    }

    for runtime in runtimes {
        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}

#[tokio::test]
async fn raft_runtime_shutdown_cancels_a_blocked_transport_send() {
    let transport = Arc::new(BlockingTransport::default());
    let runtime = Arc::new(
        RaftRuntime::spawn(
            RaftNode::new(
                1,
                "http://node-1",
                vec![
                    RaftMember::new(1, "http://node-1"),
                    RaftMember::new(2, "http://node-2"),
                ],
            )
            .unwrap(),
            Arc::clone(&transport),
            Duration::from_millis(1),
        )
        .unwrap(),
    );
    let campaign = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move { runtime.campaign().await })
    };

    tokio::time::timeout(Duration::from_secs(1), async {
        while !transport.entered_send.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("campaign must start transport delivery");

    runtime.shutdown();
    assert!(matches!(
        campaign.await.unwrap(),
        Err(RaftRuntimeError::Stopped)
    ));
    let runtime = match Arc::try_unwrap(runtime) {
        Ok(runtime) => runtime,
        Err(_) => panic!("campaign task released the runtime"),
    };
    tokio::time::timeout(Duration::from_secs(1), runtime.join())
        .await
        .expect("shutdown must cancel a blocked transport send")
        .unwrap();
}

#[tokio::test]
async fn raft_runtime_keeps_running_after_a_retryable_peer_delivery_failure() {
    let transport = Arc::new(RetryOnceTransport::default());
    let runtime = RaftRuntime::spawn(
        RaftNode::new(
            1,
            "http://node-1",
            vec![
                RaftMember::new(1, "http://node-1"),
                RaftMember::new(2, "http://node-2"),
            ],
        )
        .expect("Raft node is valid"),
        Arc::clone(&transport),
        Duration::from_millis(1),
    )
    .expect("runtime starts");

    runtime
        .campaign()
        .await
        .expect("a retryable peer failure must not terminate the runtime");
    assert!(transport.attempts.load(Ordering::Acquire) > 0);

    runtime
        .campaign()
        .await
        .expect("the owner task remains command-responsive after recovery");
    runtime.shutdown();
    runtime.join().await.expect("runtime stops cleanly");
}
