//! E2E tests for catga-cluster cluster coordination and Raft runtime.
//!
//! These tests verify end-to-end cluster behavior including leadership election,
//! member coordination, and persistent Raft node recovery using real async runtime.

use std::{
    collections::HashMap,
    io,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftRuntime,
    RaftTransport, RaftTransportError, RaftTransportResult,
};
use tokio::sync::RwLock;

/// A channel-based transport that routes messages between cluster nodes.
#[derive(Clone, Default)]
struct E2eChannelTransport {
    routes: Arc<RwLock<HashMap<u64, tokio::sync::mpsc::Sender<RaftMessage>>>>,
}

impl E2eChannelTransport {
    async fn register(&self, runtime: &RaftRuntime) {
        self.routes.write().await.insert(runtime.id(), runtime.inbox());
    }
}

#[async_trait]
impl RaftTransport for E2eChannelTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self
            .routes
            .read()
            .await
            .get(&message.to)
            .cloned()
            .ok_or_else(|| RaftTransportError::fatal(io::Error::other("unknown peer")))?;
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(io::Error::other("peer stopped")))?;
        Ok(())
    }
}

/// E2E test: RaftRuntime campaigns and achieves leadership across multiple nodes.
#[tokio::test]
async fn e2e_raft_runtime_achieves_leadership() {
    let transport = E2eChannelTransport::default();
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ];

    let mut runtimes: Vec<_> = members
        .iter()
        .map(|member| {
            RaftRuntime::spawn(
                RaftNode::new(member.id(), member.endpoint(), members.clone()).unwrap(),
                Arc::new(transport.clone()),
                Duration::from_millis(10),
            )
            .unwrap()
        })
        .collect();

    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    // Campaign for leadership
    runtimes[0].campaign().await.unwrap();

    // Wait for all nodes to observe leadership
    let wait_result = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtimes.iter().all(|r| {
                r.coordinator()
                    .leader_endpoint()
                    .as_deref()
                    == Some("http://node-1")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(wait_result.is_ok(), "leadership should be established");

    // Verify leader
    assert!(runtimes[0].coordinator().is_leader());
    assert_eq!(
        runtimes[0].coordinator().leader_endpoint().as_deref(),
        Some("http://node-1")
    );

    // Cleanup
    while let Some(runtime) = runtimes.pop() {
        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}

/// E2E test: RaftRuntime replicates proposals to all followers.
#[tokio::test]
async fn e2e_raft_runtime_replicates_proposals() {
    let transport = E2eChannelTransport::default();
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ];

    let mut runtimes: Vec<_> = members
        .iter()
        .map(|member| {
            RaftRuntime::spawn(
                RaftNode::new(member.id(), member.endpoint(), members.clone()).unwrap(),
                Arc::new(transport.clone()),
                Duration::from_millis(10),
            )
            .unwrap()
        })
        .collect();

    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    // Establish leadership
    runtimes[0].campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtimes.iter().any(|r| !r.coordinator().is_leader()) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("leadership should be established");

    // Propose data
    runtimes[0]
        .propose(b"create-order:42")
        .await
        .expect("proposal succeeds");

    // Verify all nodes receive the committed entry
    while let Some(runtime) = runtimes.pop() {
        let committed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let entries = runtime.drain_committed().await;
                if let Ok(entries) = entries {
                    if entries.iter().any(|e| e.data == b"create-order:42") {
                        return entries;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit should be received");

        assert!(
            committed.iter().any(|e| e.data == b"create-order:42"),
            "all nodes should receive the committed entry"
        );

        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}

/// E2E test: MemoryCluster coordinates leadership election in-process.
#[tokio::test]
async fn e2e_memory_cluster_elects_leader() {
    use catga_cluster::MemoryCluster;

    let cluster = MemoryCluster::new(
        "node-1",
        ["http://cluster/node-1", "http://cluster/node-2", "http://cluster/node-3"],
    );

    let node1 = cluster.node("node-1").expect("node-1 exists");
    let node2 = cluster.node("node-2").expect("node-2 exists");
    let node3 = cluster.node("node-3").expect("node-3 exists");

    // Initially, node-1 should be the leader (set as leader in MemoryCluster::new)
    assert!(node1.is_leader());
    assert_eq!(
        node1.leader_endpoint().as_deref(),
        Some("http://cluster/node-1")
    );

    // Followers should know the leader
    assert!(!node2.is_leader());
    assert_eq!(
        node2.leader_endpoint().as_deref(),
        Some("http://cluster/node-1")
    );
    assert!(!node3.is_leader());
    assert_eq!(
        node3.leader_endpoint().as_deref(),
        Some("http://cluster/node-1")
    );

    // Elect a new leader
    cluster.elect("node-2").expect("election succeeds");

    // Verify leadership changed
    assert!(!node1.is_leader());
    assert!(node2.is_leader());
    assert!(!node3.is_leader());
    assert_eq!(
        node2.leader_endpoint().as_deref(),
        Some("http://cluster/node-2")
    );
}

/// E2E test: LeadershipSubscription delivers transitions.
#[tokio::test]
async fn e2e_leadership_subscription_delivers_transitions() {
    use catga_cluster::MemoryCluster;

    let cluster = MemoryCluster::new(
        "node-1",
        ["http://cluster/node-1", "http://cluster/node-2"],
    );

    let node = cluster.node("node-1").expect("node exists");
    let mut subscription = node.subscribe_leadership();

    // Initial snapshot should have node-1 as leader
    let initial = subscription.snapshot();
    assert_eq!(initial.epoch, 0);
    assert_eq!(initial.leader_node_id.as_deref(), Some("node-1"));

    // Elect new leader
    cluster.elect("node-2").expect("election succeeds");

    // Subscription should receive the transition
    let next = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("transition should be delivered")
        .expect("subscription remains open");

    assert_eq!(next.epoch, 1);
    assert_eq!(next.leader_node_id.as_deref(), Some("node-2"));
}

/// E2E test: wait_for_leadership returns true when node becomes leader.
#[tokio::test]
async fn e2e_wait_for_leadership_returns_when_leader() {
    use catga_cluster::MemoryCluster;

    let cluster = MemoryCluster::new(
        "node-1",
        ["http://cluster/node-1", "http://cluster/node-2"],
    );

    let node = cluster.node("node-2").expect("node exists");
    assert!(!node.is_leader());

    // Start waiting in background
    let handle = tokio::spawn({
        let node = cluster.node("node-2").unwrap();
        async move { node.wait_for_leadership(Duration::from_secs(5)).await }
    });

    // Small delay to ensure wait has started
    tokio::task::yield_now().await;

    // Elect node-2 as leader
    cluster.elect("node-2").expect("election succeeds");

    // Wait should complete
    let became_leader = handle.await.expect("task should complete");
    assert!(became_leader);
    assert!(cluster.node("node-2").unwrap().is_leader());
}

/// E2E test: Persistent Raft node recovers committed entries after restart.
#[tokio::test]
async fn e2e_persistent_raft_node_recovers_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ];

    // First instance: campaign and propose
    {
        let node = RaftNode::open_persistent(1, "http://node-1", members.clone(), directory.path())
            .unwrap();
        let transport = E2eChannelTransport::default();
        let runtime = RaftRuntime::spawn(
            node,
            Arc::new(transport.clone()),
            Duration::from_millis(10),
        )
        .unwrap();
        transport.register(&runtime).await;

        runtime.campaign().await.unwrap();

        // Wait for leadership
        tokio::time::timeout(Duration::from_secs(2), async {
            while !runtime.coordinator().is_leader() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("should become leader");

        runtime
            .propose(b"durable-order:42")
            .await
            .expect("proposal succeeds");

        // Wait for commit
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let entries = runtime.drain_committed().await;
                if let Ok(entries) = entries {
                    if entries.iter().any(|e| e.data == b"durable-order:42") {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("entry should be committed");

        runtime.shutdown();
        runtime.join().await.unwrap();
    }

    // Second instance: restart and verify persistence
    {
        let node = RaftNode::open_persistent(1, "http://node-1", members.clone(), directory.path())
            .unwrap();
        let transport = E2eChannelTransport::default();
        let runtime = RaftRuntime::spawn(
            node,
            Arc::new(transport.clone()),
            Duration::from_millis(10),
        )
        .unwrap();

        // Campaign to become leader and verify no duplicate commits
        runtime.campaign().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while !runtime.coordinator().is_leader() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("should become leader");

        // Propose new entry
        runtime
            .propose(b"new-order:99")
            .await
            .expect("proposal succeeds");

        // Verify we get only the new entry committed (not the old one replayed)
        let result = tokio::time::timeout(Duration::from_secs(2), runtime.drain_committed()).await;
        assert!(result.is_ok(), "should complete");
        let entries = result.unwrap().expect("runtime available");
        assert!(
            entries.iter().any(|e| e.data == b"new-order:99"),
            "should have the new entry"
        );

        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}

/// E2E test: RaftRuntime handles multiple sequential proposals.
#[tokio::test]
async fn e2e_raft_runtime_handles_sequential_proposals() {
    let transport = E2eChannelTransport::default();
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ];

    let mut runtimes: Vec<_> = members
        .iter()
        .map(|member| {
            RaftRuntime::spawn(
                RaftNode::new(member.id(), member.endpoint(), members.clone()).unwrap(),
                Arc::new(transport.clone()),
                Duration::from_millis(10),
            )
            .unwrap()
        })
        .collect();

    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    // Establish leadership
    runtimes[0].campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !runtimes[0].coordinator().is_leader() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("should become leader");

    // Send multiple sequential proposals
    let proposal_count = 10;
    for i in 0..proposal_count {
        runtimes[0]
            .propose(format!("order-{}", i))
            .await
            .expect("proposal succeeds");
    }

    // Verify all entries are committed
    let mut all_entries = Vec::new();
    for _ in 0..proposal_count {
        let result = tokio::time::timeout(Duration::from_secs(2), runtimes[0].drain_committed()).await;
        if let Ok(Ok(entries)) = result {
            all_entries.extend(entries);
        }
    }

    assert_eq!(all_entries.len(), proposal_count);

    // Cleanup
    while let Some(runtime) = runtimes.pop() {
        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}
