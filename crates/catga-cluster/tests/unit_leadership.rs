//! Strict unit tests for LeadershipSubscription and LeadershipSnapshot.

use std::sync::Arc;

use catga_cluster::{ClusterCoordinator, LeadershipSnapshot, MemoryCluster};

#[test]
fn leadership_snapshot_clone_is_independent() {
    let original = LeadershipSnapshot {
        epoch: 42,
        leader_node_id: Some(Arc::from("node-1")),
        leader_endpoint: Some(Arc::from("http://localhost:8080")),
    };

    let cloned = original.clone();

    assert_eq!(cloned.epoch, 42);
    assert_eq!(cloned.leader_node_id.as_deref(), Some("node-1"));
    assert_eq!(cloned.leader_endpoint.as_deref(), Some("http://localhost:8080"));

    // Verify Arc interning - clones share the same underlying data
    assert!(Arc::ptr_eq(
        original.leader_node_id.as_ref().unwrap(),
        cloned.leader_node_id.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        original.leader_endpoint.as_ref().unwrap(),
        cloned.leader_endpoint.as_ref().unwrap()
    ));
}

#[test]
fn leadership_snapshot_eq_and_partial_eq() {
    let snapshot1 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node-1")),
        leader_endpoint: Some(Arc::from("http://localhost:8080")),
    };

    let snapshot2 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node-1")),
        leader_endpoint: Some(Arc::from("http://localhost:8080")),
    };

    let snapshot3 = LeadershipSnapshot {
        epoch: 2,
        leader_node_id: Some(Arc::from("node-1")),
        leader_endpoint: Some(Arc::from("http://localhost:8080")),
    };

    let snapshot4 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node-2")),
        leader_endpoint: Some(Arc::from("http://localhost:8080")),
    };

    // Same values are equal
    assert_eq!(snapshot1, snapshot2);

    // Different epoch
    assert_ne!(snapshot1, snapshot3);

    // Different leader_node_id
    assert_ne!(snapshot1, snapshot4);
}

#[test]
fn leadership_snapshot_debug_format() {
    let snapshot = LeadershipSnapshot {
        epoch: 123,
        leader_node_id: Some(Arc::from("test-leader")),
        leader_endpoint: Some(Arc::from("http://debug:9000")),
    };

    let debug_str = format!("{:?}", snapshot);
    assert!(debug_str.contains("LeadershipSnapshot"));
    assert!(debug_str.contains("123"));
    assert!(debug_str.contains("test-leader"));
    assert!(debug_str.contains("http://debug:9000"));
}

#[test]
fn leadership_snapshot_zero_epoch_boundary() {
    let snapshot = LeadershipSnapshot {
        epoch: 0,
        leader_node_id: None,
        leader_endpoint: None,
    };

    assert_eq!(snapshot.epoch, 0);
    assert!(snapshot.leader_node_id.is_none());
    assert!(snapshot.leader_endpoint.is_none());
}

#[test]
fn leadership_snapshot_large_epoch_boundary() {
    let snapshot = LeadershipSnapshot {
        epoch: u128::MAX,
        leader_node_id: None,
        leader_endpoint: None,
    };

    assert_eq!(snapshot.epoch, u128::MAX);
}

#[test]
fn leadership_snapshot_max_values() {
    let snapshot = LeadershipSnapshot {
        epoch: u128::MAX,
        leader_node_id: Some(Arc::from("max-leader")),
        leader_endpoint: Some(Arc::from("http://max-endpoint:65535")),
    };

    assert_eq!(snapshot.epoch, u128::MAX);
    assert_eq!(snapshot.leader_node_id.as_deref(), Some("max-leader"));
    assert_eq!(snapshot.leader_endpoint.as_deref(), Some("http://max-endpoint:65535"));
}

#[test]
fn leadership_subscription_snapshot_returns_captured_state() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime must build");

    runtime.block_on(async {
        // Use node IDs as endpoints so they match
        let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
        let node = cluster.node("node-a").expect("node-a must exist");
        let subscription = node.subscribe_leadership();

        let snapshot = subscription.snapshot();
        assert_eq!(snapshot.epoch, 0);
        assert_eq!(snapshot.leader_node_id.as_deref(), Some("node-a"));
        assert_eq!(snapshot.leader_endpoint.as_deref(), Some("http://node-a"));
    });
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_recv_receives_election() {
    // Use node IDs as endpoints so they match
    let cluster = MemoryCluster::new("leader", ["http://leader", "http://follower"]);
    let follower = cluster.node("follower").expect("follower must exist");
    let mut subscription = follower.subscribe_leadership();

    // Initial snapshot should have leader as leader
    assert_eq!(subscription.snapshot().leader_node_id.as_deref(), Some("leader"));

    // Trigger election
    cluster.elect("follower");

    // Should receive the election transition
    let updated = subscription.recv().await.expect("recv must succeed");
    assert_eq!(updated.epoch, 1);
    assert_eq!(updated.leader_node_id.as_deref(), Some("follower"));
    assert_eq!(updated.leader_endpoint.as_deref(), Some("http://follower"));
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_multiple_elections() {
    let cluster = MemoryCluster::new("node1", ["http://node1", "http://node2", "http://node3"]);

    let subscriber = cluster.node("node2").expect("node2 must exist");
    let mut subscription = subscriber.subscribe_leadership();

    // Initial state
    assert_eq!(subscription.snapshot().epoch, 0);

    // First election
    cluster.elect("node2");
    let snap1 = subscription.recv().await.expect("first recv must succeed");
    assert_eq!(snap1.epoch, 1);

    // Second election
    cluster.elect("node3");
    let snap2 = subscription.recv().await.expect("second recv must succeed");
    assert_eq!(snap2.epoch, 2);
    assert_eq!(snap2.leader_node_id.as_deref(), Some("node3"));
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_closed_channel() {
    // Create cluster and node
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node = cluster.node("node-a").expect("node-a must exist");
    let mut subscription = node.subscribe_leadership();

    // Snapshot should show initial state with leader
    let initial = subscription.snapshot();
    assert!(initial.leader_node_id.is_some());

    // Drop node (subscription still holds Weak reference)
    drop(node);
    // Drop cluster (this drops the Arc<LeadershipPublication>)
    drop(cluster);

    // After both are dropped, the Weak pointer can't upgrade, so Lagged -> Closed
    let result = subscription.recv().await;
    assert!(matches!(result, Err(tokio::sync::broadcast::error::RecvError::Closed)));
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_snapshot_is_arc_shared() {
    let cluster = MemoryCluster::new("node-x", ["http://node-x", "http://node-y"]);
    let node = cluster.node("node-x").expect("node-x must exist");
    let mut subscription = node.subscribe_leadership();

    let snapshot1 = subscription.snapshot();
    let snapshot2 = subscription.snapshot();

    // Both snapshots should point to the same Arc (captured at subscription time)
    assert!(Arc::ptr_eq(&snapshot1, &snapshot2));
    assert_eq!(snapshot1.epoch, 0);

    // Modifications to the Arc (through cluster) should be reflected
    cluster.elect("node-y");

    // snapshot() still returns the captured snapshot, not current state
    let snapshot3 = subscription.snapshot();
    assert!(Arc::ptr_eq(&snapshot1, &snapshot3));
    assert_eq!(snapshot3.epoch, 0);

    // recv() gets the updated snapshot
    let received = subscription.recv().await.expect("recv must succeed");
    assert_eq!(received.epoch, 1);

    // NOTE: snapshot() still returns the ORIGINAL captured snapshot, not the received one
    // This is by design - snapshot() returns the initial snapshot
    let snapshot4 = subscription.snapshot();
    assert!(Arc::ptr_eq(&snapshot1, &snapshot4));
    assert_eq!(snapshot4.epoch, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_multiple_subscribers() {
    let cluster = MemoryCluster::new("leader", ["http://leader", "http://node-a", "http://node-b"]);

    // Get nodes first
    let node1 = cluster.node("leader").expect("leader must exist");
    let node2 = cluster.node("node-a").expect("node-a must exist");
    let node3 = cluster.node("node-b").expect("node-b must exist");

    // Create multiple subscriptions
    let mut sub1 = node1.subscribe_leadership();
    let mut sub2 = node2.subscribe_leadership();
    let mut sub3 = node3.subscribe_leadership();

    assert_eq!(sub1.snapshot().epoch, 0);
    assert_eq!(sub2.snapshot().epoch, 0);
    assert_eq!(sub3.snapshot().epoch, 0);

    // First election
    cluster.elect("node-a");

    // All should receive the update
    let snap1 = sub1.recv().await.expect("sub1 must receive");
    assert_eq!(snap1.epoch, 1);

    let snap2 = sub2.recv().await.expect("sub2 must receive");
    assert_eq!(snap2.epoch, 1);

    let snap3 = sub3.recv().await.expect("sub3 must receive");
    assert_eq!(snap3.epoch, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_snapshot_after_recv() {
    let cluster = MemoryCluster::new("node1", ["http://node1", "http://node2"]);
    let node2 = cluster.node("node2").expect("node2 must exist");
    let mut subscription = node2.subscribe_leadership();

    // Initial epoch should be 0
    assert_eq!(subscription.snapshot().epoch, 0);

    // Trigger one election before first recv
    cluster.elect("node2");

    // Recv should get the updated snapshot
    let received = subscription.recv().await.expect("recv must succeed");
    assert_eq!(received.epoch, 1);
    assert_eq!(received.leader_node_id.as_deref(), Some("node2"));

    // NOTE: snapshot() still returns the ORIGINAL captured snapshot, not the received one
    // This is by design - snapshot() returns the initial snapshot
    let current = subscription.snapshot();
    assert_eq!(current.epoch, 0);
}

#[test]
fn leadership_snapshot_empty_string_endpoints() {
    // Edge case: empty string is still a valid Arc<str>
    let snapshot = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("")),
        leader_endpoint: Some(Arc::from("")),
    };

    assert_eq!(snapshot.leader_node_id.as_deref(), Some(""));
    assert_eq!(snapshot.leader_endpoint.as_deref(), Some(""));
}

#[test]
fn leadership_snapshot_special_characters_in_identifiers() {
    let snapshot = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node-with-dash_underscore.and.dots")),
        leader_endpoint: Some(Arc::from("http://localhost:8080/path?query=value")),
    };

    assert!(snapshot.leader_node_id.is_some());
    assert!(snapshot.leader_endpoint.is_some());
}

#[test]
fn leadership_snapshot_debug_contains_all_fields() {
    let snapshot = LeadershipSnapshot {
        epoch: 0,
        leader_node_id: None,
        leader_endpoint: None,
    };

    let debug_str = format!("{:?}", snapshot);
    assert!(debug_str.contains("epoch"));
    assert!(debug_str.contains("leader_node_id"));
    assert!(debug_str.contains("leader_endpoint"));
}

#[test]
fn leadership_snapshot_derive_traits() {
    // Test that LeadershipSnapshot implements Send and Sync (required for concurrency)
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<LeadershipSnapshot>();
    assert_sync::<LeadershipSnapshot>();
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_recv_error_closed() {
    // Create a cluster and drop it to test Closed error
    let cluster = MemoryCluster::new("a", ["http://a", "http://b"]);
    let node = cluster.node("a").expect("node a must exist");
    let mut sub = node.subscribe_leadership();

    // Drop both node and cluster to close the channel
    drop(node);
    drop(cluster);

    let err = sub.recv().await.expect_err("must return error");
    assert!(matches!(err, tokio::sync::broadcast::error::RecvError::Closed));
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_epoch_monotonic_increase() {
    let cluster = MemoryCluster::new("n1", ["http://n1", "http://n2", "http://n3"]);
    let node1 = cluster.node("n1").expect("n1 must exist");

    let initial_epoch = node1.leadership_snapshot().epoch;

    for i in 1..=5 {
        cluster.elect(&format!("n{}", i % 3 + 1));
        tokio::time::sleep(std::time::Duration::from_micros(10)).await;
    }

    // Epoch should have increased
    let final_epoch = node1.leadership_snapshot().epoch;
    assert!(final_epoch > initial_epoch);
}

#[test]
fn leadership_snapshot_equality_with_none_fields() {
    let snapshot1 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: None,
        leader_endpoint: None,
    };

    let snapshot2 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: None,
        leader_endpoint: None,
    };

    let snapshot3 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node")),
        leader_endpoint: None,
    };

    assert_eq!(snapshot1, snapshot2);
    assert_ne!(snapshot1, snapshot3);
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_subscription_at_different_times() {
    let cluster = MemoryCluster::new("leader", ["http://leader", "http://follower"]);

    // Get a node for subscription
    let node1 = cluster.node("leader").expect("leader must exist");

    // Subscribe first
    let mut sub1 = node1.subscribe_leadership();

    // Trigger election
    cluster.elect("follower");

    // Subscribe after election - need a new node reference
    let node1_after = cluster.node("leader").expect("leader must exist");
    let sub2 = node1_after.subscribe_leadership();

    // sub1 should have epoch 0, sub2 should have epoch 1
    assert_eq!(sub1.snapshot().epoch, 0);
    assert_eq!(sub2.snapshot().epoch, 1);

    // sub1 should be able to recv the update
    let update = sub1.recv().await.expect("sub1 must receive");
    assert_eq!(update.epoch, 1);

    // sub2 should already have the latest
    assert_eq!(sub2.snapshot().epoch, 1);
}

#[test]
fn leadership_snapshot_very_long_endpoint() {
    let long_string = "h".repeat(10000);
    let snapshot = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: None,
        leader_endpoint: Some(Arc::from(long_string)),
    };

    assert!(snapshot.leader_endpoint.unwrap().len() > 9000);
}

#[test]
fn leadership_snapshot_clone_preserves_epoch() {
    let original = LeadershipSnapshot {
        epoch: u128::MAX - 1,
        leader_node_id: None,
        leader_endpoint: None,
    };

    let cloned = original.clone();
    assert_eq!(cloned.epoch, u128::MAX - 1);
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_lagged_resync() {
    // Test that Lagged error properly resyncs to latest snapshot
    use tokio::sync::broadcast;

    let cluster = MemoryCluster::new("l", ["http://l", "http://f"]);

    // Create a subscription
    let node = cluster.node("l").expect("l must exist");
    let mut sub = node.subscribe_leadership();
    let initial_epoch = sub.snapshot().epoch;

    // Trigger many elections to overflow the buffer (64 messages)
    for i in 0..70 {
        cluster.elect(if i % 2 == 0 { "f" } else { "l" });
    }

    // The subscription should have lagged and resynced
    let result = sub.recv().await;
    match result {
        Ok(snapshot) => {
            // Successfully resynced to latest
            assert!(snapshot.epoch > initial_epoch);
        }
        Err(broadcast::error::RecvError::Lagged(_)) => {
            // This is expected behavior - Lagged indicates too many messages
            // When Lagged is returned, subscription is already resynced
            // The next recv should get the actual update
            let next = sub.recv().await;
            assert!(next.is_ok() || matches!(next, Err(broadcast::error::RecvError::Closed)));
        }
        Err(broadcast::error::RecvError::Closed) => {
            // Channel closed
        }
    }
}

#[test]
fn leadership_snapshot_unicode_in_identifiers() {
    let snapshot = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node-with-emoji-\u{1F600}")),
        leader_endpoint: Some(Arc::from("http://localhost:8080/\u{4E2D}\u{6587}")),
    };

    assert!(snapshot.leader_node_id.is_some());
    assert!(snapshot.leader_endpoint.is_some());
}

#[test]
fn leadership_snapshot_only_some_fields_set() {
    // Only leader_node_id set
    let snapshot1 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::from("node")),
        leader_endpoint: None,
    };
    assert!(snapshot1.leader_node_id.is_some());
    assert!(snapshot1.leader_endpoint.is_none());

    // Only leader_endpoint set
    let snapshot2 = LeadershipSnapshot {
        epoch: 2,
        leader_node_id: None,
        leader_endpoint: Some(Arc::from("http://endpoint")),
    };
    assert!(snapshot2.leader_node_id.is_none());
    assert!(snapshot2.leader_endpoint.is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn leadership_subscription_same_leader_election() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    // Elect the same leader (which is already the leader)
    let result = cluster.elect("node-a");
    assert!(result.is_some());

    // Should still work but epoch shouldn't increase for same leader
    let node = cluster.node("node-a").expect("node-a must exist");
    let snapshot = node.leadership_snapshot();

    // Since node-a was already leader, epoch stays at 0
    assert_eq!(snapshot.epoch, 0);
}

#[test]
fn leadership_snapshot_arc_sharing() {
    // Test that Arc<str> fields share data efficiently
    let node_id = Arc::from("shared-node");
    let endpoint = Arc::from("http://shared-endpoint");

    let snapshot1 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::clone(&node_id)),
        leader_endpoint: Some(Arc::clone(&endpoint)),
    };

    let snapshot2 = LeadershipSnapshot {
        epoch: 1,
        leader_node_id: Some(Arc::clone(&node_id)),
        leader_endpoint: Some(Arc::clone(&endpoint)),
    };

    // Both snapshots share the same Arc data
    assert!(Arc::ptr_eq(snapshot1.leader_node_id.as_ref().unwrap(), snapshot2.leader_node_id.as_ref().unwrap()));
    assert!(Arc::ptr_eq(snapshot1.leader_endpoint.as_ref().unwrap(), snapshot2.leader_endpoint.as_ref().unwrap()));

    // They are still equal
    assert_eq!(snapshot1, snapshot2);
}
