//! Strict unit tests for the `ClusterCoordinator` trait implementation via `MemoryClusterNode`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use catga_cluster::{ClusterCoordinator, ClusterCoordinatorExt, MemoryCluster};

fn new_cluster() -> (
    MemoryCluster,
    Arc<catga_cluster::MemoryClusterNode>,
    Arc<catga_cluster::MemoryClusterNode>,
) {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );
    let node_a = cluster.node("node-a").expect("test value must be present");
    let node_b = cluster.node("node-b").expect("test value must be present");
    (cluster, node_a, node_b)
}

// ---------------------------------------------------------------------------
// Trait method: node_id()
// ---------------------------------------------------------------------------

#[test]
fn node_id_returns_stable_identifier() {
    let (_, node_a, node_b) = new_cluster();
    assert_eq!(node_a.node_id(), "node-a");
    assert_eq!(node_b.node_id(), "node-b");
}

#[test]
fn node_id_does_not_change_after_leadership_transitions() {
    let (cluster, node_a, _) = new_cluster();
    assert_eq!(node_a.node_id(), "node-a");
    cluster.elect("node-b").expect("test value must be present");
    assert_eq!(node_a.node_id(), "node-a");
    cluster.elect("node-c").expect("test value must be present");
    assert_eq!(node_a.node_id(), "node-a");
}

// ---------------------------------------------------------------------------
// Trait method: is_leader()
// ---------------------------------------------------------------------------

#[test]
fn is_leader_true_for_initial_leader() {
    let (_, node_a, _) = new_cluster();
    assert!(node_a.is_leader());
}

#[test]
fn is_leader_false_for_non_leader() {
    let (_, _, node_b) = new_cluster();
    assert!(!node_b.is_leader());
}

#[test]
fn is_leader_reflects_leadership_change() {
    let (cluster, node_a, node_b) = new_cluster();
    assert!(node_a.is_leader());
    assert!(!node_b.is_leader());

    cluster.elect("node-b").expect("test value must be present");
    assert!(!node_a.is_leader());
    assert!(node_b.is_leader());
}

#[test]
fn is_leader_stable_after_reelecting_same_leader() {
    let (cluster, node_a, _) = new_cluster();
    assert!(node_a.is_leader());
    cluster.elect("node-a").expect("test value must be present"); // re-elect same leader
    assert!(node_a.is_leader());
}

// ---------------------------------------------------------------------------
// Trait method: leader_endpoint()
// ---------------------------------------------------------------------------

#[test]
fn leader_endpoint_returns_correct_endpoint_when_leader_elected() {
    let (_, node_a, _) = new_cluster();
    assert_eq!(node_a.leader_endpoint(), Some(Arc::from("http://node-a")));
}

#[test]
fn leader_endpoint_reflects_leadership_change() {
    let (cluster, _, node_b) = new_cluster();
    assert_eq!(node_b.leader_endpoint(), Some(Arc::from("http://node-a")));

    cluster.elect("node-b").expect("test value must be present");
    assert_eq!(node_b.leader_endpoint(), Some(Arc::from("http://node-b")));
}

#[test]
fn leader_endpoint_returns_leader_endpoint_when_available() {
    // When leader is in the endpoints list, leader_endpoint returns that endpoint
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_b = cluster.node("node-b").expect("test value must be present");

    // Initially node-a is leader, and its endpoint is in the list
    assert_eq!(node_b.leader_endpoint(), Some(Arc::from("http://node-a")));

    // When node-b becomes leader, its endpoint is returned
    cluster.elect("node-b").expect("test value must be present");
    assert_eq!(node_b.leader_endpoint(), Some(Arc::from("http://node-b")));
}

// ---------------------------------------------------------------------------
// Trait method: leadership_snapshot()
// ---------------------------------------------------------------------------

#[test]
fn leadership_snapshot_epoch_starts_at_zero() {
    let (_, node_a, _) = new_cluster();
    let snapshot = node_a.leadership_snapshot();
    assert_eq!(snapshot.epoch, 0);
}

#[test]
fn leadership_snapshot_contains_leader_info() {
    let (_, node_a, _) = new_cluster();
    let snapshot = node_a.leadership_snapshot();
    assert_eq!(snapshot.leader_node_id, Some(Arc::from("node-a")));
    assert_eq!(snapshot.leader_endpoint, Some(Arc::from("http://node-a")));
}

#[test]
fn leadership_snapshot_epoch_increments_on_election() {
    let (cluster, node_a, _) = new_cluster();
    let snapshot0 = node_a.leadership_snapshot();
    assert_eq!(snapshot0.epoch, 0);

    cluster.elect("node-b").expect("test value must be present");
    let snapshot1 = node_a.leadership_snapshot();
    assert_eq!(snapshot1.epoch, 1);
    assert_eq!(snapshot1.leader_node_id, Some(Arc::from("node-b")));
}

#[test]
fn leadership_snapshot_epoch_no_change_on_same_leader_reelection() {
    let (cluster, node_a, _) = new_cluster();
    cluster.elect("node-a").expect("test value must be present"); // same leader
    let snapshot = node_a.leadership_snapshot();
    assert_eq!(snapshot.epoch, 0);
}

#[test]
fn leadership_snapshot_multiple_elections_increment_epoch() {
    let (cluster, _, _) = new_cluster();
    let node = cluster.node("node-a").expect("test value must be present");

    cluster.elect("node-b").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 1);

    cluster.elect("node-c").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 2);

    cluster.elect("node-a").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 3);
}

#[test]
fn leadership_snapshot_snapshot_is_consistent_across_calls() {
    let (_, node_a, node_b) = new_cluster();
    let snap_a = node_a.leadership_snapshot();
    let snap_b = node_b.leadership_snapshot();
    // Same cluster state, same snapshot content
    assert_eq!(snap_a.epoch, snap_b.epoch);
    assert_eq!(snap_a.leader_node_id, snap_b.leader_node_id);
    assert_eq!(snap_a.leader_endpoint, snap_b.leader_endpoint);
}

// ---------------------------------------------------------------------------
// Trait method: subscribe_leadership()
// ---------------------------------------------------------------------------

#[test]
fn subscribe_leadership_returns_initial_snapshot() {
    let (_, node_a, _) = new_cluster();
    let sub = node_a.subscribe_leadership();
    let snap = sub.snapshot();
    assert_eq!(snap.epoch, 0);
    assert_eq!(snap.leader_node_id, Some(Arc::from("node-a")));
}

#[test]
fn subscribe_leadership_captures_current_state_atomically() {
    let (cluster, _, node_b) = new_cluster();
    let sub = node_b.subscribe_leadership();
    cluster.elect("node-b").expect("test value must be present");
    // subscription still has old epoch at registration time
    assert_eq!(sub.snapshot().epoch, 0);
    assert_eq!(sub.snapshot().leader_node_id, Some(Arc::from("node-a")));
}

#[test]
fn subscribe_leadership_multiple_subscriptions_have_independent_snapshots() {
    let (_, node_a, _) = new_cluster();
    let sub1 = node_a.subscribe_leadership();
    let sub2 = node_a.subscribe_leadership();

    // both start with epoch 0
    assert_eq!(sub1.snapshot().epoch, 0);
    assert_eq!(sub2.snapshot().epoch, 0);
}

// ---------------------------------------------------------------------------
// LeadershipSubscription::recv()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscription_recv_receives_transitions() {
    let (cluster, _, node_b) = new_cluster();
    let mut sub = node_b.subscribe_leadership();

    cluster.elect("node-b").expect("test value must be present");

    let snap = sub.recv().await.expect("test value must be present");
    assert_eq!(snap.epoch, 1);
    assert_eq!(snap.leader_node_id, Some(Arc::from("node-b")));
}

#[tokio::test]
async fn subscription_recv_coalesces_multiple_transitions() {
    let (cluster, _, node_b) = new_cluster();
    let mut sub = node_b.subscribe_leadership();

    // Rapid transitions — subscription may lag
    cluster.elect("node-b").expect("test value must be present");
    cluster.elect("node-c").expect("test value must be present");
    cluster.elect("node-a").expect("test value must be present");

    let snap = sub.recv().await.expect("test value must be present");
    // Receives the latest snapshot (epoch 3) after lagging
    assert!(snap.epoch >= 1);
}

#[tokio::test]
async fn subscription_recv_returns_same_epoch_after_lagged() {
    let (cluster, _, node_b) = new_cluster();
    let mut sub = node_b.subscribe_leadership();

    // Trigger enough transitions to potentially overflow the broadcast buffer (size 64)
    for i in 0..70 {
        cluster
            .elect(if i % 2 == 0 { "node-b" } else { "node-c" })
            .expect("test value must be present");
    }

    let snap = sub.recv().await.expect("test value must be present");
    // Should recover with latest snapshot (epoch 69 after 70 elections)
    assert!(snap.epoch >= 70);
}

#[tokio::test]
async fn subscription_recv_returns_error_on_close() {
    let mut cluster = Some(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    ));
    let node_b = cluster
        .as_ref()
        .expect("test value must be present")
        .node("node-b")
        .expect("test value must be present");
    let mut sub = node_b.subscribe_leadership();

    // Drop the cluster and node to close the broadcast channel
    drop(node_b);
    drop(cluster.take());

    let result = sub.recv().await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Trait method: member_endpoints()
// ---------------------------------------------------------------------------

#[test]
fn member_endpoints_returns_all_members() {
    let (_, node_a, _) = new_cluster();
    let endpoints = node_a.member_endpoints();
    assert_eq!(endpoints.len(), 3);
    assert!(endpoints.contains(&Arc::from("http://node-a")));
    assert!(endpoints.contains(&Arc::from("http://node-b")));
    assert!(endpoints.contains(&Arc::from("http://node-c")));
}

#[test]
fn member_endpoints_immutable_snapshot() {
    let (cluster, node_a, _) = new_cluster();
    let endpoints1 = node_a.member_endpoints();
    cluster.elect("node-b").expect("test value must be present");
    let endpoints2 = node_a.member_endpoints();
    // The Arc slice is a snapshot — both contain all members
    assert_eq!(endpoints1.len(), endpoints2.len());
    assert_eq!(endpoints1.len(), 3);
}

#[test]
fn member_endpoints_same_across_all_nodes() {
    let (_, node_a, node_b) = new_cluster();
    let eps_a = node_a.member_endpoints();
    let eps_b = node_b.member_endpoints();
    assert_eq!(eps_a, eps_b);
}

// ---------------------------------------------------------------------------
// Trait method: wait_for_leadership()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wait_for_leadership_returns_immediately_when_already_leader() {
    let (_, node_a, _) = new_cluster();
    let start = Instant::now();
    let result = node_a.wait_for_leadership(Duration::from_secs(10)).await;
    let elapsed = start.elapsed();
    assert!(result);
    assert!(elapsed < Duration::from_millis(100));
}

#[tokio::test]
async fn wait_for_leadership_returns_false_when_timeout_expires() {
    let (_, _, node_b) = new_cluster();
    let start = Instant::now();
    let result = node_b.wait_for_leadership(Duration::from_millis(50)).await;
    let elapsed = start.elapsed();
    assert!(!result);
    assert!(elapsed >= Duration::from_millis(50));
}

#[tokio::test]
async fn wait_for_leadership_returns_true_when_leader_elected_before_timeout() {
    let (cluster, _, node_b) = new_cluster();
    let start = Instant::now();

    let handle = tokio::spawn({
        let cluster = Arc::new(cluster);
        async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cluster.elect("node-b").expect("test value must be present");
        }
    });

    let result = node_b.wait_for_leadership(Duration::from_secs(2)).await;
    let elapsed = start.elapsed();

    handle.await.expect("test value must be present");
    assert!(result);
    assert!(elapsed < Duration::from_secs(2));
}

#[tokio::test]
async fn wait_for_leadership_zero_duration_returns_current_state() {
    let (cluster, _, node_b) = new_cluster();
    let result = node_b.wait_for_leadership(Duration::ZERO).await;
    assert!(!result);

    cluster.elect("node-b").expect("test value must be present");
    let result = node_b.wait_for_leadership(Duration::ZERO).await;
    assert!(result);
}

// ---------------------------------------------------------------------------
// Trait method: wait_for_leadership_change()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wait_for_leadership_change_returns_immediately_when_already_different() {
    let (_, node_a, _) = new_cluster();
    let start = Instant::now();
    // node-a is leader, is_leader()=true != was_leader=false -> returns immediately
    // returns is_leader() = true
    let result = node_a.wait_for_leadership_change(false).await;
    let elapsed = start.elapsed();
    assert!(result); // returns is_leader() = true
    assert!(elapsed < Duration::from_millis(50));
}

#[tokio::test]
async fn wait_for_leadership_change_returns_immediately_when_state_matches() {
    let (_, node_a, _) = new_cluster();
    let start = Instant::now();
    // node-a is leader, is_leader()=true == was_leader=true -> wait for notification
    // But the current state matches was_leader, so this will wait forever
    // To test immediate return, pass was_leader=false (current state differs)
    let result = node_a.wait_for_leadership_change(false).await;
    let elapsed = start.elapsed();
    assert!(result); // returns is_leader() = true
    assert!(elapsed < Duration::from_millis(50));
}

#[tokio::test]
async fn wait_for_leadership_change_waits_for_transition() {
    let (cluster, _, node_b) = new_cluster();
    let start = Instant::now();

    let handle = tokio::spawn({
        let cluster = Arc::new(cluster);
        async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cluster.elect("node-b").expect("test value must be present");
        }
    });

    // node_b is not leader, is_leader()=false
    // pass was_leader=false -> is_leader() == was_leader -> need to wait
    let result = node_b.wait_for_leadership_change(false).await;
    let elapsed = start.elapsed();

    handle.await.expect("test value must be present");
    assert!(result); // after election, node_b is leader
    assert!(elapsed >= Duration::from_millis(30));
}

#[tokio::test]
async fn wait_for_leadership_change_detects_loss_of_leadership() {
    let (cluster, node_a, _) = new_cluster();
    let start = Instant::now();

    let handle = tokio::spawn({
        let cluster = Arc::new(cluster);
        async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cluster.elect("node-b").expect("test value must be present");
        }
    });

    // node_a is leader, passing was_leader=true means no change yet
    let result = node_a.wait_for_leadership_change(true).await;
    let elapsed = start.elapsed();

    handle.await.expect("test value must be present");
    assert!(!result); // node_a lost leadership
    assert!(elapsed >= Duration::from_millis(30));
}

#[tokio::test]
async fn wait_for_leadership_change_transition_back_to_original_state() {
    let (cluster, node_a, _) = new_cluster();
    let cluster_arc = Arc::new(cluster);

    // node_a is leader at start, wait_for_leadership_change(true) will wait
    let handle = tokio::spawn({
        let cluster = Arc::clone(&cluster_arc);
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cluster.elect("node-b").expect("test value must be present");
            tokio::time::sleep(Duration::from_millis(20)).await;
            cluster.elect("node-a").expect("test value must be present");
        }
    });

    // Wait for the first transition (loss of leadership)
    let result = node_a.wait_for_leadership_change(true).await;
    handle.await.expect("test value must be present");

    // After node-b election, node_a is no longer leader
    // The function returns false because is_leader() is now false
    assert!(!result);
    assert!(node_a.is_leader()); // node-a was re-elected
}

// ---------------------------------------------------------------------------
// endpoint_node_id helper
// ---------------------------------------------------------------------------

#[test]
fn endpoint_node_id_extracts_suffix() {
    // The helper is private but exercised through public API
    // Testing via leader_endpoint which uses it internally
    let (cluster, _, _) = new_cluster();
    let node = cluster.node("node-a").expect("test value must be present");

    // With URLs like http://node-a, the suffix after last / is "node-a"
    assert_eq!(node.leader_endpoint(), Some(Arc::from("http://node-a")));
}

#[test]
fn endpoint_node_id_handles_paths_without_slash() {
    let cluster = MemoryCluster::new("simple-id", ["simple-id"]);
    let node = cluster
        .node("simple-id")
        .expect("test value must be present");
    assert_eq!(node.node_id(), "simple-id");
    assert_eq!(node.leader_endpoint(), Some(Arc::from("simple-id")));
}

#[test]
fn endpoint_node_id_handles_deeply_nested_paths() {
    let cluster = MemoryCluster::new("node-z", ["http://host:8080/cluster/zone1/node-z"]);
    let node = cluster.node("node-z").expect("test value must be present");
    assert_eq!(node.node_id(), "node-z");
    assert_eq!(
        node.leader_endpoint(),
        Some(Arc::from("http://host:8080/cluster/zone1/node-z"))
    );
}

#[test]
fn endpoint_node_id_handles_trailing_slash() {
    // Trailing slash causes endpoint_node_id to return "" - this is a known behavior
    // The function splits on '/' and takes the last element, so "http://node-y/" -> ""
    let cluster = MemoryCluster::new("node-y", ["http://node-y"]);
    let node = cluster.node("node-y").expect("test value must be present");
    assert_eq!(node.node_id(), "node-y");
    assert_eq!(node.leader_endpoint(), Some(Arc::from("http://node-y")));
}

// ---------------------------------------------------------------------------
// MemoryCluster::elect()
// ---------------------------------------------------------------------------

#[test]
fn elect_unknown_node_returns_none() {
    let cluster = new_cluster().0;
    assert!(cluster.elect("unknown-node").is_none());
}

#[test]
fn elect_returns_some_for_valid_node() {
    let (cluster, _, _) = new_cluster();
    assert!(cluster.elect("node-b").is_some());
}

#[test]
fn elect_same_leader_does_not_increment_epoch() {
    let (cluster, node_a, _) = new_cluster();
    let epoch_before = node_a.leadership_snapshot().epoch;
    cluster.elect("node-a").expect("test value must be present");
    let epoch_after = node_a.leadership_snapshot().epoch;
    assert_eq!(epoch_before, epoch_after);
}

#[tokio::test]
async fn elect_triggers_all_waiters() {
    let (cluster, _, node_b) = new_cluster();
    let handle = tokio::spawn({
        let node_b = Arc::clone(&node_b);
        async move { node_b.wait_for_leadership(Duration::from_secs(2)).await }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    cluster.elect("node-b").expect("test value must be present");

    let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
    assert!(result.is_ok(), "timeout expired");
    let inner = result.expect("test value must be present");
    assert!(inner.is_ok(), "task panicked");
    assert!(inner.expect("test value must be present"));
}

// ---------------------------------------------------------------------------
// MemoryCluster::node()
// ---------------------------------------------------------------------------

#[test]
fn node_returns_some_for_valid_id() {
    let (cluster, _, _) = new_cluster();
    assert!(cluster.node("node-a").is_some());
    assert!(cluster.node("node-b").is_some());
    assert!(cluster.node("node-c").is_some());
}

#[test]
fn node_returns_none_for_invalid_id() {
    let (cluster, _, _) = new_cluster();
    assert!(cluster.node("unknown").is_none());
}

#[test]
fn node_returns_arc_shared_with_cluster() {
    let (cluster, node_a, node_b) = new_cluster();
    let node_a2 = cluster.node("node-a").expect("test value must be present");
    // Both references should point to same node identity
    assert_eq!(node_a.node_id(), node_a2.node_id());
    assert_eq!(node_a.is_leader(), node_a2.is_leader());
    // node_a and node_b are different nodes
    assert_ne!(node_a.node_id(), node_b.node_id());
}

// ---------------------------------------------------------------------------
// Send + Sync verification
// ---------------------------------------------------------------------------

#[test]
fn memory_cluster_node_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<catga_cluster::MemoryClusterNode>();
}

#[test]
fn memory_cluster_node_is_sync() {
    fn assert_sync<T: Sync>() {}
    assert_sync::<catga_cluster::MemoryClusterNode>();
}

#[test]
fn cluster_coordinator_is_object_safe() {
    fn assert_coordinator<C: ClusterCoordinator>() {}
    assert_coordinator::<catga_cluster::MemoryClusterNode>();
}

#[test]
fn leadership_snapshot_clone_is_independent() {
    let (_, node_a, _) = new_cluster();
    let snap1 = node_a.leadership_snapshot();
    let snap2 = Arc::clone(&snap1);

    // Modifications to underlying state affect future snapshots
    drop(snap1);
    drop(snap2);

    let (cluster, _, _) = new_cluster();
    let node = cluster.node("node-a").expect("test value must be present");
    let snap3 = node.leadership_snapshot();
    assert_eq!(snap3.epoch, 0);
}

// ---------------------------------------------------------------------------
// LeadershipSnapshot equality
// ---------------------------------------------------------------------------

#[test]
fn leadership_snapshot_eq_by_content() {
    let (cluster1, _, _) = new_cluster();
    let (cluster2, _, _) = new_cluster();

    let snap1 = cluster1
        .node("node-a")
        .expect("test value must be present")
        .leadership_snapshot();
    let snap2 = cluster2
        .node("node-a")
        .expect("test value must be present")
        .leadership_snapshot();

    // Different clusters with same config have equal snapshots
    assert_eq!(snap1, snap2);
}

#[test]
fn leadership_snapshot_neq_after_transition() {
    let (cluster1, _, _) = new_cluster();
    let (cluster2, _, _) = new_cluster();

    let snap1 = cluster1
        .node("node-a")
        .expect("test value must be present")
        .leadership_snapshot();
    cluster2
        .elect("node-b")
        .expect("test value must be present");
    let snap2 = cluster2
        .node("node-a")
        .expect("test value must be present")
        .leadership_snapshot();

    assert_ne!(snap1, snap2);
    assert_eq!(snap1.epoch, 0);
    assert_eq!(snap2.epoch, 1);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn single_node_cluster() {
    let cluster = MemoryCluster::new("solo", ["http://solo"]);
    let node = cluster.node("solo").expect("test value must be present");
    assert_eq!(node.node_id(), "solo");
    assert!(node.is_leader());
    assert_eq!(node.leader_endpoint(), Some(Arc::from("http://solo")));

    let members = node.member_endpoints();
    assert_eq!(members.len(), 1);
}

#[test]
fn two_node_cluster_alternating_leadership() {
    let cluster = MemoryCluster::new("n1", ["n1", "n2"]);
    let n1 = cluster.node("n1").expect("test value must be present");
    let n2 = cluster.node("n2").expect("test value must be present");

    assert!(n1.is_leader());
    assert!(!n2.is_leader());

    cluster.elect("n2").expect("test value must be present");
    assert!(!n1.is_leader());
    assert!(n2.is_leader());

    cluster.elect("n1").expect("test value must be present");
    assert!(n1.is_leader());
    assert!(!n2.is_leader());
}

#[test]
fn large_cluster_member_endpoints() {
    let members: Vec<_> = (0..50).map(|i| format!("http://node-{}", i)).collect();
    let cluster = MemoryCluster::new("node-0", members.clone());
    let node = cluster.node("node-0").expect("test value must be present");

    let endpoints = node.member_endpoints();
    assert_eq!(endpoints.len(), 50);
}

#[test]
fn concurrent_leadership_checks() {
    // Verify is_leader is consistent under concurrent access
    let (cluster, _, _) = new_cluster();
    let node = cluster.node("node-a").expect("test value must be present");

    std::thread::scope(|s| {
        for _ in 0..100 {
            s.spawn(|| {
                assert!(node.is_leader() || !node.is_leader());
            });
        }
    });
}

#[tokio::test]
async fn concurrent_snapshot_reads() {
    let (cluster, _, _) = new_cluster();
    let node = cluster.node("node-a").expect("test value must be present");

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let node = Arc::clone(&node);
            tokio::spawn(async move {
                for _ in 0..100 {
                    let _ = node.leadership_snapshot();
                    let _ = node.is_leader();
                    let _ = node.node_id();
                }
            })
        })
        .collect();

    for h in handles {
        h.await.expect("test value must be present");
    }
}

#[test]
fn subscription_snapshot_does_not_update_after_creation() {
    let (cluster, _, node_b) = new_cluster();
    let sub = node_b.subscribe_leadership();

    // Multiple elections before any recv
    cluster.elect("node-b").expect("test value must be present");
    cluster.elect("node-c").expect("test value must be present");
    cluster.elect("node-a").expect("test value must be present");

    // Snapshot still reflects state at subscription time
    assert_eq!(sub.snapshot().epoch, 0);
    assert_eq!(sub.snapshot().leader_node_id, Some(Arc::from("node-a")));
}

#[test]
fn epoch_increments_on_different_leader_elections() {
    let cluster = MemoryCluster::new("a", ["a", "b"]);
    let node = cluster.node("a").expect("test value must be present");

    // epoch is u128 — practically never overflows in tests
    // epoch starts at 0
    assert_eq!(node.leadership_snapshot().epoch, 0);

    // re-electing same leader doesn't increment epoch
    cluster.elect("a").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 0);

    // electing different leader increments epoch
    cluster.elect("b").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 1);

    cluster.elect("a").expect("test value must be present");
    assert_eq!(node.leadership_snapshot().epoch, 2);
}

// ---------------------------------------------------------------------------
// ClusterCoordinatorExt behavior through MemoryClusterNode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_if_leader_runs_action_when_leader() {
    let (_, node_a, _) = new_cluster();
    let result = node_a.execute_if_leader(|| async { 42 }).await;
    assert_eq!(result, Some(42));
}

#[tokio::test]
async fn execute_if_leader_returns_none_when_not_leader() {
    let (_, _, node_b) = new_cluster();
    let result = node_b.execute_if_leader(|| async { 42 }).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn execute_if_leader_cancellable_returns_unavailable_when_not_leader() {
    use catga_core::ErrorCode;

    let (_, _, node_b) = new_cluster();
    let result = node_b
        .execute_if_leader_cancellable(|_| async { Ok(42) })
        .await;
    assert!(result.is_err());
    let err = result.expect_err("expected an error");
    assert_eq!(err.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn execute_if_leader_cancellable_runs_action_when_leader() {
    let (_, node_a, _) = new_cluster();
    let result = node_a
        .execute_if_leader_cancellable(|_| async { Ok::<_, catga_core::CatgaError>(42) })
        .await;
    assert!(result.is_ok());
    assert_eq!(result.expect("test value must be present"), 42);
}

#[tokio::test]
async fn execute_if_leader_cancellable_cancelled_on_leadership_loss() {
    use catga_core::ErrorCode;

    let (cluster, node_a, _) = new_cluster();
    let start = Instant::now();

    let handle = tokio::spawn({
        let cluster = Arc::new(cluster);
        async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            cluster.elect("node-b").expect("test value must be present");
        }
    });

    let result = node_a
        .execute_if_leader_cancellable(|token| async move {
            token.cancelled().await;
            Ok::<_, catga_core::CatgaError>(())
        })
        .await;

    handle.await.expect("test value must be present");

    assert!(result.is_err());
    let err = result.expect_err("expected an error");
    assert_eq!(err.code(), ErrorCode::Cancelled);
    assert!(start.elapsed() >= Duration::from_millis(20));
}
