//! Unit tests for MemoryCluster and MemoryClusterNode.

use std::sync::Arc;
use std::time::Duration;

use catga_cluster::{ClusterCoordinator, MemoryCluster};

#[test]
fn memory_cluster_new_single_node() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a"]);
    let node = cluster.node("node-a");
    assert!(node.is_some());
    let node = node.unwrap();
    assert_eq!(node.node_id(), "node-a");
    assert!(node.is_leader());
}

#[test]
fn memory_cluster_new_multi_node() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node_a = cluster.node("node-a");
    let node_b = cluster.node("node-b");
    let node_c = cluster.node("node-c");

    assert!(node_a.is_some());
    assert!(node_b.is_some());
    assert!(node_c.is_some());

    assert!(node_a.unwrap().is_leader());
    assert!(!node_b.unwrap().is_leader());
    assert!(!node_c.unwrap().is_leader());
}

#[test]
fn memory_cluster_node_nonexistent() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node = cluster.node("nonexistent");
    assert!(node.is_none());
}

#[test]
fn memory_cluster_elect_changes_leader() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert!(node_a.is_leader());
    assert!(!node_b.is_leader());

    cluster.elect("node-b").expect("election should succeed");

    assert!(!node_a.is_leader());
    assert!(node_b.is_leader());
}

#[test]
fn memory_cluster_elect_same_leader_returns_early() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node_a = cluster.node("node-a").unwrap();
    assert!(node_a.is_leader());

    let result = cluster.elect("node-a");
    assert!(result.is_some());

    assert!(node_a.is_leader());
}

#[test]
fn memory_cluster_elect_nonexistent_returns_none() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let result = cluster.elect("nonexistent");
    assert!(result.is_none());
}

#[test]
fn memory_cluster_elect_multiple_transitions() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();
    let node_c = cluster.node("node-c").unwrap();

    assert!(node_a.is_leader());

    cluster.elect("node-b").expect("election should succeed");
    assert!(!node_a.is_leader());
    assert!(node_b.is_leader());
    assert!(!node_c.is_leader());

    cluster.elect("node-c").expect("election should succeed");
    assert!(!node_a.is_leader());
    assert!(!node_b.is_leader());
    assert!(node_c.is_leader());

    cluster.elect("node-a").expect("election should succeed");
    assert!(node_a.is_leader());
    assert!(!node_b.is_leader());
    assert!(!node_c.is_leader());
}

#[test]
fn memory_cluster_node_leader_endpoint() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a:8080", "http://node-b:8080"]);

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert_eq!(
        node_a.leader_endpoint(),
        Some(Arc::from("http://node-a:8080"))
    );
    assert_eq!(
        node_b.leader_endpoint(),
        Some(Arc::from("http://node-a:8080"))
    );

    cluster.elect("node-b").expect("election should succeed");

    assert_eq!(
        node_a.leader_endpoint(),
        Some(Arc::from("http://node-b:8080"))
    );
    assert_eq!(
        node_b.leader_endpoint(),
        Some(Arc::from("http://node-b:8080"))
    );
}

#[test]
fn memory_cluster_node_member_endpoints() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node = cluster.node("node-a").unwrap();
    let endpoints = node.member_endpoints();

    assert_eq!(endpoints.len(), 3);
    assert!(endpoints.contains(&Arc::from("http://node-a")));
    assert!(endpoints.contains(&Arc::from("http://node-b")));
    assert!(endpoints.contains(&Arc::from("http://node-c")));
}

#[test]
fn memory_cluster_node_leadership_snapshot() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let snapshot = node.leadership_snapshot();

    assert_eq!(snapshot.epoch, 0);
    assert_eq!(
        snapshot.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-a")
    );
    assert_eq!(
        snapshot.leader_endpoint.as_ref().map(|s| s.as_ref()),
        Some("http://node-a")
    );
}

#[test]
fn memory_cluster_node_leadership_snapshot_after_election() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let snapshot_before = node.leadership_snapshot();
    assert_eq!(snapshot_before.epoch, 0);

    cluster.elect("node-b").expect("election should succeed");

    let snapshot_after = node.leadership_snapshot();
    assert_eq!(snapshot_after.epoch, 1);
    assert_eq!(
        snapshot_after.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-b")
    );
}

#[tokio::test]
async fn memory_cluster_subscribe_leadership_receives_updates() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let mut subscription = node.subscribe_leadership();

    let initial = subscription.recv().await.unwrap();
    assert_eq!(initial.epoch, 0);

    cluster.elect("node-b").expect("election should succeed");

    let update = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(update.epoch, 1);
}

#[tokio::test]
async fn memory_cluster_subscribe_leadership_initial_snapshot() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node = cluster.node("node-b").unwrap();
    let mut subscription = node.subscribe_leadership();

    let snapshot = subscription.recv().await.unwrap();
    assert_eq!(
        snapshot.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-a")
    );
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_immediate_success() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let result = node.wait_for_leadership(Duration::from_secs(1)).await;
    assert!(result);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_times_out() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-b").unwrap();
    let result = node.wait_for_leadership(Duration::from_millis(50)).await;
    assert!(!result);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_becomes_leader() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));

    let node = cluster.node("node-b").unwrap();
    let node_clone = Arc::clone(&node);

    let handle =
        tokio::spawn(async move { node_clone.wait_for_leadership(Duration::from_secs(5)).await });

    tokio::time::sleep(Duration::from_millis(20)).await;

    cluster.elect("node-b").expect("election should succeed");

    let result = handle.await.unwrap();
    assert!(result);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_change_immediate() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let result = node.wait_for_leadership_change(false).await;
    assert!(result);

    let node_b = cluster.node("node-b").unwrap();
    let result_b = node_b.wait_for_leadership_change(true).await;
    assert!(!result_b);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_change_after_election() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));

    let node_a = cluster.node("node-a").unwrap();
    let node_a_clone = Arc::clone(&node_a);

    let handle = tokio::spawn(async move { node_a_clone.wait_for_leadership_change(true).await });

    tokio::time::sleep(Duration::from_millis(20)).await;

    cluster.elect("node-b").expect("election should succeed");

    let result = handle.await.unwrap();
    assert!(!result);
}

#[tokio::test]
async fn memory_cluster_execute_if_leader_when_leader() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let result = node.execute_if_leader(|| async { 42 }).await;
    assert_eq!(result, Some(42));
}

#[tokio::test]
async fn memory_cluster_execute_if_leader_when_not_leader() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-b").unwrap();
    let result = node.execute_if_leader(|| async { 42 }).await;
    assert_eq!(result, None);
}

#[tokio::test]
async fn memory_cluster_execute_if_leader_after_election() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));

    let node_b = cluster.node("node-b").unwrap();
    let result_before = node_b.execute_if_leader(|| async { 42 }).await;
    assert_eq!(result_before, None);

    cluster.elect("node-b").expect("election should succeed");

    let result_after = node_b.execute_if_leader(|| async { 99 }).await;
    assert_eq!(result_after, Some(99));
}

#[test]
fn memory_cluster_concurrent_elect_safety() {
    use std::sync::Arc;
    use std::thread;

    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    ));

    let mut handles = vec![];
    for i in 0..10 {
        let cluster_clone = Arc::clone(&cluster);
        let handle = thread::spawn(move || {
            let node = if i % 2 == 0 { "node-b" } else { "node-c" };
            cluster_clone.elect(node)
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_some());
    }
}

#[test]
fn memory_cluster_concurrent_node_queries() {
    use std::sync::Arc;
    use std::thread;

    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    ));

    let mut handles = vec![];
    for _ in 0..20 {
        let cluster_clone = Arc::clone(&cluster);
        let handle = thread::spawn(move || {
            let node = cluster_clone.node("node-a");
            node.is_some() && node.unwrap().is_leader()
        });
        handles.push(handle);
    }

    for handle in handles {
        assert!(handle.join().unwrap());
    }
}

#[tokio::test]
async fn memory_cluster_concurrent_elect_and_query() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));

    let cluster_clone = Arc::clone(&cluster);
    let elect_handle = tokio::spawn(async move {
        for _ in 0..100 {
            cluster_clone
                .elect("node-b")
                .expect("election should succeed");
            tokio::time::sleep(Duration::from_micros(100)).await;
            cluster_clone
                .elect("node-a")
                .expect("election should succeed");
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    });

    let cluster_clone = Arc::clone(&cluster);
    let query_handle_a = tokio::spawn(async move {
        for _ in 0..500 {
            let node = cluster_clone.node("node-a").unwrap();
            let _is_leader = node.is_leader();
            let _leader_endpoint = node.leader_endpoint();
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    });

    let cluster_clone = Arc::clone(&cluster);
    let query_handle_b = tokio::spawn(async move {
        for _ in 0..500 {
            let node = cluster_clone.node("node-b").unwrap();
            let _is_leader = node.is_leader();
            let _leader_endpoint = node.leader_endpoint();
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    });

    elect_handle.await.unwrap();
    query_handle_a.await.unwrap();
    query_handle_b.await.unwrap();
}

#[test]
fn memory_cluster_epoch_increments_on_election() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node = cluster.node("node-a").unwrap();

    let snapshot_0 = node.leadership_snapshot();
    assert_eq!(snapshot_0.epoch, 0);

    cluster.elect("node-b").expect("election should succeed");
    let snapshot_1 = node.leadership_snapshot();
    assert_eq!(snapshot_1.epoch, 1);

    cluster.elect("node-c").expect("election should succeed");
    let snapshot_2 = node.leadership_snapshot();
    assert_eq!(snapshot_2.epoch, 2);

    cluster.elect("node-a").expect("election should succeed");
    let snapshot_3 = node.leadership_snapshot();
    assert_eq!(snapshot_3.epoch, 3);
}

#[test]
fn memory_cluster_epoch_stays_same_on_same_leader() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let snapshot_0 = node.leadership_snapshot();
    assert_eq!(snapshot_0.epoch, 0);

    cluster.elect("node-a").expect("election should succeed");
    let snapshot_1 = node.leadership_snapshot();
    assert_eq!(snapshot_1.epoch, 0);

    cluster.elect("node-a").expect("election should succeed");
    let snapshot_2 = node.leadership_snapshot();
    assert_eq!(snapshot_2.epoch, 0);
}

#[tokio::test]
async fn memory_cluster_multiple_subscribers() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();
    let node_c = cluster.node("node-c").unwrap();

    let mut sub_a = node_a.subscribe_leadership();
    let mut sub_b = node_b.subscribe_leadership();
    let mut sub_c = node_c.subscribe_leadership();

    let initial_a = sub_a.recv().await.unwrap();
    let initial_b = sub_b.recv().await.unwrap();
    let initial_c = sub_c.recv().await.unwrap();

    assert_eq!(
        initial_a.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-a")
    );
    assert_eq!(
        initial_b.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-a")
    );
    assert_eq!(
        initial_c.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-a")
    );

    cluster.elect("node-b").expect("election should succeed");

    let update_a = tokio::time::timeout(Duration::from_secs(1), sub_a.recv())
        .await
        .unwrap()
        .unwrap();
    let update_b = tokio::time::timeout(Duration::from_secs(1), sub_b.recv())
        .await
        .unwrap()
        .unwrap();
    let update_c = tokio::time::timeout(Duration::from_secs(1), sub_c.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(update_a.epoch, 1);
    assert_eq!(update_b.epoch, 1);
    assert_eq!(update_c.epoch, 1);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_multiple_nodes() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    ));

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();
    let node_c = cluster.node("node-c").unwrap();

    let handle_a = tokio::spawn({
        let node = Arc::clone(&node_a);
        async move { node.wait_for_leadership(Duration::from_secs(2)).await }
    });

    let handle_b = tokio::spawn({
        let node = Arc::clone(&node_b);
        async move { node.wait_for_leadership(Duration::from_secs(2)).await }
    });

    let handle_c = tokio::spawn({
        let node = Arc::clone(&node_c);
        async move { node.wait_for_leadership(Duration::from_secs(2)).await }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    cluster.elect("node-b").expect("election should succeed");

    let result_a = handle_a.await.unwrap();
    let result_b = handle_b.await.unwrap();
    let result_c = handle_c.await.unwrap();

    assert!(!result_a);
    assert!(result_b);
    assert!(!result_c);
}

#[test]
fn memory_cluster_empty_topology_edge_case() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a"]);

    let node = cluster.node("node-a").unwrap();
    let endpoints = node.member_endpoints();

    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].as_ref(), "http://node-a");
}

#[test]
fn memory_cluster_large_topology() {
    let endpoints: Vec<String> = (0..100).map(|i| format!("http://node-{}", i)).collect();
    let cluster = MemoryCluster::new("node-50", endpoints.clone());

    let node = cluster.node("node-50").unwrap();
    assert!(node.is_leader());

    let endpoints_result = node.member_endpoints();
    assert_eq!(endpoints_result.len(), 100);

    cluster.elect("node-0").expect("election should succeed");

    let node_0 = cluster.node("node-0").unwrap();
    assert!(node_0.is_leader());

    let node_99 = cluster.node("node-99").unwrap();
    assert!(!node_99.is_leader());
    assert_eq!(node_99.leader_endpoint(), Some(Arc::from("http://node-0")));
}

#[test]
fn memory_cluster_node_id_extraction() {
    let cluster = MemoryCluster::new(
        "alpha",
        [
            "http://host1:8080/path/alpha",
            "http://host2:8080/path/beta",
        ],
    );

    let node_alpha = cluster.node("alpha").unwrap();
    let node_beta = cluster.node("beta").unwrap();

    assert!(node_alpha.is_leader());
    assert!(!node_beta.is_leader());

    assert_eq!(node_alpha.node_id(), "alpha");
    assert_eq!(node_beta.node_id(), "beta");
}

#[test]
fn memory_cluster_endpoint_node_id_extraction() {
    let cluster = MemoryCluster::new("node-x", ["http://example.com/api/node-x"]);

    let node = cluster.node("node-x").unwrap();
    assert!(node.is_leader());

    cluster.elect("node-x").expect("election should succeed");

    let snapshot = node.leadership_snapshot();
    assert_eq!(
        snapshot.leader_endpoint.as_ref().map(|s| s.as_ref()),
        Some("http://example.com/api/node-x")
    );
}

#[tokio::test]
async fn memory_cluster_execute_if_leader_with_async_work() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let result = node
        .execute_if_leader(|| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "work completed"
        })
        .await;
    assert_eq!(result, Some("work completed"));

    cluster.elect("node-b").expect("election should succeed");

    let result = node
        .execute_if_leader(|| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            "should not run"
        })
        .await;
    assert_eq!(result, None);
}

#[test]
fn memory_cluster_is_leader_consistency_across_nodes() {
    let cluster = MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b", "http://node-c"],
    );

    let nodes: Vec<_> = vec!["node-a", "node-b", "node-c"]
        .iter()
        .map(|id| cluster.node(id).unwrap())
        .collect();

    let leader_count = nodes.iter().filter(|n| n.is_leader()).count();
    assert_eq!(leader_count, 1);

    cluster.elect("node-b").expect("election should succeed");

    let leader_count = nodes.iter().filter(|n| n.is_leader()).count();
    assert_eq!(leader_count, 1);
}

#[tokio::test]
async fn memory_cluster_wait_for_leadership_zero_duration() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let result = node.wait_for_leadership(Duration::ZERO).await;
    assert!(result);

    let node_b = cluster.node("node-b").unwrap();
    let result_b = node_b.wait_for_leadership(Duration::ZERO).await;
    assert!(!result_b);
}

#[test]
fn memory_cluster_elect_updates_all_node_views() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert!(node_a.is_leader());
    assert!(!node_b.is_leader());
    assert_eq!(node_a.leader_endpoint(), node_b.leader_endpoint());

    cluster.elect("node-b").expect("election should succeed");

    assert!(!node_a.is_leader());
    assert!(node_b.is_leader());
    assert_eq!(node_a.leader_endpoint(), node_b.leader_endpoint());
}

#[tokio::test]
async fn memory_cluster_concurrent_subscriptions_and_elections() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));

    let node = cluster.node("node-a").unwrap();
    let mut subscription = node.subscribe_leadership();

    let cluster_clone = Arc::clone(&cluster);
    let elect_handle = tokio::spawn(async move {
        for i in 0..20 {
            let target = if i % 2 == 0 { "node-b" } else { "node-a" };
            cluster_clone
                .elect(target)
                .expect("election should succeed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let mut updates_received = 0;
    for _ in 0..20 {
        if tokio::time::timeout(Duration::from_millis(10), subscription.recv())
            .await
            .is_ok()
        {
            updates_received += 1;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    elect_handle.await.unwrap();

    assert!(
        updates_received >= 1,
        "Should receive at least one update, got {}",
        updates_received
    );
}

#[tokio::test]
async fn memory_cluster_subscription_notifies_on_election() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let mut subscription = node.subscribe_leadership();

    cluster.elect("node-b").expect("election should succeed");

    let update = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(update.epoch, 1);
    assert_eq!(
        update.leader_node_id.as_ref().map(|s| s.as_ref()),
        Some("node-b")
    );
}

#[tokio::test]
async fn memory_cluster_no_notify_on_same_leader() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);

    let node = cluster.node("node-a").unwrap();
    let mut subscription = node.subscribe_leadership();

    let _initial = subscription.recv().await.unwrap();

    cluster.elect("node-a").expect("election should succeed");

    let update = tokio::time::timeout(Duration::from_millis(50), subscription.recv()).await;
    assert!(update.is_err(), "Should not receive update for same leader");
}
