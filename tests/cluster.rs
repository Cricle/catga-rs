use std::{sync::Arc, time::Duration};

use catga_cluster::{ClusterCoordinator, MemoryCluster};

#[tokio::test]
async fn cluster_publishes_leadership_changes_without_polling_or_global_locks() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));
    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert!(node_a.is_leader());
    assert_eq!(node_a.leader_endpoint().as_deref(), Some("http://node-a"));
    assert!(!node_b.is_leader());

    let waiter = {
        let node_b = Arc::clone(&node_b);
        tokio::spawn(async move { node_b.wait_for_leadership(Duration::from_secs(1)).await })
    };
    tokio::task::yield_now().await;
    cluster.elect("node-b").unwrap();

    assert!(waiter.await.unwrap());
    assert_eq!(node_b.leader_endpoint().as_deref(), Some("http://node-b"));
    assert_eq!(
        node_b
            .member_endpoints()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["http://node-a", "http://node-b"]
    );
}

#[tokio::test]
async fn cluster_executes_actions_only_while_the_caller_is_leader() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert_eq!(node_a.execute_if_leader(|| async { 7_u32 }).await, Some(7));
    assert_eq!(node_b.execute_if_leader(|| async { 9_u32 }).await, None);
}
