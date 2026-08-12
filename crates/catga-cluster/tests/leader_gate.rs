//! Contract tests for leader-gated request handling over a `MemoryCluster`:
//! the leader-only pipeline behavior, the cancellable leader gate, and the
//! immutable cluster health snapshot.

#[path = "common/bump.rs"]
mod bump;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bump::{Bump, bump_mediator};
use catga_cluster::{ClusterCoordinatorExt, LeaderOnlyBehavior, MemoryCluster, cluster_health};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Pipeline};
use tokio_util::sync::CancellationToken;

fn two_node_cluster() -> MemoryCluster {
    MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"])
}

#[tokio::test]
async fn leader_only_behavior_runs_the_pipeline_on_the_leader() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline = Pipeline::new().with(LeaderOnlyBehavior::new(leader));

    assert_eq!(mediator.send_with(Bump(41), &pipeline).await?, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn leader_only_behavior_rejects_a_follower_with_the_leader_endpoint() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline = Pipeline::new().with(LeaderOnlyBehavior::new(follower));

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Conflict
                && error.to_string().contains("http://cluster/one")
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the handler must not run on a follower"
    );
    Ok(())
}

#[tokio::test]
async fn leader_only_behavior_reports_an_unknown_leader() -> CatgaResult<()> {
    // A leader outside the member list leaves the endpoint unknown.
    let cluster = MemoryCluster::new("ghost", ["http://cluster/one"]);
    let follower = cluster.node("one").expect("configured member");
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline = Pipeline::new().with(LeaderOnlyBehavior::new(follower));

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Conflict && error.to_string().contains("unknown")
    ));
    Ok(())
}

#[tokio::test]
async fn execute_if_leader_runs_only_on_the_leader() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    let follower = cluster.node("two").expect("configured member");

    let ran = ClusterCoordinatorExt::execute_if_leader(leader.as_ref(), || async { 7_u8 }).await;
    assert_eq!(ran, Some(7));

    let skipped =
        ClusterCoordinatorExt::execute_if_leader(follower.as_ref(), || async { 9_u8 }).await;
    assert_eq!(skipped, None);
}

#[tokio::test]
async fn execute_if_leader_cancellable_rejects_a_follower_without_running() {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");

    let result = follower
        .execute_if_leader_cancellable(|_token| async { Ok::<u8, CatgaError>(1) })
        .await;
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::Unavailable
    ));
}

#[tokio::test]
async fn execute_if_leader_cancellable_cancels_work_when_leadership_is_lost() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");

    let started = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let action = {
        let started = Arc::clone(&started);
        let cancelled = Arc::clone(&cancelled);
        move |token: CancellationToken| async move {
            started.notify_one();
            token.cancelled().await;
            cancelled.notify_one();
            Ok::<u8, CatgaError>(0)
        }
    };

    let running = tokio::spawn({
        let leader = Arc::clone(&leader);
        async move { leader.execute_if_leader_cancellable(action).await }
    });
    started.notified().await;
    cluster.elect("two").expect("two is a member");

    let result = running.await.expect("action task must join");
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::Cancelled
    ));
    cancelled.notified().await;
}

#[tokio::test]
async fn execute_if_leader_cancellable_returns_the_action_result_on_the_leader() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");

    let result = leader
        .execute_if_leader_cancellable(|_token| async { Ok::<u8, CatgaError>(5) })
        .await;
    assert_eq!(result.expect("leader action must succeed"), 5);
}

#[test]
fn cluster_health_reports_the_leader_view() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");

    let health = cluster_health(leader.as_ref());
    assert_eq!(health, health.clone());
    assert!(health.has_leader());
    assert!(health.is_leader());
    assert_eq!(health.leader_endpoint(), Some("http://cluster/one"));
    assert_eq!(health.cluster_size(), 2);
    assert_eq!(health.node_id(), "one");
}

#[test]
fn cluster_health_reports_the_follower_view() {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");

    let health = cluster_health(follower.as_ref());
    assert!(health.has_leader());
    assert!(!health.is_leader());
    assert_eq!(health.leader_endpoint(), Some("http://cluster/one"));
}

#[test]
fn cluster_health_reports_a_leaderless_view() {
    let cluster = MemoryCluster::new("ghost", ["http://cluster/one"]);
    let node = cluster.node("one").expect("configured member");

    let health = cluster_health(node.as_ref());
    assert!(!health.has_leader());
    assert!(!health.is_leader());
    assert_eq!(health.leader_endpoint(), None);
    assert_eq!(health.cluster_size(), 1);
}
