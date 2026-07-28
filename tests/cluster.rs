//! Cluster coordinator and leadership behavior tests.

use std::{sync::Arc, time::Duration};

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, ClusterCoordinatorExt, ClusterForwarder, ForwardToLeaderBehavior,
    LeaderOnlyBehavior, MemoryCluster, SingletonTaskRunner,
};
use catga_core::{CatgaResult, ErrorCode, Handler, Mediator, Pipeline, Registry, Request};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn leadership_subscription_delivers_transitions_to_independent_receivers() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").expect("configured node-a");
    let node_b = cluster.node("node-b").expect("configured node-b");
    let mut first = node_a.subscribe_leadership();
    let mut second = node_b.subscribe_leadership();

    assert_eq!(first.snapshot().epoch, 0);
    cluster.elect("node-b").expect("configured node-b");
    cluster.elect("node-a").expect("configured node-a");

    let first_snapshot = first.recv().await.expect("cluster remains alive");
    let second_snapshot = second.recv().await.expect("cluster remains alive");
    assert_eq!(first_snapshot.epoch, 1);
    assert_eq!(first_snapshot.leader_node_id.as_deref(), Some("node-b"));
    assert_eq!(
        second_snapshot.leader_endpoint.as_deref(),
        Some("http://node-b")
    );
}

#[tokio::test]
async fn leadership_subscription_snapshot_cannot_block_an_election() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));
    let node = cluster.node("node-a").expect("configured node-a");
    let subscription = node.subscribe_leadership();
    let retained_snapshot = subscription.snapshot();
    let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
    let election_cluster = Arc::clone(&cluster);

    std::thread::spawn(move || {
        let result = election_cluster.elect("node-b");
        let _ = completed_tx.send(result);
    });

    assert_eq!(
        completed_rx
            .await
            .expect("election worker remains available"),
        Some(())
    );
    assert_eq!(retained_snapshot.leader_node_id.as_deref(), Some("node-a"));
}

#[tokio::test]
async fn leadership_subscription_closes_after_its_coordinator_drops() {
    let mut subscription = {
        let cluster = MemoryCluster::new("node-a", ["http://node-a"]);
        let node = cluster.node("node-a").expect("configured node-a");
        node.subscribe_leadership()
    };

    let result = tokio::time::timeout(Duration::from_millis(50), subscription.recv()).await;
    assert!(matches!(
        result,
        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
    ));
}

#[tokio::test]
async fn leadership_snapshot_matches_each_serialized_election_state() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").expect("configured node-a");
    let node_b = cluster.node("node-b").expect("configured node-b");

    for (leader, endpoint) in [("node-b", "http://node-b"), ("node-a", "http://node-a")] {
        cluster.elect(leader).expect("configured leader");
        let snapshot = node_a.leadership_snapshot();

        assert_eq!(snapshot.leader_node_id.as_deref(), Some(leader));
        assert_eq!(snapshot.leader_endpoint.as_deref(), Some(endpoint));
        assert_eq!(node_a.is_leader(), leader == "node-a");
        assert_eq!(node_b.is_leader(), leader == "node-b");
    }
}

#[tokio::test]
async fn lagged_leadership_subscription_resumes_only_with_newer_transitions() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node = cluster.node("node-a").expect("configured node-a");
    let mut subscription = node.subscribe_leadership();

    for transition in 0..65 {
        let leader = if transition % 2 == 0 {
            "node-b"
        } else {
            "node-a"
        };
        cluster.elect(leader).expect("configured leader");
    }

    let current = subscription
        .recv()
        .await
        .expect("lagged subscription resynchronizes to the latest snapshot");
    assert_eq!(current.epoch, 65);

    cluster.elect("node-a").expect("configured leader");
    let next = subscription
        .recv()
        .await
        .expect("subscription resumes after lag");
    assert_eq!(next.epoch, 66);
}

#[tokio::test]
async fn cluster_publishes_leadership_changes_without_polling() {
    let cluster = Arc::new(MemoryCluster::new(
        "node-a",
        ["http://node-a", "http://node-b"],
    ));
    let node_a = cluster.node("node-a").unwrap();
    let node_b = cluster.node("node-b").unwrap();

    assert!(node_a.is_leader());
    assert_eq!(node_a.leader_endpoint().as_deref(), Some("http://node-a"));
    let health = catga_cluster::cluster_health(node_a.as_ref());
    assert!(health.has_leader());
    assert!(health.is_leader());
    assert_eq!(health.cluster_size(), 2);
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

#[tokio::test]
async fn leadership_loss_cancels_active_action() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").unwrap();
    let action_started = Arc::new(Notify::new());
    let cancellations = Arc::new(AtomicUsize::new(0));
    let cancellation_observed = Arc::new(Notify::new());

    let action = tokio::spawn({
        let action_started = Arc::clone(&action_started);
        let cancellations = Arc::clone(&cancellations);
        let cancellation_observed = Arc::clone(&cancellation_observed);
        async move {
            node_a
                .execute_if_leader_cancellable(move |leadership_lost| {
                    let action_started = Arc::clone(&action_started);
                    let cancellations = Arc::clone(&cancellations);
                    let cancellation_observed = Arc::clone(&cancellation_observed);
                    async move {
                        action_started.notify_one();
                        leadership_lost.cancelled().await;
                        cancellations.fetch_add(1, Ordering::SeqCst);
                        cancellation_observed.notify_one();
                        Ok(())
                    }
                })
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(1), action_started.notified())
        .await
        .unwrap();
    cluster.elect("node-b").unwrap();

    assert_eq!(
        action.await.unwrap().unwrap_err().code(),
        ErrorCode::Cancelled
    );
    tokio::time::timeout(Duration::from_secs(1), cancellation_observed.notified())
        .await
        .unwrap();
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn leadership_change_waiter_observes_a_loss_before_regaining_leadership() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_a = cluster.node("node-a").unwrap();
    let waiter = node_a.wait_for_leadership_change(true);
    tokio::pin!(waiter);

    assert!(futures::poll!(&mut waiter).is_pending());
    cluster.elect("node-b").unwrap();
    cluster.elect("node-a").unwrap();

    assert!(futures::poll!(&mut waiter).is_ready());
}

#[tokio::test]
async fn nonleader_cancellable_execution_returns_unavailable_without_calling_action() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let node_b = cluster.node("node-b").unwrap();
    let action_called = Arc::new(AtomicBool::new(false));

    let result = node_b
        .execute_if_leader_cancellable({
            let action_called = Arc::clone(&action_called);
            move |_| {
                action_called.store(true, Ordering::SeqCst);
                async { Ok(()) }
            }
        })
        .await;

    assert_eq!(result.unwrap_err().code(), ErrorCode::Unavailable);
    assert!(!action_called.load(Ordering::SeqCst));
}

#[derive(Debug)]
struct LeaderWork;

impl catga_core::Message for LeaderWork {}

impl Request for LeaderWork {
    type Response = u32;
}

struct LeaderHandler(Arc<AtomicUsize>);

struct TestForwarder(Arc<AtomicUsize>);

#[async_trait]
impl ClusterForwarder<LeaderWork> for TestForwarder {
    async fn forward(&self, _: LeaderWork, leader_endpoint: &str) -> CatgaResult<u32> {
        assert_eq!(leader_endpoint, "http://node-a");
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(99)
    }
}

#[async_trait]
impl Handler<LeaderWork> for LeaderHandler {
    async fn handle(&self, _: LeaderWork) -> CatgaResult<u32> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(42)
    }
}

#[tokio::test]
async fn leader_only_behavior_rejects_non_leader_requests_before_the_handler_runs() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let executions = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<LeaderWork, _>(LeaderHandler(Arc::clone(&executions)))
        .unwrap();
    let mediator = Mediator::new(registry);

    let non_leader = Pipeline::new().with(LeaderOnlyBehavior::new(cluster.node("node-b").unwrap()));
    assert_eq!(
        mediator
            .send_with(LeaderWork, &non_leader)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(executions.load(Ordering::Relaxed), 0);

    let leader = Pipeline::new().with(LeaderOnlyBehavior::new(cluster.node("node-a").unwrap()));
    assert_eq!(mediator.send_with(LeaderWork, &leader).await.unwrap(), 42);
    assert_eq!(executions.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn forward_to_leader_behavior_uses_the_known_leader_without_running_the_local_handler() {
    let cluster = MemoryCluster::new("node-a", ["http://node-a", "http://node-b"]);
    let executions = Arc::new(AtomicUsize::new(0));
    let forwards = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<LeaderWork, _>(LeaderHandler(Arc::clone(&executions)))
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(ForwardToLeaderBehavior::new(
        cluster.node("node-b").unwrap(),
        Arc::new(TestForwarder(Arc::clone(&forwards))),
    ));

    assert_eq!(mediator.send_with(LeaderWork, &pipeline).await.unwrap(), 99);
    assert_eq!(forwards.load(Ordering::Relaxed), 1);
    assert_eq!(executions.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn singleton_task_runner_cancels_on_leadership_loss_and_restarts_when_elected_again() {
    let cluster = Arc::new(MemoryCluster::new("node-a", ["node-a", "node-b"]));
    let coordinator = cluster.node("node-a").unwrap();
    let runner = SingletonTaskRunner::new(coordinator);
    let shutdown = CancellationToken::new();
    let starts = Arc::new(AtomicUsize::new(0));
    let cancellations = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());

    let task = tokio::spawn({
        let starts = Arc::clone(&starts);
        let cancellations = Arc::clone(&cancellations);
        let started = Arc::clone(&started);
        let stopped = Arc::clone(&stopped);
        let shutdown = shutdown.clone();
        async move {
            runner
                .run(shutdown, move |leadership_lost| {
                    let starts = Arc::clone(&starts);
                    let cancellations = Arc::clone(&cancellations);
                    let started = Arc::clone(&started);
                    let stopped = Arc::clone(&stopped);
                    async move {
                        starts.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        leadership_lost.cancelled().await;
                        cancellations.fetch_add(1, Ordering::SeqCst);
                        stopped.notify_one();
                    }
                })
                .await;
        }
    });

    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    cluster.elect("node-b").unwrap();
    tokio::time::timeout(Duration::from_secs(1), stopped.notified())
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(cancellations.load(Ordering::SeqCst), 1);

    cluster.elect("node-a").unwrap();
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 2);

    shutdown.cancel();
    task.await.unwrap();
    assert_eq!(cancellations.load(Ordering::SeqCst), 2);
}
