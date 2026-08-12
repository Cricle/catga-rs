//! Contract tests for `SingletonTaskRunner`: leadership-scoped background work
//! starts on election, is cancelled on loss, restarts after completion, and
//! stops promptly on shutdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use catga_cluster::{MemoryCluster, SingletonTaskRunner};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn two_node_cluster() -> MemoryCluster {
    MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"])
}

/// Drives the runner task to completion, failing the test if it outlives the deadline.
async fn join_runner(handle: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the runner must stop within the deadline")
        .expect("the runner task must not panic");
}

#[tokio::test]
async fn work_starts_on_election_and_is_cancelled_when_leadership_moves() {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let runner = Arc::new(SingletonTaskRunner::new(follower));
    let shutdown = CancellationToken::new();

    let started = Arc::new(Notify::new());
    let epoch_cancelled = Arc::new(Notify::new());
    let task = {
        let started = Arc::clone(&started);
        let epoch_cancelled = Arc::clone(&epoch_cancelled);
        move |token: CancellationToken| {
            let started = Arc::clone(&started);
            let epoch_cancelled = Arc::clone(&epoch_cancelled);
            async move {
                started.notify_one();
                token.cancelled().await;
                epoch_cancelled.notify_one();
            }
        }
    };

    let handle = tokio::spawn({
        let runner = Arc::clone(&runner);
        let shutdown = shutdown.clone();
        async move { runner.run(shutdown, task).await }
    });

    // A follower never starts the work.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), started.notified())
            .await
            .is_err(),
        "work must not start while this node follows"
    );

    cluster.elect("two").expect("two is a member");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("work must start after the election");

    cluster.elect("one").expect("one is a member");
    tokio::time::timeout(Duration::from_secs(2), epoch_cancelled.notified())
        .await
        .expect("losing leadership must cancel the running epoch");

    shutdown.cancel();
    join_runner(handle).await;
}

#[tokio::test]
async fn completed_work_restarts_after_the_restart_delay() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    let runner = Arc::new(SingletonTaskRunner::with_restart_delay(
        leader,
        Duration::from_millis(5),
    ));
    let shutdown = CancellationToken::new();

    let invocations = Arc::new(AtomicUsize::new(0));
    let task = {
        let invocations = Arc::clone(&invocations);
        move |_token: CancellationToken| {
            let invocations = Arc::clone(&invocations);
            async move {
                invocations.fetch_add(1, Ordering::SeqCst);
            }
        }
    };

    let handle = tokio::spawn({
        let runner = Arc::clone(&runner);
        let shutdown = shutdown.clone();
        async move { runner.run(shutdown, task).await }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while invocations.load(Ordering::SeqCst) < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "completed work must restart while this node leads"
        );
        tokio::task::yield_now().await;
    }

    shutdown.cancel();
    join_runner(handle).await;
}

#[tokio::test]
async fn a_leadership_change_during_the_restart_wait_aborts_the_restart() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    // Long enough that the sleep never fires inside the test.
    let runner = Arc::new(SingletonTaskRunner::with_restart_delay(
        leader,
        Duration::from_secs(30),
    ));
    let shutdown = CancellationToken::new();

    let invocations = Arc::new(AtomicUsize::new(0));
    let task = {
        let invocations = Arc::clone(&invocations);
        move |_token: CancellationToken| {
            let invocations = Arc::clone(&invocations);
            async move {
                invocations.fetch_add(1, Ordering::SeqCst);
            }
        }
    };

    let handle = tokio::spawn({
        let runner = Arc::clone(&runner);
        let shutdown = shutdown.clone();
        async move { runner.run(shutdown, task).await }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while invocations.load(Ordering::SeqCst) < 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the leader runs the work once"
        );
        tokio::task::yield_now().await;
    }

    // Let the runner park inside the post-completion restart select first.
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Losing leadership while the restart sleeps abandons that restart.
    cluster.elect("two").expect("two is a member");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "the restart must not fire while this node follows"
    );

    cluster.elect("one").expect("one is a member");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while invocations.load(Ordering::SeqCst) < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "regaining leadership starts the work again"
        );
        tokio::task::yield_now().await;
    }

    shutdown.cancel();
    join_runner(handle).await;
}

#[tokio::test]
async fn shutdown_cancels_in_flight_work_and_stops_the_runner() {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    let runner = Arc::new(SingletonTaskRunner::new(leader));
    let shutdown = CancellationToken::new();

    let started = Arc::new(Notify::new());
    let epoch_cancelled = Arc::new(Notify::new());
    let task = {
        let started = Arc::clone(&started);
        let epoch_cancelled = Arc::clone(&epoch_cancelled);
        move |token: CancellationToken| {
            let started = Arc::clone(&started);
            let epoch_cancelled = Arc::clone(&epoch_cancelled);
            async move {
                started.notify_one();
                token.cancelled().await;
                epoch_cancelled.notify_one();
            }
        }
    };

    let handle = tokio::spawn({
        let runner = Arc::clone(&runner);
        let shutdown = shutdown.clone();
        async move { runner.run(shutdown, task).await }
    });

    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("work must start on the leader");
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(2), epoch_cancelled.notified())
        .await
        .expect("shutdown must cancel the running epoch");
    join_runner(handle).await;
}

#[tokio::test]
async fn shutdown_while_waiting_for_leadership_stops_the_runner() {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let runner = Arc::new(SingletonTaskRunner::new(follower));
    let shutdown = CancellationToken::new();

    let invocations = Arc::new(AtomicUsize::new(0));
    let task = {
        let invocations = Arc::clone(&invocations);
        move |_token: CancellationToken| {
            let invocations = Arc::clone(&invocations);
            async move {
                invocations.fetch_add(1, Ordering::SeqCst);
            }
        }
    };

    let handle = tokio::spawn({
        let runner = Arc::clone(&runner);
        let shutdown = shutdown.clone();
        async move { runner.run(shutdown, task).await }
    });

    tokio::task::yield_now().await;
    shutdown.cancel();
    join_runner(handle).await;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        0,
        "a follower that never leads never runs the work"
    );
}
