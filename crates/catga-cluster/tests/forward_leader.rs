//! Contract tests for `ForwardToLeaderBehavior`: local execution on the leader,
//! plain forwarding on a follower, and bounded retry classification while
//! leadership settles after a failover.

#[path = "common/bump.rs"]
mod bump;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bump::{Bump, bump_mediator};
use catga_cluster::{ClusterForwarder, ForwardToLeaderBehavior, MemoryCluster};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Pipeline};

/// A forwarder that replays a scripted outcome sequence and records every call.
struct ScriptedForwarder {
    outcomes: Mutex<VecDeque<CatgaResult<u64>>>,
    calls: AtomicUsize,
    endpoints: Mutex<Vec<String>>,
}

impl ScriptedForwarder {
    fn with_outcomes(outcomes: impl IntoIterator<Item = CatgaResult<u64>>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            calls: AtomicUsize::new(0),
            endpoints: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn endpoints(&self) -> Vec<String> {
        self.endpoints
            .lock()
            .expect("endpoint log mutex poisoned")
            .clone()
    }
}

#[async_trait]
impl ClusterForwarder<Bump> for ScriptedForwarder {
    async fn forward(&self, request: Bump, leader_endpoint: &str) -> CatgaResult<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.endpoints
            .lock()
            .expect("endpoint log mutex poisoned")
            .push(leader_endpoint.to_owned());
        self.outcomes
            .lock()
            .expect("outcome script mutex poisoned")
            .pop_front()
            .unwrap_or_else(|| Ok(request.0 * 10))
    }
}

fn two_node_cluster() -> MemoryCluster {
    MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"])
}

fn leaderless_cluster() -> MemoryCluster {
    // A leader outside the member list leaves the endpoint unknown.
    MemoryCluster::new("ghost", ["http://cluster/one"])
}

#[tokio::test]
async fn the_leader_executes_locally_without_forwarding() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let leader = cluster.node("one").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([]));
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(leader, forwarder.clone()).with_retry(3, Duration::ZERO),
    );

    assert_eq!(mediator.send_with(Bump(4), &pipeline).await?, 5);
    assert_eq!(forwarder.calls(), 0, "the leader must not forward");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn a_follower_forwards_to_the_leader_endpoint_once_without_retry() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([Ok(99)]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(ForwardToLeaderBehavior::new(follower, forwarder.clone()));

    assert_eq!(mediator.send_with(Bump(4), &pipeline).await?, 99);
    assert_eq!(forwarder.calls(), 1);
    assert_eq!(forwarder.endpoints(), ["http://cluster/one"]);
    Ok(())
}

#[tokio::test]
async fn a_follower_without_retry_fails_fast_when_no_leader_is_known() -> CatgaResult<()> {
    let cluster = leaderless_cluster();
    let follower = cluster.node("one").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(ForwardToLeaderBehavior::new(follower, forwarder.clone()));

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Conflict
                && error.to_string().contains("no cluster leader")
    ));
    assert_eq!(forwarder.calls(), 0, "no forward attempt without a leader");
    Ok(())
}

#[tokio::test]
async fn retry_recovers_from_transient_forward_failures() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([
        Err(CatgaError::new(ErrorCode::Transient, "leader not ready")),
        Err(CatgaError::new(ErrorCode::Conflict, "leadership moved")),
        Ok(77),
    ]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(follower, forwarder.clone())
            .with_retry(3, Duration::from_millis(1)),
    );

    assert_eq!(mediator.send_with(Bump(4), &pipeline).await?, 77);
    assert_eq!(forwarder.calls(), 3);
    assert_eq!(
        forwarder.endpoints(),
        [
            "http://cluster/one",
            "http://cluster/one",
            "http://cluster/one"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn retry_returns_non_retryable_errors_immediately() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([Err(CatgaError::new(
        ErrorCode::Validation,
        "malformed request",
    ))]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(follower, forwarder.clone())
            .with_retry(5, Duration::from_millis(1)),
    );

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(forwarder.calls(), 1, "validation errors must not retry");
    Ok(())
}

#[tokio::test]
async fn retry_stops_after_the_configured_attempt_budget() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([
        Err(CatgaError::new(ErrorCode::Transient, "first")),
        Err(CatgaError::new(ErrorCode::Transient, "second")),
        Ok(1),
    ]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(follower, forwarder.clone())
            .with_retry(2, Duration::from_millis(1)),
    );

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Transient && error.to_string().contains("second")
    ));
    assert_eq!(forwarder.calls(), 2, "the budget caps the attempts");
    Ok(())
}

#[tokio::test]
async fn retry_treats_an_unknown_leader_as_a_retryable_conflict() -> CatgaResult<()> {
    let cluster = leaderless_cluster();
    let follower = cluster.node("one").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(follower, forwarder.clone())
            .with_retry(2, Duration::from_millis(1)),
    );

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Conflict
                && error.to_string().contains("no cluster leader")
    ));
    assert_eq!(
        forwarder.calls(),
        0,
        "an unknown leader never reaches the transport"
    );
    Ok(())
}

#[tokio::test]
async fn a_zero_retry_budget_means_a_single_attempt() -> CatgaResult<()> {
    let cluster = two_node_cluster();
    let follower = cluster.node("two").expect("configured member");
    let forwarder = Arc::new(ScriptedForwarder::with_outcomes([Err(CatgaError::new(
        ErrorCode::Transient,
        "still settling",
    ))]));
    let mediator = bump_mediator(&Arc::new(AtomicUsize::new(0)));
    let pipeline = Pipeline::new().with(
        ForwardToLeaderBehavior::new(follower, forwarder.clone())
            .with_retry(0, Duration::from_millis(1)),
    );

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::Transient
    ));
    assert_eq!(forwarder.calls(), 1, "zero is treated as one attempt");
    Ok(())
}
