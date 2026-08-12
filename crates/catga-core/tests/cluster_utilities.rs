//! Contract tests for the backend-agnostic cluster utilities: the leader-only
//! pipeline behavior and the cluster-health snapshot, both generic over
//! `ConsensusCoordinator`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use catga_core::{
    CatgaResult, ConsensusCoordinator, ErrorCode, LeaderOnlyBehavior, LeaderOnlyCommand, Mediator,
    Message, MessageTypeId, Pipeline, Registry, Request, cluster_health, request_handler,
};

struct BumpTypeId;

impl MessageTypeId for BumpTypeId {
    const NAME: &'static str = "Bump";
}

#[derive(Clone)]
struct Bump(u64);

impl Message for Bump {}

impl Request for Bump {
    type Response = u64;
    type TypeId = BumpTypeId;
}

impl LeaderOnlyCommand for Bump {}

/// A static coordinator view; every field is fixed at construction.
struct StubCoordinator {
    node_id: String,
    is_leader: bool,
    leader_endpoint: Option<Arc<str>>,
    members: Vec<Arc<str>>,
}

impl StubCoordinator {
    fn leader() -> Self {
        Self {
            node_id: "one".to_owned(),
            is_leader: true,
            leader_endpoint: Some("http://cluster/one".into()),
            members: vec!["http://cluster/one".into(), "http://cluster/two".into()],
        }
    }

    fn follower() -> Self {
        Self {
            is_leader: false,
            ..Self::leader()
        }
    }

    fn leaderless() -> Self {
        Self {
            is_leader: false,
            leader_endpoint: None,
            ..Self::leader()
        }
    }
}

impl ConsensusCoordinator for StubCoordinator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        self.is_leader
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        self.leader_endpoint.as_ref().map(Arc::clone)
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        self.members.clone().into()
    }
}

fn bump_mediator(calls: &Arc<AtomicUsize>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<Bump, _>(request_handler({
            let calls = Arc::clone(calls);
            move |bump: Bump| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(bump.0 + 1)
                }
            }
        }))
        .expect("bump handler must register");
    Mediator::new(registry)
}

#[tokio::test]
async fn leader_only_behavior_runs_the_pipeline_on_the_leader() -> CatgaResult<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline =
        Pipeline::new().with(LeaderOnlyBehavior::new(Arc::new(StubCoordinator::leader())));

    assert_eq!(mediator.send_with(Bump(41), &pipeline).await?, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn leader_only_behavior_rejects_a_follower_with_the_leader_endpoint() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    // A trait-object coordinator exercises the `?Sized` path applications use.
    let coordinator: Arc<dyn ConsensusCoordinator> = Arc::new(StubCoordinator::follower());
    let pipeline = Pipeline::new().with(LeaderOnlyBehavior::new(coordinator));

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
}

#[tokio::test]
async fn leader_only_behavior_reports_an_unknown_leader() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = bump_mediator(&calls);
    let pipeline = Pipeline::new().with(LeaderOnlyBehavior::new(Arc::new(
        StubCoordinator::leaderless(),
    )));

    let result = mediator.send_with(Bump(1), &pipeline).await;
    assert!(matches!(
        result,
        Err(ref error)
            if error.code() == ErrorCode::Conflict && error.to_string().contains("unknown")
    ));
}

#[test]
fn cluster_health_snapshots_the_leader_view() {
    let health = cluster_health(&StubCoordinator::leader());
    assert!(health.has_leader());
    assert!(health.is_leader());
    assert_eq!(health.leader_endpoint(), Some("http://cluster/one"));
    assert_eq!(health.cluster_size(), 2);
    assert_eq!(health.node_id(), "one");

    // The report is a plain value: clonable, comparable, and debuggable.
    let copy = health.clone();
    assert_eq!(health, copy);
    assert!(format!("{health:?}").contains("one"));
}

#[test]
fn cluster_health_snapshots_follower_and_leaderless_views() {
    let follower = cluster_health(&StubCoordinator::follower());
    assert!(follower.has_leader());
    assert!(!follower.is_leader());
    assert_eq!(follower.leader_endpoint(), Some("http://cluster/one"));

    let leaderless = cluster_health(&StubCoordinator::leaderless());
    assert!(!leaderless.has_leader());
    assert!(!leaderless.is_leader());
    assert_eq!(leaderless.leader_endpoint(), None);
}
