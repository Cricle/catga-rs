use std::sync::Arc;

use catga_cluster::{ClusterCoordinator, ClusterCoordinatorExt, RaftMember, RaftMessage, RaftNode};
use catga_core::{CatgaResult, Handler, Mediator, Pipeline, Registry, Request};

use async_trait::async_trait;

fn members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ]
}

fn relay(nodes: &mut [RaftNode]) {
    for _ in 0..100 {
        let messages: Vec<RaftMessage> = nodes
            .iter_mut()
            .flat_map(RaftNode::drain_messages)
            .collect::<Vec<_>>();
        if messages.is_empty() {
            return;
        }
        for message in messages {
            nodes
                .iter_mut()
                .find(|node| node.id() == message.to)
                .expect("Raft must only address configured peers")
                .step(message)
                .unwrap();
        }
    }
    panic!("Raft messages did not quiesce");
}

#[tokio::test]
async fn raft_leadership_subscription_publishes_the_campaign_winner() {
    let mut node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .expect("valid single-node Raft configuration");
    let coordinator = node.coordinator();
    let mut receiver = coordinator.subscribe_leadership();

    assert_eq!(receiver.snapshot().leader_node_id, None);
    node.campaign().expect("single node can campaign");

    let snapshot = receiver
        .recv()
        .await
        .expect("coordinator remains available");
    assert_eq!(snapshot.epoch, 1);
    assert_eq!(snapshot.leader_node_id.as_deref(), Some("1"));
    assert_eq!(snapshot.leader_endpoint.as_deref(), Some("http://node-1"));
}

#[tokio::test]
async fn raft_leadership_subscription_closes_after_its_coordinator_drops() {
    let mut subscription = {
        let node = RaftNode::new(
            1,
            "http://node-1",
            vec![RaftMember::new(1, "http://node-1")],
        )
        .expect("valid single-node Raft configuration");
        let coordinator = node.coordinator();
        coordinator.subscribe_leadership()
    };

    let result =
        tokio::time::timeout(std::time::Duration::from_millis(50), subscription.recv()).await;
    assert!(matches!(
        result,
        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed))
    ));
}

#[test]
fn raft_node_elects_and_commits_a_single_node_proposal() {
    let mut node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .unwrap();
    let coordinator = node.coordinator();

    node.campaign().unwrap();
    assert!(coordinator.is_leader());
    assert_eq!(
        coordinator.leader_endpoint().as_deref(),
        Some("http://node-1")
    );

    node.propose(b"create-order:7").unwrap();
    assert_eq!(
        node.drain_committed()
            .into_iter()
            .map(|entry| entry.data)
            .collect::<Vec<_>>(),
        [b"create-order:7".to_vec()]
    );
}

#[test]
fn raft_node_applies_backpressure_before_pending_commits_grow_unbounded() {
    let mut node = RaftNode::new_with_pending_commit_capacity(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
        1,
    )
    .unwrap();

    node.campaign().unwrap();
    node.propose(b"first").unwrap();
    assert_eq!(node.pending_commit_count(), 1);

    assert!(matches!(
        node.try_propose(b"second"),
        Err(catga_cluster::RaftNodeError::PendingCommitCapacity { capacity: 1 })
    ));
    assert_eq!(node.pending_commit_count(), 1);
    assert_eq!(node.next_committed().unwrap().data, b"first");
}

#[test]
fn raft_node_pages_peer_commits_without_losing_a_durable_overflow() {
    let cluster_members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ];
    let mut nodes = vec![
        RaftNode::new(1, "http://node-1", cluster_members.clone()).unwrap(),
        RaftNode::new_with_pending_commit_capacity(2, "http://node-2", cluster_members, 1).unwrap(),
    ];

    nodes[0].campaign().unwrap();
    relay(&mut nodes);
    nodes[0].propose(b"first").unwrap();
    nodes[0].propose(b"second").unwrap();
    relay(&mut nodes);

    let follower = &mut nodes[1];
    assert_eq!(follower.pending_commit_count(), 1);
    assert_eq!(follower.next_committed().unwrap().data, b"first");
    assert_eq!(
        follower.try_next_committed().unwrap().unwrap().data,
        b"second"
    );
}

#[test]
fn persistent_raft_node_recovers_committed_log_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let members = vec![RaftMember::new(1, "http://node-1")];

    {
        let mut node =
            RaftNode::open_persistent(1, "http://node-1", members.clone(), directory.path())
                .unwrap();
        node.campaign().unwrap();
        node.propose(b"create-order:8").unwrap();
        assert_eq!(
            node.drain_committed()
                .into_iter()
                .map(|entry| entry.data)
                .collect::<Vec<_>>(),
            [b"create-order:8".to_vec()]
        );
    }

    let mut restarted =
        RaftNode::open_persistent(1, "http://node-1", members, directory.path()).unwrap();
    assert_eq!(
        restarted
            .persisted_committed_entries()
            .unwrap()
            .into_iter()
            .map(|entry| entry.data)
            .collect::<Vec<_>>(),
        [b"create-order:8".to_vec()]
    );

    restarted.campaign().unwrap();
    restarted.propose(b"create-order:9").unwrap();
    assert!(
        restarted
            .drain_committed()
            .into_iter()
            .any(|entry| entry.data == b"create-order:9")
    );
}

#[test]
fn raft_nodes_replicate_a_proposal_and_publish_the_elected_leader() {
    let cluster_members = members();
    let mut nodes = cluster_members
        .iter()
        .map(|member| {
            RaftNode::new(member.id(), member.endpoint(), cluster_members.clone()).unwrap()
        })
        .collect::<Vec<_>>();

    nodes[0].campaign().unwrap();
    relay(&mut nodes);
    assert!(
        nodes
            .iter()
            .all(|node| node.coordinator().leader_endpoint().as_deref() == Some("http://node-1"))
    );

    nodes[0].propose(b"reserve-inventory:9").unwrap();
    relay(&mut nodes);
    for node in &mut nodes {
        assert!(
            node.drain_committed()
                .into_iter()
                .any(|entry| entry.data == b"reserve-inventory:9")
        );
    }
}

#[derive(Debug)]
struct RaftLeaderWork;

impl catga_core::Message for RaftLeaderWork {}

impl Request for RaftLeaderWork {
    type Response = u8;
}

struct RaftLeaderHandler;

#[async_trait]
impl Handler<RaftLeaderWork> for RaftLeaderHandler {
    async fn handle(&self, _: RaftLeaderWork) -> CatgaResult<u8> {
        Ok(42)
    }
}

#[tokio::test]
async fn raft_coordinator_enables_leader_only_pipeline_after_election() {
    let mut node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .unwrap();
    node.campaign().unwrap();
    let coordinator = node.coordinator();
    assert!(
        coordinator
            .wait_for_leadership(std::time::Duration::from_millis(1))
            .await
    );

    let mut registry = Registry::new();
    registry
        .register_request::<RaftLeaderWork, _>(RaftLeaderHandler)
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(catga_cluster::LeaderOnlyBehavior::new(Arc::clone(
        &coordinator,
    )));

    assert_eq!(
        mediator.send_with(RaftLeaderWork, &pipeline).await.unwrap(),
        42
    );

    assert_eq!(
        ClusterCoordinatorExt::execute_if_leader(coordinator.as_ref(), || async { 7_u8 }).await,
        Some(7)
    );
}
