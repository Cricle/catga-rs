#![allow(missing_docs)]

//! Public, deterministic contract coverage for cluster coordination primitives.

use std::time::Duration;

use catga_cluster::{
    ClusterCoordinator, ClusterCoordinatorExt, MemoryCluster, RaftClusterConfig, RaftInboundPolicy,
    RaftInboundPolicyError, RaftInboundRejection, RaftMember, RaftMessage, RaftNode, RaftNodeError,
    RaftPeerIdentity, StaticRaftInboundPolicy, cluster_health,
};
use catga_core::ErrorCode;

#[test]
fn memory_cluster_publishes_elections_and_gates_leader_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime must build");

    runtime.block_on(async {
        let cluster = MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"]);
        let one = cluster
            .node("one")
            .expect("configured leader must be present");
        let two = cluster
            .node("two")
            .expect("configured follower must be present");
        let mut subscription = two.subscribe_leadership();

        assert_eq!(subscription.snapshot().epoch, 0);
        assert_eq!(
            subscription.snapshot().leader_endpoint.as_deref(),
            Some("http://cluster/one")
        );
        assert!(one.is_leader());
        assert!(!two.is_leader());
        assert_eq!(cluster_health(one.as_ref()).cluster_size(), 2);
        assert_eq!(one.execute_if_leader(|| async { "ran" }).await, Some("ran"));
        assert_eq!(two.execute_if_leader(|| async { "ran" }).await, None);
        assert_eq!(
            two.execute_if_leader_cancellable(|_| async { Ok::<_, catga_core::CatgaError>(()) })
                .await
                .expect_err("followers must reject cancellable work")
                .code(),
            ErrorCode::Unavailable
        );

        assert_eq!(cluster.elect("two"), Some(()));
        let elected = subscription.recv().await.expect("election must publish");
        assert_eq!(elected.epoch, 1);
        assert_eq!(elected.leader_node_id.as_deref(), Some("two"));
        assert_eq!(
            elected.leader_endpoint.as_deref(),
            Some("http://cluster/two")
        );
        assert!(two.wait_for_leadership(Duration::ZERO).await);
        assert!(!one.wait_for_leadership(Duration::ZERO).await);
        assert_eq!(cluster.elect("missing"), None);
        assert!(cluster.node("missing").is_none());
    });
}

#[test]
fn local_configuration_and_node_construction_validate_membership() {
    let config = RaftClusterConfig::local(1, 3, 12_000).expect("local config must be valid");
    let members = config.members().expect("local members must be valid");

    assert_eq!(
        config
            .tick_interval()
            .expect("default timing must validate"),
        Duration::from_millis(10)
    );
    assert_eq!(members.len(), 3);
    assert_eq!(members[0].id(), 2);
    assert_eq!(members[0].endpoint(), "http://localhost:12001");
    assert!(matches!(
        RaftClusterConfig::local(3, 3, 12_000),
        Err(catga_cluster::RaftClusterConfigError::InvalidLocalCluster)
    ));
    assert!(matches!(
        RaftClusterConfig::local(0, 2, u16::MAX),
        Err(catga_cluster::RaftClusterConfigError::InvalidLocalCluster)
    ));

    assert!(matches!(
        RaftNode::new(1, "http://cluster/one", Vec::new()),
        Err(RaftNodeError::EmptyMembers)
    ));
    assert!(matches!(
        RaftNode::new(
            1,
            "http://cluster/one",
            vec![RaftMember::new(1, "http://cluster/other")],
        ),
        Err(RaftNodeError::LocalEndpointMismatch { .. })
    ));
    assert!(matches!(
        RaftNode::new(
            1,
            "http://cluster/one",
            vec![
                RaftMember::new(1, "http://cluster/one"),
                RaftMember::new(1, "http://cluster/duplicate"),
            ],
        ),
        Err(RaftNodeError::DuplicateMemberId(1))
    ));
}

#[test]
fn static_inbound_policy_binds_sender_ids_to_authenticated_peers() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "  node-two  ")])
        .expect("valid member map must construct");
    let authenticated = RaftPeerIdentity::new("node-two").expect("non-empty identity must work");
    let another_peer = RaftPeerIdentity::new("node-three").expect("non-empty identity must work");
    let mut message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    };

    assert_eq!(policy.authorize(Some(&authenticated), &message), Ok(()));
    assert_eq!(
        policy.authorize(None, &message),
        Err(RaftInboundRejection::Unauthenticated)
    );
    assert_eq!(
        policy.authorize(Some(&another_peer), &message),
        Err(RaftInboundRejection::Forbidden)
    );
    message.to = 2;
    assert_eq!(
        policy.authorize(Some(&authenticated), &message),
        Err(RaftInboundRejection::Forbidden)
    );
    assert!(matches!(
        StaticRaftInboundPolicy::new(0, [(2, "node-two")]),
        Err(RaftInboundPolicyError::ZeroNodeId)
    ));
    assert!(matches!(
        RaftPeerIdentity::new(" \t "),
        Err(RaftInboundPolicyError::EmptyIdentity)
    ));
}
