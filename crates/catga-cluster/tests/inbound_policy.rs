//! Contract tests for transport-neutral Raft ingress authorization:
//! peer identity validation, static policy construction, and per-frame decisions.

use std::sync::Arc;

use catga_cluster::{
    RaftInboundPolicy, RaftInboundPolicyError, RaftInboundRejection, RaftMessage, RaftPeerIdentity,
    StaticRaftInboundPolicy,
};

fn frame(from: u64, to: u64) -> RaftMessage {
    RaftMessage {
        from,
        to,
        ..Default::default()
    }
}

#[test]
fn peer_identity_trims_and_rejects_empty_values() {
    let identity = RaftPeerIdentity::new("  spiffe://node-2  ").expect("non-empty identity");
    assert_eq!(identity.as_str(), "spiffe://node-2");
    assert_eq!(identity.as_ref(), "spiffe://node-2");

    assert_eq!(
        RaftPeerIdentity::new("   "),
        Err(RaftInboundPolicyError::EmptyIdentity)
    );
}

#[test]
fn static_policy_validates_the_member_map() {
    assert!(matches!(
        StaticRaftInboundPolicy::new(0, [(2, "peer-two")]),
        Err(RaftInboundPolicyError::ZeroNodeId)
    ));
    assert!(matches!(
        StaticRaftInboundPolicy::new(1, [(0, "peer-zero")]),
        Err(RaftInboundPolicyError::ZeroNodeId)
    ));
    assert!(matches!(
        StaticRaftInboundPolicy::new(1, [(2, "  ")]),
        Err(RaftInboundPolicyError::EmptyIdentity)
    ));
    assert!(matches!(
        StaticRaftInboundPolicy::new(1, [(2, "a"), (2, "b")]),
        Err(RaftInboundPolicyError::DuplicatePeerId)
    ));
}

#[test]
fn policy_error_display_names_each_failure() {
    assert_eq!(
        RaftInboundPolicyError::ZeroNodeId.to_string(),
        "Raft node IDs must be non-zero"
    );
    assert_eq!(
        RaftInboundPolicyError::DuplicatePeerId.to_string(),
        "a Raft peer identity was configured more than once"
    );
    assert_eq!(
        RaftInboundPolicyError::EmptyIdentity.to_string(),
        "a Raft peer identity must not be empty"
    );
}

#[test]
fn authorize_rejects_unauthenticated_frames() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy");
    assert_eq!(
        policy.authorize(None, &frame(2, 1)),
        Err(RaftInboundRejection::Unauthenticated)
    );
}

#[test]
fn authorize_rejects_frames_not_addressed_to_the_local_node() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy");
    let peer = RaftPeerIdentity::new("peer-two").expect("non-empty identity");
    assert_eq!(
        policy.authorize(Some(&peer), &frame(2, 3)),
        Err(RaftInboundRejection::Forbidden)
    );
}

#[test]
fn authorize_rejects_self_originated_and_zero_sender_frames() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy");
    let peer = RaftPeerIdentity::new("peer-two").expect("non-empty identity");
    assert_eq!(
        policy.authorize(Some(&peer), &frame(1, 1)),
        Err(RaftInboundRejection::Forbidden)
    );
    assert_eq!(
        policy.authorize(Some(&peer), &frame(0, 1)),
        Err(RaftInboundRejection::Forbidden)
    );
}

#[test]
fn authorize_rejects_unknown_members_and_mismatched_identities() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy");
    let peer = RaftPeerIdentity::new("peer-two").expect("non-empty identity");
    let impostor = RaftPeerIdentity::new("peer-three").expect("non-empty identity");

    assert_eq!(
        policy.authorize(Some(&peer), &frame(3, 1)),
        Err(RaftInboundRejection::Forbidden),
        "sender 3 is not a configured member"
    );
    assert_eq!(
        policy.authorize(Some(&impostor), &frame(2, 1)),
        Err(RaftInboundRejection::Forbidden),
        "the identity must match the claimed sender"
    );
}

#[test]
fn authorize_accepts_the_bound_identity_for_a_member() {
    let policy = StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid policy");
    let peer = RaftPeerIdentity::new("peer-two").expect("non-empty identity");
    assert_eq!(policy.authorize(Some(&peer), &frame(2, 1)), Ok(()));
}

#[test]
fn policies_dispatch_through_arc_and_closures() {
    let policy = Arc::new(StaticRaftInboundPolicy::new(1, [(2, "peer-two")]).expect("valid"));
    let peer = RaftPeerIdentity::new("peer-two").expect("non-empty identity");
    assert_eq!(policy.authorize(Some(&peer), &frame(2, 1)), Ok(()));

    let closure =
        |peer: Option<&RaftPeerIdentity>, message: &RaftMessage| match (peer, message.from) {
            (Some(identity), 2) if identity.as_str() == "peer-two" => Ok(()),
            (Some(_), _) => Err(RaftInboundRejection::Forbidden),
            (None, _) => Err(RaftInboundRejection::Unauthenticated),
        };
    assert_eq!(closure.authorize(Some(&peer), &frame(2, 1)), Ok(()));
    assert_eq!(
        closure.authorize(None, &frame(2, 1)),
        Err(RaftInboundRejection::Unauthenticated)
    );
}
