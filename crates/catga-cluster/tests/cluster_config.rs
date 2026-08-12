//! Contract tests for `RaftClusterConfig` validation and error reporting that
//! do not require deserializing user-supplied configuration.

use std::error::Error;
use std::time::Duration;

use catga_cluster::{RaftClusterConfig, RaftClusterConfigError, RaftNodeError};

#[test]
fn local_builds_a_deterministic_member_layout() {
    let config = RaftClusterConfig::local(1, 3, 9000).expect("valid dimensions");
    let members = config.members().expect("valid members");

    assert_eq!(members.len(), 3);
    assert_eq!(members[0].id(), 2);
    assert_eq!(members[0].endpoint(), "http://localhost:9001");
    assert_eq!(members[1].id(), 1);
    assert_eq!(members[2].id(), 3);
}

#[test]
fn local_rejects_impossible_dimensions() {
    assert!(matches!(
        RaftClusterConfig::local(0, 0, 9000),
        Err(RaftClusterConfigError::InvalidLocalCluster)
    ));
    assert!(matches!(
        RaftClusterConfig::local(3, 3, 9000),
        Err(RaftClusterConfigError::InvalidLocalCluster)
    ));
    // Three nodes starting at the last port cannot fit in the port space.
    assert!(matches!(
        RaftClusterConfig::local(0, 3, u16::MAX),
        Err(RaftClusterConfigError::InvalidLocalCluster)
    ));
}

#[test]
fn raft_timing_derives_validated_tick_durations() {
    let config = RaftClusterConfig::local(0, 1, 9000).expect("valid dimensions");
    let timing = config.raft_timing().expect("default timing is valid");

    assert_eq!(timing.tick_interval(), Duration::from_millis(10));
    assert_eq!(
        config.tick_interval().expect("default timing is valid"),
        Duration::from_millis(10)
    );
    assert!(timing.election_ticks() > timing.heartbeat_ticks());
    assert_eq!(timing, timing.clone());
}

#[test]
fn config_error_display_names_each_failure() {
    assert_eq!(
        RaftClusterConfigError::ZeroMemberId.to_string(),
        "Raft member id zero is reserved"
    );
    assert_eq!(
        RaftClusterConfigError::EmptyEndpoint.to_string(),
        "Raft member endpoints must not be empty"
    );
    assert_eq!(
        RaftClusterConfigError::LocalMemberDuplicated(7).to_string(),
        "remote members must not repeat local node id 7"
    );
    assert_eq!(
        RaftClusterConfigError::DuplicateMemberId(4).to_string(),
        "duplicate remote Raft member id 4"
    );
    assert_eq!(
        RaftClusterConfigError::InvalidTiming.to_string(),
        "tick, heartbeat, and election timing must be non-zero with election after heartbeat"
    );
    assert_eq!(
        RaftClusterConfigError::InvalidLocalCluster.to_string(),
        "a local cluster needs at least one node and a valid local id"
    );

    let node_error = RaftClusterConfigError::Node(RaftNodeError::ZeroMemberId);
    assert_eq!(
        node_error.to_string(),
        RaftNodeError::ZeroMemberId.to_string()
    );
}

#[test]
fn config_error_exposes_the_node_error_as_its_source() {
    let node_error = RaftClusterConfigError::Node(RaftNodeError::ZeroMemberId);
    let source = node_error.source().expect("node errors carry a source");
    assert_eq!(source.to_string(), RaftNodeError::ZeroMemberId.to_string());

    assert!(RaftClusterConfigError::ZeroMemberId.source().is_none());
    assert!(RaftClusterConfigError::InvalidTiming.source().is_none());
}

#[test]
fn config_error_converts_from_a_node_error() {
    let error: RaftClusterConfigError = RaftNodeError::ZeroMemberId.into();
    assert!(matches!(error, RaftClusterConfigError::Node(_)));
}
