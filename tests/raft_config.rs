//! Raft configuration validation tests.

use std::time::Duration;

use catga_cluster::{ClusterCoordinator, RaftClusterConfig, RaftClusterConfigError, RaftMember};

#[test]
fn cluster_config_deserializes_validates_timing_and_opens_persistent_nodes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let json = serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://node-1",
        "members": [
            { "id": 2, "endpoint": "http://node-2" },
            { "id": 3, "endpoint": "http://node-3" }
        ],
        "tickIntervalMs": 10,
        "electionTimeoutMs": 151,
        "heartbeatIntervalMs": 51,
        "persistentStatePath": directory.path()
    });
    let config: RaftClusterConfig = serde_json::from_value(json)?;

    assert_eq!(config.tick_interval()?, Duration::from_millis(10));
    assert_eq!(config.raft_timing()?.election_ticks(), 16);
    assert_eq!(config.raft_timing()?.heartbeat_ticks(), 6);
    assert_eq!(
        config.members()?,
        vec![
            RaftMember::new(1, "http://node-1"),
            RaftMember::new(2, "http://node-2"),
            RaftMember::new(3, "http://node-3"),
        ]
    );
    assert_eq!(config.open_node()?.id(), 1);

    Ok(())
}

#[test]
fn cluster_config_rejects_invalid_timing_and_duplicate_remote_members()
-> Result<(), Box<dyn std::error::Error>> {
    let timing: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://node-1",
        "members": [{ "id": 2, "endpoint": "http://node-2" }],
        "tickIntervalMs": 10,
        "electionTimeoutMs": 50,
        "heartbeatIntervalMs": 50
    }))?;
    assert!(timing.raft_timing().is_err());

    let duplicate: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://node-1",
        "members": [
            { "id": 2, "endpoint": "http://node-2" },
            { "id": 2, "endpoint": "http://node-2b" }
        ]
    }))?;
    assert!(duplicate.members().is_err());

    Ok(())
}

#[test]
fn cluster_config_rejects_a_reserved_remote_raft_member_id()
-> Result<(), Box<dyn std::error::Error>> {
    let config: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://node-1",
        "members": [{ "id": 0, "endpoint": "http://node-0" }]
    }))?;

    assert!(config.members().is_err());

    Ok(())
}

#[test]
fn local_cluster_rejects_impossible_port_ranges_without_panicking() {
    let result = std::panic::catch_unwind(|| RaftClusterConfig::local(0, u64::MAX, 1));

    assert!(matches!(
        result,
        Ok(Err(RaftClusterConfigError::InvalidLocalCluster))
    ));
}

#[test]
fn local_cluster_maps_zero_based_process_indexes_to_valid_raft_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let config = RaftClusterConfig::local(1, 3, 6_000)?;

    assert_eq!(
        config.members()?,
        vec![
            RaftMember::new(2, "http://localhost:6001"),
            RaftMember::new(1, "http://localhost:6000"),
            RaftMember::new(3, "http://localhost:6002"),
        ]
    );

    Ok(())
}

#[test]
fn local_cluster_allows_one_stable_raft_member() -> Result<(), Box<dyn std::error::Error>> {
    let config = RaftClusterConfig::local(0, 1, 6_000)?;

    assert_eq!(
        config.members()?,
        vec![RaftMember::new(1, "http://localhost:6000")]
    );

    Ok(())
}

#[test]
fn deserialized_single_node_cluster_opens_and_elects() -> Result<(), Box<dyn std::error::Error>> {
    let config: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://node-1",
        "members": []
    }))?;

    assert_eq!(config.members()?, vec![RaftMember::new(1, "http://node-1")]);
    let mut node = config.open_node()?;
    node.campaign()?;
    assert!(node.coordinator().is_leader());

    Ok(())
}
