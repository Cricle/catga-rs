//! Integration tests for CatgaRaft runtime lifecycle.
//!
//! These tests verify the full runtime lifecycle including:
//! - Builder creation and configuration
//! - Runtime startup with state machine
//! - Proposal handling (leader vs non-leader)
//! - Coordinator state management

use catga_core::{
    CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine, ErrorCode,
};
use catga_raft::CatgaRaftRuntimeBuilder;
use std::sync::Mutex;

/// A test state machine that records applied entries.
#[derive(Default)]
struct TestMachine {
    /// Record of all applied entries (index, data).
    applied: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl TestMachine {
    /// Creates a new TestMachine instance.
    fn new() -> Self {
        Self::default()
    }

    /// Returns the number of entries that have been applied.
    #[allow(dead_code)]
    fn applied_count(&self) -> usize {
        self.applied.lock().unwrap().len()
    }

    /// Returns a copy of all applied entries.
    #[allow(dead_code)]
    fn get_applied(&self) -> Vec<(u64, Vec<u8>)> {
        self.applied.lock().unwrap().clone()
    }

    /// Clears all applied entries.
    #[allow(dead_code)]
    fn clear(&self) {
        self.applied.lock().unwrap().clear();
    }
}

impl ConsensusStateMachine for TestMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let mut applied = self.applied.lock().unwrap();
        applied.push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(vec![])
    }

    fn restore(&mut self, _data: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

// ============================================================================
// Test 1: Runtime Builder from CLI
// ============================================================================

/// Tests that the builder can be created from CLI-style arguments.
#[tokio::test]
async fn test_runtime_builder_from_cli() {
    // Create builder from CLI args for a 3-node cluster, this is node 0
    let builder = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3).expect("from_cli should succeed");

    // Verify node ID is correctly computed (node index 0 -> node_id 1)
    assert_eq!(builder.config().node_id, 1);

    // Verify cluster ID defaults to 1
    assert_eq!(builder.config().cluster_id, 1);

    // Verify members are correctly computed (nodes 1 and 2)
    let members = builder.members();
    assert_eq!(members.len(), 2);

    // Node 1 (index 1) should have id 2 and port 9100 + 1 * 100 = 9200
    assert_eq!(members[0].0, 2);
    assert_eq!(members[0].1, "http://127.0.0.1:9200");

    // Node 2 (index 2) should have id 3 and port 9100 + 2 * 100 = 9300
    assert_eq!(members[1].0, 3);
    assert_eq!(members[1].1, "http://127.0.0.1:9300");
}

/// Tests builder from_cli with single node cluster.
#[tokio::test]
async fn test_runtime_builder_from_cli_single_node() {
    let builder = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 1).expect("from_cli should succeed");

    assert_eq!(builder.config().node_id, 1);
    assert!(builder.members().is_empty());
}

/// Tests builder from_cli with middle node in cluster.
#[tokio::test]
async fn test_runtime_builder_from_cli_middle_node() {
    // Node index 1 (middle node) in a 3-node cluster
    let builder = CatgaRaftRuntimeBuilder::from_cli(9100, 1, 3).expect("from_cli should succeed");

    assert_eq!(builder.config().node_id, 2); // node index 1 -> node_id 2
    assert_eq!(builder.members().len(), 2);

    // Should include nodes 0 and 2 (excluding this node)
    let member_ids: Vec<u64> = builder.members().iter().map(|(id, _)| *id).collect();
    assert!(member_ids.contains(&1)); // node 0 -> id 1
    assert!(member_ids.contains(&3)); // node 2 -> id 3
}

/// Tests that from_cli fails with zero nodes.
#[tokio::test]
async fn test_runtime_builder_from_cli_zero_nodes() {
    let result = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 0);
    assert!(result.is_err());
}

// ============================================================================
// Test 2: Runtime Startup
// ============================================================================

/// Tests that the runtime starts correctly with a state machine.
#[tokio::test]
async fn test_runtime_startup() {
    let state_machine = TestMachine::new();
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .with_cluster_id(1)
        .start(state_machine)
        .await
        .expect("start should succeed");

    // Verify runtime is alive
    assert!(runtime.is_alive(), "runtime should be alive after startup");

    // Verify config is correctly set
    assert_eq!(runtime.config().node_id, 1);
    assert_eq!(runtime.config().cluster_id, 1);

    // Verify coordinator is accessible
    let coord = runtime.coordinator();
    assert_eq!(coord.node_id(), "node-1");
    assert!(!coord.is_leader(), "node should not be leader on startup");
    assert!(
        coord.leader_endpoint().is_none(),
        "no leader known on startup"
    );

    // Shutdown the runtime
    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests startup with custom configuration.
#[tokio::test]
async fn test_runtime_startup_custom_config() {
    use catga_raft::config::CatgaRaftConfig;
    let config = CatgaRaftConfig {
        node_id: 42,
        cluster_id: 100,
        election_tick: 20,
        heartbeat_tick: 5,
        max_size_per_msg: 128 * 1024 * 1024,
        max_inflight_msgs: 512,
    };

    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(config)
        .with_members(vec![
            (2, "http://127.0.0.1:9200".to_string()),
            (3, "http://127.0.0.1:9300".to_string()),
        ])
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    assert!(runtime.is_alive());
    assert_eq!(runtime.config().node_id, 42);
    assert_eq!(runtime.config().cluster_id, 100);
    assert_eq!(runtime.config().election_tick, 20);
    assert_eq!(runtime.config().heartbeat_tick, 5);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that multiple runtimes can coexist (for multi-node simulation).
#[tokio::test]
async fn test_runtime_multiple_nodes() {
    // Start runtime for node 0
    let runtime0 = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Start runtime for node 1
    let runtime1 = CatgaRaftRuntimeBuilder::from_cli(9100, 1, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Start runtime for node 2
    let runtime2 = CatgaRaftRuntimeBuilder::from_cli(9100, 2, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Verify all runtimes are alive
    assert!(runtime0.is_alive());
    assert!(runtime1.is_alive());
    assert!(runtime2.is_alive());

    // Verify node IDs
    assert_eq!(runtime0.config().node_id, 1);
    assert_eq!(runtime1.config().node_id, 2);
    assert_eq!(runtime2.config().node_id, 3);

    // Shutdown all runtimes
    runtime0.shutdown();
    runtime1.shutdown();
    runtime2.shutdown();

    let _ = Box::new(runtime0).join().await;
    let _ = Box::new(runtime1).join().await;
    let _ = Box::new(runtime2).join().await;
}

// ============================================================================
// Test 3: Propose as Leader
// ============================================================================

/// Tests that proposals succeed when the node is set as leader.
#[tokio::test]
async fn test_runtime_propose_as_leader() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Set this node as leader
    runtime.set_leader(Some("http://127.0.0.1:9100".to_string()));

    // Verify coordinator shows we are leader
    let coord = runtime.coordinator();
    assert!(coord.is_leader(), "should be leader after set_leader");
    assert!(coord.leader_endpoint().is_some());

    // Propose some data
    let result = runtime.propose(b"test data".to_vec()).await;
    assert!(result.is_ok(), "propose should succeed when leader");

    // Propose more data
    let result = runtime.propose(vec![1, 2, 3, 4, 5]).await;
    assert!(result.is_ok(), "second propose should succeed");

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that leader status can be changed dynamically.
#[tokio::test]
async fn test_runtime_leader_transition() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Initially not leader
    let coord = runtime.coordinator();
    assert!(!coord.is_leader());

    // Propose fails when not leader
    let result = runtime.propose(b"should fail".to_vec()).await;
    assert!(result.is_err());

    // Set as leader
    runtime.set_leader(Some("http://127.0.0.1:9100".to_string()));
    assert!(runtime.coordinator().is_leader());

    // Now propose succeeds
    let result = runtime.propose(b"should succeed".to_vec()).await;
    assert!(result.is_ok());

    // Lose leadership
    runtime.set_leader(None);
    assert!(!runtime.coordinator().is_leader());

    // Propose fails again
    let result = runtime.propose(b"should fail again".to_vec()).await;
    assert!(result.is_err());

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Test 4: Propose Not Leader
// ============================================================================

/// Tests that proposals fail when the node is not the leader.
#[tokio::test]
async fn test_runtime_not_leader() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Explicitly ensure we are not leader
    runtime.set_leader(None);

    let coord = runtime.coordinator();
    assert!(!coord.is_leader(), "should not be leader");
    assert!(
        coord.leader_endpoint().is_none(),
        "no leader should be known"
    );

    // Propose should fail
    let result = runtime.propose(b"test data".to_vec()).await;
    assert!(result.is_err(), "propose should fail when not leader");

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that proposals fail even without explicitly calling set_leader(None).
#[tokio::test]
async fn test_runtime_not_leader_default() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Default state should be non-leader
    let coord = runtime.coordinator();
    assert!(!coord.is_leader(), "default state should not be leader");

    // Propose should fail
    let result = runtime.propose(b"test data".to_vec()).await;
    assert!(result.is_err());

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that knowing about a leader endpoint still means this node is leader.
/// Note: set_leader(Some(...)) means "I am the leader" - there's no separate
/// "knowing about another leader" state in the current API.
#[tokio::test]
async fn test_runtime_set_leader_endpoint() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Simulate knowing a leader exists (and being that leader)
    runtime.set_leader(Some("http://127.0.0.1:9200".to_string()));

    let coord = runtime.coordinator();
    assert!(coord.is_leader(), "coordinator should report we are leader");
    assert_eq!(
        coord.leader_endpoint().map(|s| s.to_string()),
        Some("http://127.0.0.1:9200".to_string())
    );

    // We are the leader, so propose should succeed
    let result = runtime.propose(b"test data".to_vec()).await;
    assert!(
        result.is_ok(),
        "propose should succeed when we are the leader"
    );

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Test 5: Coordinator State
// ============================================================================

/// Tests that the coordinator correctly tracks node ID.
#[tokio::test]
async fn test_runtime_coordinator_node_id() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 2, 5)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    let coord = runtime.coordinator();
    assert_eq!(coord.node_id(), "node-3", "node index 2 -> id 3");

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that the coordinator correctly tracks leadership state.
#[tokio::test]
async fn test_runtime_coordinator_leadership() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    let coord = runtime.coordinator();

    // Initial state: not leader
    assert!(!coord.is_leader());
    assert!(coord.leader_endpoint().is_none());

    // Simulate knowing about another leader (node at port 9200)
    runtime.set_leader(Some("http://127.0.0.1:9200".to_string()));
    // Note: coordinator.is_leader() returns true when ANY endpoint is set
    // This reflects the current implementation where is_leader = endpoint.is_some()
    assert!(coord.is_leader());
    assert!(coord.leader_endpoint().is_some());

    // Clear leadership
    runtime.set_leader(None);
    assert!(!coord.is_leader());
    assert!(coord.leader_endpoint().is_none());

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// Tests that the coordinator correctly tracks member endpoints.
#[tokio::test]
async fn test_runtime_coordinator_members() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    let coord = runtime.coordinator();
    let members = coord.member_endpoints();

    // Node 0 in a 3-node cluster should have 2 other members
    assert_eq!(members.len(), 2);

    // Members are node 1 (port 9100 + 1 * 100 = 9200) and node 2 (port 9100 + 2 * 100 = 9300)
    let member_strs: Vec<String> = members.iter().map(|s| s.to_string()).collect();
    assert!(member_strs.contains(&"http://127.0.0.1:9200".to_string()));
    assert!(member_strs.contains(&"http://127.0.0.1:9300".to_string()));

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Test 6: Shutdown Lifecycle
// ============================================================================

/// Tests that shutdown signals are properly propagated.
#[tokio::test]
async fn test_runtime_shutdown_signal() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Runtime is alive
    assert!(runtime.is_alive());
    assert!(!runtime.is_shutdown_requested());

    // Request shutdown
    runtime.shutdown();

    // Shutdown is requested
    assert!(runtime.is_shutdown_requested());

    // Join and wait for shutdown
    Box::new(runtime).join().await.expect("join should succeed");
}

// ============================================================================
// Test 7: Applied Index
// ============================================================================

/// Tests that applied_index returns the correct value.
#[tokio::test]
async fn test_runtime_applied_index() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(9100, 0, 3)
        .expect("from_cli should succeed")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Initially applied index should be 0
    let index = runtime
        .applied_index()
        .await
        .expect("applied_index should succeed");
    assert_eq!(index, 0);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Test 8: Membership Operations
// ============================================================================

/// Tests that add_member and remove_member are routed through the owner
/// loop and rejected with `NotLeader` (mapped to `Unavailable`) on a node
/// that cannot hold leadership: its self endpoint sits on an isolated port
/// (so no other suite in this binary collides with it) and its members are
/// unreachable, so it can never gather a quorum and the outcome is
/// deterministic.
#[tokio::test]
async fn test_runtime_membership_operations() {
    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(catga_raft::CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_self_endpoint("http://127.0.0.1:19900")
        .with_member(2, "http://127.0.0.1:19901")
        .with_member(3, "http://127.0.0.1:19902")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    // Not the leader: membership requests are rejected honestly.
    let result = runtime
        .add_member(4, "http://127.0.0.1:19903".to_string())
        .await;
    let err = result.expect_err("add_member must fail when not leader");
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.message().contains("not leader"));

    let result = runtime.remove_member(2).await;
    let err = result.expect_err("remove_member must fail when not leader");
    assert_eq!(err.code(), ErrorCode::Unavailable);
    assert!(err.message().contains("not leader"));

    // The membership view is unchanged by the rejected requests.
    let members = ConsensusRuntime::coordinator(&runtime).member_endpoints();
    assert_eq!(members.len(), 2);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}
