//! Integration tests for `crates/catga-raft/src/builder.rs`.
//!
//! Covers the public surface of [`CatgaRaftRuntimeBuilder`]:
//! - construction via `new()` / `Default` and `from_cli()`
//! - the fluent `with_*` configuration setters (observed through `config()` / `members()`)
//! - error handling for invalid CLI arguments
//! - `start()` validation of broken configurations (zero node id, unparseable
//!   self endpoint, self/duplicate member ids)
//! - `start()` / `start_default()` wiring into a running `CatgaRaftRuntime`
//!
//! Note: pipeline config values have no public getter on the builder, so the
//! pipeline setters are verified by starting a runtime and proposing through
//! the resulting pipeline.

use std::sync::Mutex;
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::{CatgaRaftConfig, CatgaRaftError, CatgaRaftRuntimeBuilder, PipelineConfig};

/// A minimal state machine used to start runtimes from the builder.
#[derive(Default)]
struct TestMachine {
    applied: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl ConsensusStateMachine for TestMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().unwrap().push((index, data.to_vec()));
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
// Construction and defaults
// ============================================================================

/// `new()` and `Default` must produce identical, documented defaults.
#[test]
fn builder_new_and_default_equivalent() {
    let a = CatgaRaftRuntimeBuilder::new();
    let b = CatgaRaftRuntimeBuilder::default();

    for builder in [&a, &b] {
        let cfg = builder.config();
        assert_eq!(cfg.node_id, 0);
        assert_eq!(cfg.cluster_id, 0);
        assert_eq!(cfg.election_tick, 10);
        assert_eq!(cfg.heartbeat_tick, 3);
        assert_eq!(cfg.max_size_per_msg, 8 * 1024 * 1024);
        assert_eq!(cfg.max_inflight_msgs, 256);
        assert!(builder.members().is_empty());
    }
}

// ============================================================================
// from_cli
// ============================================================================

/// `from_cli` derives a 1-indexed node id and peer endpoints at
/// `base_port + i * 100` (absolute peer index), excluding this node itself.
#[test]
fn builder_from_cli_computes_peer_endpoints() {
    let builder = CatgaRaftRuntimeBuilder::from_cli(8300, 0, 3).expect("from_cli should succeed");

    assert_eq!(builder.config().node_id, 1, "node index 0 -> node id 1");
    assert_eq!(builder.config().cluster_id, 1, "cluster id defaults to 1");

    assert_eq!(
        builder.members(),
        &[
            (2, "http://127.0.0.1:8400".to_string()),
            (3, "http://127.0.0.1:8500".to_string()),
        ],
        "peers get ports base_port + index * 100"
    );
}

/// Ports are absolute per node index, so a middle node sees distinct ports
/// for every peer and never collides with its own port.
#[test]
fn builder_from_cli_middle_node_has_distinct_peer_ports() {
    // Node index 2 of 5: peers are indices 0, 1, 3, 4.
    let builder = CatgaRaftRuntimeBuilder::from_cli(8300, 2, 5).expect("from_cli should succeed");

    assert_eq!(builder.config().node_id, 3);
    assert_eq!(
        builder.members(),
        &[
            (1, "http://127.0.0.1:8300".to_string()), // index 0
            (2, "http://127.0.0.1:8400".to_string()), // index 1
            (4, "http://127.0.0.1:8600".to_string()), // index 3
            (5, "http://127.0.0.1:8700".to_string()), // index 4
        ]
    );
}

/// Edge case: node index outside `0..nodes` means this node is never skipped,
/// so every generated peer ends up in the member list.
#[test]
fn builder_from_cli_node_index_beyond_cluster() {
    let builder = CatgaRaftRuntimeBuilder::from_cli(8300, 5, 3).expect("from_cli should succeed");

    assert_eq!(builder.config().node_id, 6);
    let members = builder.members();
    assert_eq!(members.len(), 3, "no peer is excluded when index is out of range");
    let ids: Vec<u64> = members.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

/// Error case: a zero-node cluster is rejected with a `Raft` error.
#[test]
fn builder_from_cli_zero_nodes_rejected() {
    match CatgaRaftRuntimeBuilder::from_cli(8300, 0, 0) {
        Err(CatgaRaftError::Raft(msg)) => {
            assert!(
                msg.contains("at least one node"),
                "unexpected error message: {msg}"
            );
        }
        other => panic!("expected Raft error, got {other:?}"),
    }
}

// ============================================================================
// Fluent configuration setters
// ============================================================================

/// `with_config` fully replaces the Raft configuration.
#[test]
fn builder_with_config_replaces_config() {
    let config = CatgaRaftConfig {
        node_id: 42,
        cluster_id: 7,
        election_tick: 20,
        heartbeat_tick: 5,
        max_size_per_msg: 1024,
        max_inflight_msgs: 8,
    };

    let builder = CatgaRaftRuntimeBuilder::new().with_config(config);
    let cfg = builder.config();
    assert_eq!(cfg.node_id, 42);
    assert_eq!(cfg.cluster_id, 7);
    assert_eq!(cfg.election_tick, 20);
    assert_eq!(cfg.heartbeat_tick, 5);
    assert_eq!(cfg.max_size_per_msg, 1024);
    assert_eq!(cfg.max_inflight_msgs, 8);
}

/// Individual `with_*` setters mutate only their target field and chain.
#[test]
fn builder_fluent_config_setters() {
    let builder = CatgaRaftRuntimeBuilder::new()
        .with_cluster_id(9)
        .with_election_tick(25)
        .with_heartbeat_tick(2)
        .with_max_size_per_msg(2048)
        .with_max_inflight_msgs(16);

    let cfg = builder.config();
    assert_eq!(cfg.cluster_id, 9);
    assert_eq!(cfg.election_tick, 25);
    assert_eq!(cfg.heartbeat_tick, 2);
    assert_eq!(cfg.max_size_per_msg, 2048);
    assert_eq!(cfg.max_inflight_msgs, 16);
    // Untouched fields keep their defaults.
    assert_eq!(cfg.node_id, 0);
}

/// `with_member` appends while `with_members` replaces the whole list.
#[test]
fn builder_with_member_appends_with_members_replaces() {
    let builder = CatgaRaftRuntimeBuilder::new()
        .with_member(2, "http://127.0.0.1:7100")
        .with_member(3u64, String::from("http://127.0.0.1:7200"));

    assert_eq!(
        builder.members(),
        &[
            (2, "http://127.0.0.1:7100".to_string()),
            (3, "http://127.0.0.1:7200".to_string()),
        ]
    );

    let builder = builder.with_members(vec![(9, "http://127.0.0.1:7900".to_string())]);
    assert_eq!(
        builder.members(),
        &[(9, "http://127.0.0.1:7900".to_string())],
        "with_members must replace existing members"
    );
}

/// `Clone` yields an independent copy and `Debug` is implemented.
#[test]
fn builder_clone_and_debug() {
    let builder = CatgaRaftRuntimeBuilder::from_cli(8300, 0, 2)
        .expect("from_cli should succeed")
        .with_member(9, "http://127.0.0.1:7900");

    let clone = builder.clone();
    assert_eq!(clone.config().node_id, builder.config().node_id);
    assert_eq!(clone.members(), builder.members());

    // Mutating the original must not affect the clone.
    let mutated = builder.with_cluster_id(55);
    assert_eq!(mutated.config().cluster_id, 55);
    assert_eq!(clone.config().cluster_id, 1);

    let dbg = format!("{:?}", clone);
    assert!(dbg.contains("CatgaRaftRuntimeBuilder"));
}

/// `with_topology` wires the self endpoint and the peer list in one call.
#[test]
fn builder_with_topology_sets_endpoint_and_members() {
    let builder = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_topology(
            "http://catga-raft-0.catga-raft-headless.default.svc.cluster.local:9100",
            vec![
                (
                    2,
                    "http://catga-raft-1.catga-raft-headless.default.svc.cluster.local:9100"
                        .to_string(),
                ),
                (
                    3,
                    "http://catga-raft-2.catga-raft-headless.default.svc.cluster.local:9100"
                        .to_string(),
                ),
            ],
        );

    assert_eq!(
        builder.self_endpoint(),
        Some("http://catga-raft-0.catga-raft-headless.default.svc.cluster.local:9100")
    );
    assert_eq!(builder.members().len(), 2);
    assert_eq!(builder.members()[0].0, 2);
    assert_eq!(builder.members()[1].0, 3);
}

/// A topology wired via `with_topology` starts a live runtime; port 0 lets the
/// gRPC server bind an ephemeral port so the test never collides with others.
#[tokio::test]
async fn builder_with_topology_starts_runtime() {
    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_topology(
            "http://127.0.0.1:0",
            vec![
                (2, "http://127.0.0.1:7100".to_string()),
                (3, "http://127.0.0.1:7200".to_string()),
            ],
        )
        .start(TestMachine::default())
        .await
        .expect("start should succeed");

    assert!(runtime.is_alive());
    assert_eq!(runtime.config().node_id, 1);

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join should succeed");
}

// ============================================================================
// start() validation
// ============================================================================

/// `start` must reject the default builder: `CatgaRaftConfig::default()`
/// leaves `node_id` at 0, which would boot a broken raft node.
#[tokio::test]
async fn builder_start_rejects_zero_node_id() {
    match CatgaRaftRuntimeBuilder::new()
        .start(TestMachine::default())
        .await
    {
        Err(CatgaRaftError::Raft(msg)) => {
            assert!(msg.contains("node_id"), "unexpected error message: {msg}");
        }
        Err(other) => panic!("expected Raft error for node_id 0, got {other:?}"),
        Ok(_) => panic!("expected error, but start succeeded"),
    }
}

/// An unparseable self endpoint must surface as a `Raft` error from `start`,
/// not a panic or a late transport failure.
#[tokio::test]
async fn builder_start_rejects_unparseable_self_endpoint() {
    for endpoint in ["http://no-port-here", "http://127.0.0.1:not-a-port"] {
        let result = CatgaRaftRuntimeBuilder::new()
            .with_config(CatgaRaftConfig {
                node_id: 1,
                cluster_id: 1,
                ..Default::default()
            })
            .with_self_endpoint(endpoint)
            .start(TestMachine::default())
            .await;
        match result {
            Err(CatgaRaftError::Raft(msg)) => {
                assert!(
                    msg.contains(endpoint),
                    "message should name the endpoint: {msg}"
                );
                assert!(
                    msg.contains("port"),
                    "message should mention the port: {msg}"
                );
            }
            Err(other) => panic!("expected Raft error for endpoint {endpoint:?}, got {other:?}"),
            Ok(_) => panic!("expected error for endpoint {endpoint:?}, but start succeeded"),
        }
    }
}

/// The member list must only contain peers: registering the own node id is a
/// configuration error.
#[tokio::test]
async fn builder_start_rejects_own_id_in_members() {
    let result = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 2,
            cluster_id: 1,
            ..Default::default()
        })
        .with_member(2, "http://127.0.0.1:7100")
        .start(TestMachine::default())
        .await;
    match result {
        Err(CatgaRaftError::Raft(msg)) => {
            assert!(
                msg.contains('2'),
                "message should name the offending id: {msg}"
            );
        }
        Err(other) => panic!("expected Raft error for self in members, got {other:?}"),
        Ok(_) => panic!("expected error, but start succeeded"),
    }
}

/// Duplicate member ids would corrupt the voter set; `start` must reject them.
#[tokio::test]
async fn builder_start_rejects_duplicate_member_ids() {
    let result = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_member(2, "http://127.0.0.1:7100")
        .with_member(2, "http://127.0.0.1:7200")
        .start(TestMachine::default())
        .await;
    match result {
        Err(CatgaRaftError::Raft(msg)) => {
            assert!(msg.contains("duplicate"), "unexpected error message: {msg}");
        }
        Err(other) => panic!("expected Raft error for duplicate member ids, got {other:?}"),
        Ok(_) => panic!("expected error, but start succeeded"),
    }
}

// ============================================================================
// start() / start_default()
// ============================================================================

/// `start` produces a live runtime whose config and coordinator reflect the builder.
#[tokio::test]
async fn builder_start_single_node_runtime() {
    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 7,
            cluster_id: 9,
            ..Default::default()
        })
        .start(TestMachine::default())
        .await
        .expect("start should succeed");

    assert!(runtime.is_alive());
    assert_eq!(runtime.config().node_id, 7);
    assert_eq!(runtime.config().cluster_id, 9);

    let coord = runtime.coordinator();
    assert_eq!(coord.node_id(), "node-7", "coordinator id derives from node_id");
    assert!(!coord.is_leader());
    assert!(coord.member_endpoints().is_empty());

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join should succeed");
}

/// Member endpoints registered on the builder reach the coordinator on start.
#[tokio::test]
async fn builder_start_propagates_members_to_coordinator() {
    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_member(2, "http://127.0.0.1:7100")
        .with_member(3, "http://127.0.0.1:7200")
        .start(TestMachine::default())
        .await
        .expect("start should succeed");

    let endpoints: Vec<String> = runtime
        .coordinator()
        .member_endpoints()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        endpoints,
        vec![
            "http://127.0.0.1:7100".to_string(),
            "http://127.0.0.1:7200".to_string(),
        ],
        "builder members must be visible to the coordinator"
    );

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join should succeed");
}

/// Pipeline convenience setters chain and the resulting runtime can accept proposals.
#[tokio::test]
async fn builder_pipeline_setters_produce_running_runtime() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(8300, 0, 1)
        .expect("from_cli should succeed")
        .with_pipeline_config(PipelineConfig {
            batch_size: 4,
            flush_interval: Duration::from_millis(2),
            max_inflight: 32,
        })
        .with_batch_size(2)
        .with_flush_interval(Duration::from_millis(5))
        .with_max_inflight(16)
        .start(TestMachine::default())
        .await
        .expect("start should succeed");

    assert!(runtime.is_alive());

    // The pipeline must be started: proposing as leader goes through it.
    runtime.set_leader(Some("http://127.0.0.1:8300".to_string()));
    runtime
        .propose(vec![1, 2, 3])
        .await
        .expect("propose should succeed as leader");

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join should succeed");
}

/// `start_default` constructs the state machine via `Default` without consuming the builder.
#[tokio::test]
async fn builder_start_default_constructs_machine() {
    let builder = CatgaRaftRuntimeBuilder::from_cli(8300, 0, 1).expect("from_cli should succeed");

    let runtime = builder
        .start_default::<TestMachine>()
        .await
        .expect("start_default should succeed");

    assert!(runtime.is_alive());
    assert_eq!(runtime.config().node_id, 1);
    assert_eq!(runtime.applied_index().await.expect("applied_index"), 0);

    // The builder was borrowed, not consumed, so it is still usable.
    assert_eq!(builder.config().node_id, 1);

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join should succeed");
}
