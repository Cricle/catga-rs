//! Integration tests for `src/config.rs`.
//!
//! `config.rs` defines two plain data structs, `CatgaRaftConfig` and
//! `PipelineConfig`, both re-exported at the crate root. These tests cover:
//! - `Default` values for both configs
//! - The `CatgaRaftConfig::for_cluster_size` election-tick scaling helper
//! - Construction / mutation / `Clone` behavior (both derive `Clone` + `Debug`)
//! - The configs flowing through public consumers:
//!   - `CatgaRaftRuntimeBuilder::{with_config, config, with_*}` (CatgaRaftConfig)
//!   - `PipelineManager::new(PipelineConfig)` driving batching/backpressure
//!
//! Note: `pre_vote`/`check_quorum` are deliberately *not* fields of
//! `CatgaRaftConfig`; they are raft::Config-level knobs hard-enabled in
//! `node::RaftNode::new`, and their effect is asserted in
//! `tests/node_tests.rs` against the underlying raft core.

use std::time::Duration;

use catga_raft::{CatgaRaftConfig, CatgaRaftRuntimeBuilder, PipelineConfig, PipelineManager};

// ============================================================================
// CatgaRaftConfig: defaults and construction
// ============================================================================

/// `CatgaRaftConfig::default()` must expose the documented default values,
/// and the defaults must form a sensible Raft relationship: heartbeats fire
/// more often than election timeouts, and limits are non-zero.
#[test]
fn config_catga_default_field_values() {
    let cfg = CatgaRaftConfig::default();
    assert_eq!(cfg.node_id, 0);
    assert_eq!(cfg.cluster_id, 0);
    assert_eq!(cfg.election_tick, 10);
    assert_eq!(cfg.heartbeat_tick, 3);
    // Aligned with the tonic server decode limit in `transport::server`.
    assert_eq!(cfg.max_size_per_msg, 8 * 1024 * 1024);
    assert_eq!(cfg.max_inflight_msgs, 256);

    // Tick/limit invariants implied by the defaults.
    assert!(cfg.heartbeat_tick >= 1);
    assert!(cfg.heartbeat_tick < cfg.election_tick);
    assert!(cfg.max_size_per_msg > 0);
    assert!(cfg.max_inflight_msgs > 0);
}

/// Struct-literal construction with the functional-update (`..Default::default()`)
/// pattern, as used by `CatgaRaftRuntimeBuilder::from_cli`.
#[test]
fn config_catga_struct_update_syntax() {
    let cfg = CatgaRaftConfig {
        node_id: 7,
        cluster_id: 42,
        ..Default::default()
    };
    assert_eq!(cfg.node_id, 7);
    assert_eq!(cfg.cluster_id, 42);
    // Untouched fields keep their defaults.
    assert_eq!(cfg.election_tick, 10);
    assert_eq!(cfg.heartbeat_tick, 3);
    assert_eq!(cfg.max_size_per_msg, 8 * 1024 * 1024);
    assert_eq!(cfg.max_inflight_msgs, 256);
}

/// All fields are public and mutable; `Clone` produces an identical copy
/// (no `PartialEq` impl, so compare field by field).
#[test]
fn config_catga_mutation_and_clone() {
    let mut cfg = CatgaRaftConfig::default();
    cfg.node_id = 3;
    cfg.cluster_id = 9;
    cfg.election_tick = 20;
    cfg.heartbeat_tick = 5;
    cfg.max_size_per_msg = 1024;
    cfg.max_inflight_msgs = 8;

    let cloned = cfg.clone();
    assert_eq!(cloned.node_id, 3);
    assert_eq!(cloned.cluster_id, 9);
    assert_eq!(cloned.election_tick, 20);
    assert_eq!(cloned.heartbeat_tick, 5);
    assert_eq!(cloned.max_size_per_msg, 1024);
    assert_eq!(cloned.max_inflight_msgs, 8);

    // Mutating the clone must not affect the original.
    let mut other = cfg.clone();
    other.node_id = 99;
    assert_eq!(cfg.node_id, 3);

    // Debug derive must not panic.
    let _ = format!("{:?}", cfg);
}

/// The config has no built-in validation: extreme values are accepted as-is.
/// Documenting this edge case keeps the contract explicit.
#[test]
fn config_catga_accepts_extreme_values() {
    let cfg = CatgaRaftConfig {
        node_id: u64::MAX,
        cluster_id: u64::MAX,
        election_tick: 0,
        heartbeat_tick: usize::MAX,
        max_size_per_msg: u64::MAX,
        max_inflight_msgs: usize::MAX,
    };
    assert_eq!(cfg.node_id, u64::MAX);
    assert_eq!(cfg.election_tick, 0);
    assert_eq!(cfg.heartbeat_tick, usize::MAX);
}

// ============================================================================
// CatgaRaftConfig::for_cluster_size (election tuning helper)
// ============================================================================

/// `for_cluster_size` scales `election_tick` with cluster size and leaves
/// every other field at its default. Wider randomized election windows
/// (`[election_tick, 2 * election_tick)`) reduce the probability that many
/// followers time out simultaneously and split votes (election storms).
#[test]
fn config_catga_for_cluster_size_scaling_table() {
    // (cluster size, expected election_tick)
    let cases: &[(u64, usize)] = &[
        (0, 10), // degenerate size keeps the default
        (1, 10),
        (3, 10),
        (5, 10), // boundary: <=5 keeps the default
        (6, 20),
        (10, 20),
        (20, 20), // boundary: <=20
        (21, 30),
        (50, 30),  // the audit's election-storm cluster size
        (100, 30), // boundary: <=100
        (101, 40),
        (1000, 40),
        (u64::MAX, 40),
    ];
    for &(n, expected) in cases {
        let cfg = CatgaRaftConfig::for_cluster_size(n);
        assert_eq!(
            cfg.election_tick, expected,
            "for_cluster_size({n}) must set election_tick = {expected}"
        );
        // Everything else must be the untouched default.
        assert_eq!(cfg.node_id, 0, "node_id stays default for size {n}");
        assert_eq!(cfg.cluster_id, 0, "cluster_id stays default for size {n}");
        assert_eq!(
            cfg.heartbeat_tick, 3,
            "heartbeat_tick stays default for size {n}"
        );
        assert_eq!(cfg.max_size_per_msg, 8 * 1024 * 1024);
        assert_eq!(cfg.max_inflight_msgs, 256);
    }
}

/// Every scaled config must still satisfy raft's own invariant
/// (`election_tick > heartbeat_tick`, checked by raft::Config::validate),
/// so the helper's output can flow straight into `RaftNode::new`.
#[test]
fn config_catga_for_cluster_size_satisfies_raft_invariants() {
    for n in [1u64, 5, 6, 20, 21, 100, 101, 500] {
        let cfg = CatgaRaftConfig::for_cluster_size(n);
        assert!(
            cfg.election_tick > cfg.heartbeat_tick,
            "election_tick must stay greater than heartbeat_tick for size {n}"
        );
    }
}

/// The helper is opt-in and pure: it must not mutate the `Default` impl,
/// and repeated calls must be deterministic.
#[test]
fn config_catga_for_cluster_size_does_not_alter_defaults() {
    let _ = CatgaRaftConfig::for_cluster_size(100);
    let plain = CatgaRaftConfig::default();
    // Defaults unchanged (tests above and CI depend on these exact values).
    assert_eq!(plain.election_tick, 10);
    assert_eq!(plain.heartbeat_tick, 3);
    // Deterministic: same input, same output.
    let a = CatgaRaftConfig::for_cluster_size(50);
    let b = CatgaRaftConfig::for_cluster_size(50);
    assert_eq!(a.election_tick, b.election_tick);
}

/// The scaled config can be applied through the builder setters.
#[test]
fn config_catga_for_cluster_size_through_builder() {
    let mut cfg = CatgaRaftConfig::for_cluster_size(50);
    cfg.node_id = 1;
    cfg.cluster_id = 7;
    let builder = CatgaRaftRuntimeBuilder::new().with_config(cfg);
    assert_eq!(builder.config().election_tick, 30);
    assert_eq!(builder.config().node_id, 1);
    assert_eq!(builder.config().cluster_id, 7);
}

// ============================================================================
// CatgaRaftConfig through the public builder API
// ============================================================================

/// `CatgaRaftRuntimeBuilder::new()` starts from `CatgaRaftConfig::default()`.
#[test]
fn config_catga_builder_starts_from_default() {
    let builder = CatgaRaftRuntimeBuilder::new();
    let cfg = builder.config();
    assert_eq!(cfg.node_id, 0);
    assert_eq!(cfg.cluster_id, 0);
    assert_eq!(cfg.election_tick, 10);
    assert_eq!(cfg.heartbeat_tick, 3);
    assert_eq!(cfg.max_size_per_msg, 8 * 1024 * 1024);
    assert_eq!(cfg.max_inflight_msgs, 256);
}

/// `with_config` stores the exact config supplied.
#[test]
fn config_catga_builder_with_config_round_trip() {
    let custom = CatgaRaftConfig {
        node_id: 5,
        cluster_id: 77,
        election_tick: 15,
        heartbeat_tick: 2,
        max_size_per_msg: 4096,
        max_inflight_msgs: 32,
    };
    let builder = CatgaRaftRuntimeBuilder::new().with_config(custom);
    let cfg = builder.config();
    assert_eq!(cfg.node_id, 5);
    assert_eq!(cfg.cluster_id, 77);
    assert_eq!(cfg.election_tick, 15);
    assert_eq!(cfg.heartbeat_tick, 2);
    assert_eq!(cfg.max_size_per_msg, 4096);
    assert_eq!(cfg.max_inflight_msgs, 32);
}

/// Each field-level builder setter mutates exactly one config field.
#[test]
fn config_catga_builder_field_setters() {
    let builder = CatgaRaftRuntimeBuilder::new()
        .with_cluster_id(123)
        .with_election_tick(30)
        .with_heartbeat_tick(7)
        .with_max_size_per_msg(2048)
        .with_max_inflight_msgs(64);

    let cfg = builder.config();
    assert_eq!(cfg.cluster_id, 123);
    assert_eq!(cfg.election_tick, 30);
    assert_eq!(cfg.heartbeat_tick, 7);
    assert_eq!(cfg.max_size_per_msg, 2048);
    assert_eq!(cfg.max_inflight_msgs, 64);
    // Untouched fields remain at their defaults.
    assert_eq!(cfg.node_id, 0);
}

// ============================================================================
// PipelineConfig: defaults and construction
// ============================================================================

/// `PipelineConfig::default()` must expose the documented default values.
#[test]
fn config_pipeline_default_field_values() {
    let cfg = PipelineConfig::default();
    assert_eq!(cfg.batch_size, 64);
    assert_eq!(cfg.flush_interval, Duration::from_millis(1));
    assert_eq!(cfg.max_inflight, 1024);
}

/// Public fields are mutable; `Clone`/`Debug` work as derived.
#[test]
fn config_pipeline_mutation_clone_debug() {
    let mut cfg = PipelineConfig::default();
    cfg.batch_size = 1;
    cfg.flush_interval = Duration::from_secs(60);
    cfg.max_inflight = 2;

    let cloned = cfg.clone();
    assert_eq!(cloned.batch_size, 1);
    assert_eq!(cloned.flush_interval, Duration::from_secs(60));
    assert_eq!(cloned.max_inflight, 2);

    // Debug derive must not panic.
    let _ = format!("{:?}", cloned);
}

/// Edge case: the struct accepts a zero flush interval; validation, if any,
/// is the responsibility of consumers. We only construct here — never start a
/// `PipelineManager` with it, since `tokio::time::interval` requires a
/// positive period.
#[test]
fn config_pipeline_zero_flush_interval_accepted() {
    let cfg = PipelineConfig {
        batch_size: 0,
        flush_interval: Duration::ZERO,
        max_inflight: 0,
    };
    assert_eq!(cfg.batch_size, 0);
    assert_eq!(cfg.flush_interval, Duration::ZERO);
    assert_eq!(cfg.max_inflight, 0);
}

// ============================================================================
// PipelineConfig through PipelineManager (public consumer)
// ============================================================================

/// `batch_size = 1` forces a synchronous flush on every propose, so the
/// pending queue must be empty immediately after `propose` returns.
#[tokio::test]
async fn config_pipeline_batch_size_one_flushes_immediately() {
    let cfg = PipelineConfig {
        batch_size: 1,
        // Long interval: the background flusher must not be what empties the
        // queue within this test's timeframe.
        flush_interval: Duration::from_secs(60),
        max_inflight: 64,
    };
    let manager = PipelineManager::new(cfg);
    manager.start();

    manager.propose(b"entry".to_vec()).unwrap();
    assert_eq!(manager.pending_count(), 0);

    manager.stop();
}

/// Edge case: `max_inflight = 0` must reject proposals with `Timeout` because
/// the in-flight limit check happens before queueing.
#[tokio::test]
async fn config_pipeline_zero_max_inflight_rejects_propose() {
    let cfg = PipelineConfig {
        batch_size: 64,
        flush_interval: Duration::from_secs(60),
        max_inflight: 0,
    };
    let manager = PipelineManager::new(cfg);
    manager.start();

    let result = manager.propose(b"entry".to_vec());
    assert!(result.is_err());
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 0);

    manager.stop();
}

/// Error case: a manager built from a custom config but never started must
/// reject proposals as `NotLeader`.
#[tokio::test]
async fn config_pipeline_not_started_rejects_propose() {
    let cfg = PipelineConfig {
        batch_size: 8,
        flush_interval: Duration::from_millis(10),
        max_inflight: 16,
    };
    let manager = PipelineManager::new(cfg);

    let result = manager.propose(b"entry".to_vec());
    assert!(result.is_err());
    assert_eq!(manager.pending_count(), 0);
}

/// `PipelineManager::default()` is defined as `new(PipelineConfig::default())`
/// and must start in a clean state.
#[test]
fn config_pipeline_manager_default_initial_state() {
    let manager = PipelineManager::default();
    assert_eq!(manager.pending_count(), 0);
    assert_eq!(manager.inflight_count(), 0);

    // `batch_completed` uses saturating subtraction: completing more than is
    // in flight must not underflow.
    manager.batch_completed(10);
    assert_eq!(manager.inflight_count(), 0);
}
