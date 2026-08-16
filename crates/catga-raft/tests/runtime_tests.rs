//! Integration tests for `crates/catga-raft/src/runtime.rs` (`CatgaRaftRuntime`).
//!
//! `CatgaRaftRuntime` is re-exported at the crate root and implements the
//! backend-agnostic `catga_core::ConsensusRuntime` trait. These tests exercise
//! the runtime's public surface:
//!
//! - Direct construction (`CatgaRaftRuntime::new`) and `new_for_test`
//! - Component accessors (`pipeline`, `apply`, `coordinator`, `config`)
//! - `propose` leadership gating and error mapping (`Unavailable`, `TransportFailed`)
//! - `applied_index` delegation to the `ApplyThread`
//! - Membership stubs (`add_member` / `remove_member`)
//! - Shutdown / join lifecycle and the `is_shutdown_requested` flag
//! - The default `propose_and_wait` implementation (timeout + success paths)
//!
//! No network ports are bound: proposals stay inside the in-process
//! `PipelineManager` and member endpoints are plain strings.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use catga_core::{
    CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine, ErrorCode,
};
use catga_raft::{
    ApplyThread, CatgaRaftConfig, CatgaRaftCoordinator, CatgaRaftRuntime, CatgaRaftRuntimeBuilder,
    PipelineConfig, PipelineManager,
};

/// A test state machine that records applied entries in order.
#[derive(Default)]
struct TestMachine {
    applied: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl TestMachine {
    fn new() -> Self {
        Self::default()
    }

    fn applied_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.applied.lock().unwrap().clone()
    }
}

impl ConsensusStateMachine for TestMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().unwrap().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _data: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

/// Starts a runtime through the public builder (pipeline already running).
async fn start_runtime(node_id: u64) -> CatgaRaftRuntime<TestMachine> {
    let config = CatgaRaftConfig {
        node_id,
        cluster_id: 1,
        ..Default::default()
    };
    CatgaRaftRuntimeBuilder::new()
        .with_config(config)
        .start(TestMachine::new())
        .await
        .expect("builder start should succeed")
}

// ============================================================================
// Construction and accessors
// ============================================================================

/// Direct construction wires the components and exposes sane defaults.
#[test]
fn runtime_new_defaults_and_accessors() {
    let config = CatgaRaftConfig {
        node_id: 5,
        cluster_id: 9,
        ..Default::default()
    };
    let coordinator = CatgaRaftCoordinator::new("node-5".to_string());
    coordinator.set_members(vec!["ep-a".to_string(), "ep-b".to_string()]);
    let pipeline = PipelineManager::new(PipelineConfig::default());
    let apply = ApplyThread::new(TestMachine::new());

    let runtime = CatgaRaftRuntime::new(pipeline, apply, coordinator, config);

    // Alive and no shutdown requested right after construction.
    assert!(runtime.is_alive(), "fresh runtime must be alive");
    assert!(!runtime.is_shutdown_requested());

    // Configuration is preserved.
    assert_eq!(runtime.config().node_id, 5);
    assert_eq!(runtime.config().cluster_id, 9);

    // Component accessors reflect the constructed state.
    assert_eq!(runtime.coordinator().node_id(), "node-5");
    assert!(!runtime.coordinator().is_leader());
    assert_eq!(runtime.coordinator().member_endpoints().len(), 2);
    assert_eq!(runtime.pipeline().pending_count(), 0);
    assert_eq!(runtime.pipeline().inflight_count(), 0);
    assert_eq!(runtime.apply().applied_index(), 0);
}

/// `new_for_test` builds a minimal runtime with default config values.
#[test]
fn runtime_new_for_test_minimal_runtime() {
    let runtime = CatgaRaftRuntime::<TestMachine>::new_for_test(7);

    assert_eq!(runtime.config().node_id, 7);
    assert_eq!(runtime.config().cluster_id, 0);
    assert_eq!(runtime.config().election_tick, 10);
    assert_eq!(runtime.config().heartbeat_tick, 3);
    assert_eq!(runtime.coordinator().node_id(), "node-7");
    assert!(!runtime.coordinator().is_leader());
    assert!(
        runtime.coordinator().member_endpoints().is_empty(),
        "minimal runtime has no members"
    );
    assert!(runtime.is_alive());
    assert!(!runtime.is_shutdown_requested());
}

// ============================================================================
// Propose: leadership gating and error mapping
// ============================================================================

/// Proposing on a non-leader fails fast with `ErrorCode::Unavailable`.
#[tokio::test]
async fn runtime_propose_not_leader_returns_unavailable() {
    let runtime = start_runtime(1).await;

    assert!(!runtime.coordinator().is_leader());
    let err = runtime
        .propose(b"payload".to_vec())
        .await
        .expect_err("propose must fail when not leader");
    assert_eq!(err.code(), ErrorCode::Unavailable);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// As leader with a running pipeline, proposals are accepted fire-and-forget.
#[tokio::test]
async fn runtime_propose_as_leader_accepts_entries() {
    let runtime = start_runtime(1).await;
    runtime.set_leader(Some("http://self-endpoint".to_string()));
    assert!(runtime.coordinator().is_leader());

    runtime
        .propose(b"first".to_vec())
        .await
        .expect("first propose should succeed as leader");
    runtime
        .propose(vec![1, 2, 3])
        .await
        .expect("second propose should succeed as leader");

    // Proposals are fire-and-forget: nothing is applied without Raft progress.
    assert_eq!(runtime.applied_index().await.expect("applied_index ok"), 0);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// `new_for_test` uses an unstarted pipeline: a leader proposal fails and the
/// pipeline error is mapped to `ErrorCode::TransportFailed`.
#[tokio::test]
async fn runtime_new_for_test_leader_propose_fails_pipeline_not_running() {
    let runtime = CatgaRaftRuntime::<TestMachine>::new_for_test(3);

    // Not leader yet: leadership check fires first.
    let err = runtime
        .propose(b"x".to_vec())
        .await
        .expect_err("propose must fail without leadership");
    assert_eq!(err.code(), ErrorCode::Unavailable);

    // Leader, but the default pipeline was never started.
    runtime.set_leader(Some("self".to_string()));
    let err = runtime
        .propose(b"x".to_vec())
        .await
        .expect_err("propose must fail when pipeline is not running");
    assert_eq!(err.code(), ErrorCode::TransportFailed);
}

/// Stopping the pipeline after startup maps proposal errors to
/// `ErrorCode::TransportFailed` as well.
#[tokio::test]
async fn runtime_propose_after_pipeline_stop_maps_transport_failed() {
    let runtime = start_runtime(2).await;
    runtime.set_leader(Some("http://self-endpoint".to_string()));

    // Sanity: leader with a running pipeline accepts a proposal.
    runtime
        .propose(b"warm-up".to_vec())
        .await
        .expect("warm-up ok");

    runtime.pipeline().stop();
    let err = runtime
        .propose(b"late".to_vec())
        .await
        .expect_err("propose must fail after pipeline stop");
    assert_eq!(err.code(), ErrorCode::TransportFailed);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Applied index delegation
// ============================================================================

/// `applied_index` mirrors the ApplyThread's progress; `set_applied_index`
/// is a documented no-op.
#[tokio::test]
async fn runtime_applied_index_tracks_apply_thread() {
    let runtime = start_runtime(1).await;

    assert_eq!(runtime.applied_index().await.expect("applied_index ok"), 0);

    runtime
        .apply()
        .apply_entry(3, b"three")
        .expect("apply_entry ok");
    let advanced = runtime
        .apply()
        .advance(vec![(4, b"four".to_vec()), (5, b"five".to_vec())].into_iter())
        .expect("advance ok");
    assert_eq!(advanced, 5);

    assert_eq!(runtime.applied_index().await.expect("applied_index ok"), 5);

    // The state machine observed the entries in order.
    let entries = runtime.apply().state_machine().lock().applied_entries();
    assert_eq!(
        entries,
        vec![
            (3, b"three".to_vec()),
            (4, b"four".to_vec()),
            (5, b"five".to_vec()),
        ]
    );

    // `set_applied_index` is intentionally a no-op in the current design.
    runtime.set_applied_index(99);
    assert_eq!(runtime.applied_index().await.expect("applied_index ok"), 5);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Membership changes and coordinator view
// ============================================================================

/// Membership-change requests are routed through the owner loop. A node that
/// is not the leader rejects them with `NotLeader` (mapped to `Unavailable`)
/// and leaves the coordinator's member list untouched. The self endpoint sits
/// on an isolated port and the single peer is unreachable, so this node can
/// never gather a quorum — the rejection is deterministic.
#[tokio::test]
async fn runtime_membership_not_leader_returns_unavailable() {
    let runtime = CatgaRaftRuntimeBuilder::new()
        .with_config(CatgaRaftConfig {
            node_id: 1,
            cluster_id: 1,
            ..Default::default()
        })
        .with_self_endpoint("http://127.0.0.1:19905")
        .with_member(2, "http://peer-2")
        .start(TestMachine::new())
        .await
        .expect("start should succeed");

    let add_err = runtime
        .add_member(3, "http://peer-3".to_string())
        .await
        .expect_err("add_member must fail when not leader");
    assert_eq!(add_err.code(), ErrorCode::Unavailable);
    assert!(add_err.message().contains("not leader"));

    let remove_err = runtime
        .remove_member(2)
        .await
        .expect_err("remove_member must fail when not leader");
    assert_eq!(remove_err.code(), ErrorCode::Unavailable);
    assert!(remove_err.message().contains("not leader"));

    // Rejected proposals must not mutate the membership view.
    let members = runtime.coordinator().member_endpoints();
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].as_ref(), "http://peer-2");

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

/// `set_leader` updates the view observable through the trait-object
/// coordinator returned by `ConsensusRuntime::coordinator`.
#[tokio::test]
async fn runtime_set_leader_visible_through_trait_coordinator() {
    let runtime = start_runtime(1).await;
    let coord: Arc<dyn ConsensusCoordinator> = ConsensusRuntime::coordinator(&runtime);

    assert_eq!(coord.node_id(), "node-1");
    assert!(!coord.is_leader());
    assert!(coord.leader_endpoint().is_none());

    runtime.set_leader(Some("http://leader-endpoint".to_string()));
    assert!(coord.is_leader());
    assert_eq!(
        coord.leader_endpoint().map(|s| s.to_string()),
        Some("http://leader-endpoint".to_string())
    );

    runtime.set_leader(None);
    assert!(!coord.is_leader());
    assert!(coord.leader_endpoint().is_none());

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Shutdown / join lifecycle
// ============================================================================

/// The shutdown flag is sticky, `shutdown` is idempotent, and `join`
/// completes successfully, after which the runtime reports not alive.
#[tokio::test]
async fn runtime_shutdown_flag_is_sticky_and_join_completes() {
    let runtime = start_runtime(1).await;

    assert!(!runtime.is_shutdown_requested());
    runtime.shutdown();
    assert!(runtime.is_shutdown_requested());

    // Idempotent: a second request keeps the flag set.
    runtime.shutdown();
    assert!(runtime.is_shutdown_requested());

    // Still reports alive until joined (owner task semantics).
    assert!(runtime.is_alive());

    Box::new(runtime).join().await.expect("join should succeed");
    // Runtime is consumed by join; nothing further to observe.
}

// ============================================================================
// `propose_and_wait` without an owner loop
// ============================================================================

/// The attributed `propose_and_wait` override needs the owner loop to match
/// committed entry contexts; a bare runtime (built without the builder, so
/// no owner-loop channels) reports that honestly instead of hanging.
#[tokio::test]
async fn runtime_propose_and_wait_without_owner_loop_is_unavailable() {
    let runtime = CatgaRaftRuntime::<TestMachine>::new_for_test(1);
    runtime.set_leader(Some("http://self-endpoint".to_string()));

    let err = runtime
        .propose_and_wait(b"stuck".to_vec(), Duration::from_millis(60))
        .await
        .expect_err("must fail without an owner loop");
    assert_eq!(err.code(), ErrorCode::Unavailable);

    runtime.shutdown();
    let _ = Box::new(runtime).join().await;
}

// ============================================================================
// Thread-safety: the runtime handle is shareable
// ============================================================================

/// Compile-time proof that the runtime handle can cross threads/tasks.
#[test]
fn runtime_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CatgaRaftRuntime<TestMachine>>();
}

/// Multiple tasks can share one runtime handle and propose concurrently.
#[tokio::test]
async fn runtime_shareable_across_tasks_for_concurrent_proposals() {
    let runtime = Arc::new(start_runtime(1).await);
    runtime.set_leader(Some("http://self-endpoint".to_string()));

    let mut handles = Vec::new();
    for task in 0..4u64 {
        let rt = Arc::clone(&runtime);
        handles.push(tokio::spawn(async move {
            let mut results = Vec::new();
            for i in 0..10u64 {
                let payload = format!("task-{task}-entry-{i}").into_bytes();
                results.push(rt.propose(payload).await);
            }
            results
        }));
    }

    for handle in handles {
        for result in handle.await.expect("task should not panic") {
            result.expect("every concurrent propose should be accepted");
        }
    }

    runtime.shutdown();
    assert!(runtime.is_shutdown_requested());

    // All task clones are dropped by now, so ownership can be reclaimed and
    // the runtime joined through the normal lifecycle path.
    let runtime = match Arc::try_unwrap(runtime) {
        Ok(runtime) => runtime,
        Err(_) => panic!("all shared clones dropped"),
    };
    Box::new(runtime).join().await.expect("join should succeed");
}
