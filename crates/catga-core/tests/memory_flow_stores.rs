//! Strict contract tests for the process-local flow support stores: durable
//! flow state with staleness-based claiming, DSL step progress versions,
//! state-machine snapshots, projection checkpoints, and persistent
//! subscriptions with competing-consumer leases.

use std::time::Duration;

use catga_core::flow::dsl_progress::{DslStepProgress, DslStepProgressStore};
use catga_core::flow::state_machine::{StateMachineSnapshot, StateMachineStore};
use catga_core::flow::{FlowState, FlowStore};
use catga_core::memory::{
    MemoryDslStepProgress, MemoryFlows, MemoryProjectionCheckpoints, MemoryStateMachines,
    MemorySubscriptions,
};
use catga_core::{
    PersistentSubscription, ProjectionCheckpoint, ProjectionCheckpointStore,
    SubscriptionCheckpoint, SubscriptionStore,
};

// ---------------------------------------------------------------------------
// MemoryFlows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flows_enforce_optimistic_versions_and_staleness_claims() {
    let flows = MemoryFlows::default();

    let state = FlowState::new("f-1", "checkout", [], "worker-a");
    assert!(flows.create(state.clone()).await.expect("create succeeds"));
    assert!(
        !flows
            .create(FlowState::new("f-1", "checkout", [], "worker-a"))
            .await
            .expect("create succeeds"),
        "a duplicate flow identifier is refused"
    );

    let loaded = flows
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("flow retained");
    assert_eq!(loaded.flow_type(), "checkout");
    assert_eq!(loaded.version(), 0);
    assert!(flows.get("missing").await.expect("get succeeds").is_none());

    // Updates require the exact version successor.
    let next = loaded.next_version().expect("version advances");
    assert!(
        !flows
            .update(3, next.clone())
            .await
            .expect("update succeeds"),
        "a non-successor version is refused"
    );
    assert!(
        flows
            .update(0, next.clone())
            .await
            .expect("update succeeds")
    );
    assert!(
        !flows
            .update(0, next.clone())
            .await
            .expect("update succeeds"),
        "a stale expected version is refused"
    );
    let orphan = FlowState::new("missing", "checkout", [], "w")
        .next_version()
        .expect("version advances");
    assert!(
        !flows.update(0, orphan).await.expect("update succeeds"),
        "a missing flow refuses the update"
    );
}

#[tokio::test]
async fn flows_claim_only_stale_running_flows_and_heartbeat_by_owner() {
    let flows = MemoryFlows::default();
    flows
        .create(FlowState::new("f-1", "checkout", [], "worker-a"))
        .await
        .expect("create succeeds");
    flows
        .create(FlowState::new("f-2", "refund", [], "worker-a"))
        .await
        .expect("create succeeds");

    // A zero staleness bound makes every running flow claimable by type.
    let claimed = flows
        .try_claim("refund", "worker-b", Duration::ZERO)
        .await
        .expect("claim succeeds")
        .expect("a stale running flow claims");
    assert_eq!(claimed.id(), "f-2");
    assert_eq!(claimed.owner(), Some("worker-b"));
    assert_eq!(claimed.version(), 1);

    // The claimed flow has a fresh heartbeat, so it is no longer stale.
    assert!(
        flows
            .try_claim("refund", "worker-c", Duration::from_secs(3600))
            .await
            .expect("claim succeeds")
            .is_none()
    );
    assert!(
        flows
            .try_claim("missing-type", "worker-c", Duration::ZERO)
            .await
            .expect("claim succeeds")
            .is_none()
    );

    // Heartbeats require the current owner and version.
    assert!(
        !flows
            .heartbeat("f-2", "worker-a", 1)
            .await
            .expect("heartbeat succeeds"),
        "a former owner refuses the heartbeat"
    );
    assert!(
        flows
            .heartbeat("f-2", "worker-b", 1)
            .await
            .expect("heartbeat succeeds")
    );
    assert!(
        !flows
            .heartbeat("f-2", "worker-b", 0)
            .await
            .expect("heartbeat succeeds"),
        "a stale version refuses the heartbeat"
    );
    assert!(
        !flows
            .heartbeat("missing", "worker-b", 1)
            .await
            .expect("heartbeat succeeds")
    );
}

// ---------------------------------------------------------------------------
// MemoryDslStepProgress
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dsl_step_progress_tracks_versioned_step_payloads() {
    let store = MemoryDslStepProgress::default();

    let progress = DslStepProgress::new("f-1", 2, [1_u8, 2]);
    assert_eq!(progress.version(), 0);
    assert!(
        store
            .create(progress.clone())
            .await
            .expect("create succeeds")
    );
    assert!(
        !store.create(progress).await.expect("create succeeds"),
        "a duplicate step is refused"
    );

    let loaded = store
        .get("f-1", 2)
        .await
        .expect("get succeeds")
        .expect("progress retained");
    assert_eq!(loaded.flow_id(), "f-1");
    assert_eq!(loaded.step_index(), 2);
    assert_eq!(loaded.payload(), &[1, 2]);

    // Updates require the exact version successor and an existing record.
    let next = loaded.next_version([3_u8]).expect("version advances");
    assert!(
        !store
            .update(5, next.clone())
            .await
            .expect("update succeeds"),
        "a non-successor expected version is refused"
    );
    assert!(store.update(0, next).await.expect("update succeeds"));
    assert_eq!(
        store
            .get("f-1", 2)
            .await
            .expect("get succeeds")
            .expect("progress retained")
            .payload(),
        &[3]
    );
    let orphan = DslStepProgress::new("f-2", 0, [9_u8])
        .next_version([8_u8])
        .expect("version advances");
    assert!(
        !store.update(0, orphan).await.expect("update succeeds"),
        "a missing record refuses the update"
    );

    assert!(store.delete("f-1", 2).await.expect("delete succeeds"));
    assert!(!store.delete("f-1", 2).await.expect("delete succeeds"));
    assert!(store.get("f-1", 2).await.expect("get succeeds").is_none());
}

// ---------------------------------------------------------------------------
// MemoryStateMachines
// ---------------------------------------------------------------------------

#[tokio::test]
async fn state_machines_snapshot_versions_with_cas_updates() {
    let store = MemoryStateMachines::<String>::default();

    let snapshot = StateMachineSnapshot::new("m-1", "idle".to_string());
    assert_eq!(snapshot.version(), 0);
    assert!(
        store
            .create(snapshot.clone())
            .await
            .expect("create succeeds")
    );
    assert!(
        !store
            .create(StateMachineSnapshot::new("m-1", "dup".to_string()))
            .await
            .expect("create succeeds"),
        "a duplicate instance is refused"
    );

    let loaded = store
        .get("m-1")
        .await
        .expect("get succeeds")
        .expect("snapshot retained");
    assert_eq!(loaded.state(), "idle");
    assert!(store.get("missing").await.expect("get succeeds").is_none());

    // Updates require the exact version successor and an existing instance.
    let next = loaded
        .next_version("running".to_string())
        .expect("advances");
    assert!(
        !store
            .update(3, next.clone())
            .await
            .expect("update succeeds"),
        "a non-successor expected version is refused"
    );
    assert!(store.update(0, next).await.expect("update succeeds"));
    assert_eq!(
        store
            .get("m-1")
            .await
            .expect("get succeeds")
            .expect("snapshot retained")
            .state(),
        "running"
    );
    let orphan = StateMachineSnapshot::new("missing", "x".to_string())
        .next_version("y".to_string())
        .expect("advances");
    assert!(
        !store.update(0, orphan).await.expect("update succeeds"),
        "a missing instance refuses the update"
    );
}

// ---------------------------------------------------------------------------
// MemoryProjectionCheckpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn projection_checkpoints_are_scoped_per_projection() {
    let store = MemoryProjectionCheckpoints::default();

    store
        .save(ProjectionCheckpoint::new("totals", "s-1", 4))
        .await
        .expect("save succeeds");
    store
        .save(ProjectionCheckpoint::new("totals", "s-2", 7))
        .await
        .expect("save succeeds");
    store
        .save(ProjectionCheckpoint::new("counts", "s-1", 2))
        .await
        .expect("save succeeds");

    let checkpoint = store
        .load("totals", "s-1")
        .await
        .expect("load succeeds")
        .expect("checkpoint retained");
    assert_eq!(checkpoint.version(), 4);
    assert!(
        store
            .load("totals", "missing")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .load("missing", "s-1")
            .await
            .expect("load succeeds")
            .is_none()
    );

    // Saving replaces the retained checkpoint for the same stream.
    store
        .save(ProjectionCheckpoint::new("totals", "s-1", 9))
        .await
        .expect("save succeeds");
    assert_eq!(
        store
            .load("totals", "s-1")
            .await
            .expect("load succeeds")
            .expect("checkpoint retained")
            .version(),
        9
    );

    // Deleting one stream keeps the projection's other streams.
    store
        .delete("totals", "s-1")
        .await
        .expect("delete succeeds");
    assert!(
        store
            .load("totals", "s-1")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .load("totals", "s-2")
            .await
            .expect("load succeeds")
            .is_some()
    );

    // Deleting the projection keeps unrelated projections.
    store.delete_all("totals").await.expect("delete succeeds");
    assert!(
        store
            .load("totals", "s-2")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .load("counts", "s-1")
            .await
            .expect("load succeeds")
            .is_some()
    );
}

// ---------------------------------------------------------------------------
// MemorySubscriptions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscriptions_combine_definitions_checkpoints_and_leases() {
    let store = MemorySubscriptions::default();

    assert!(store.load("sub-b").await.expect("load succeeds").is_none());
    store
        .save(PersistentSubscription::new("sub-b", "order-*"))
        .await
        .expect("save succeeds");
    store
        .save(PersistentSubscription::new("sub-a", "*").with_event_types(["Tick"]))
        .await
        .expect("save succeeds");

    // Definitions list in name order with their filters intact.
    let listed = store.list().await.expect("list succeeds");
    let names: Vec<&str> = listed.iter().map(PersistentSubscription::name).collect();
    assert_eq!(names, ["sub-a", "sub-b"]);
    assert_eq!(listed[0].event_types(), &["Tick".into()]);
    assert!(listed[0].matches_stream("anything"));
    assert!(listed[0].matches_event_type("Tick"));
    assert!(!listed[0].matches_event_type("Tock"));
    assert!(listed[1].matches_stream("order-1"));
    assert!(!listed[1].matches_stream("refund-1"));

    // Checkpoints are scoped per subscription and stream.
    store
        .save_checkpoint(SubscriptionCheckpoint::new("sub-b", "order-1", 3))
        .await
        .expect("checkpoint succeeds");
    assert_eq!(
        store
            .load_checkpoint("sub-b", "order-1")
            .await
            .expect("load succeeds")
            .expect("checkpoint retained")
            .version(),
        3
    );
    assert!(
        store
            .load_checkpoint("sub-b", "order-2")
            .await
            .expect("load succeeds")
            .is_none()
    );

    // A competing lease admits exactly one consumer until released.
    assert!(
        store
            .try_acquire("sub-b", "consumer-a")
            .await
            .expect("acquire succeeds")
    );
    assert!(
        !store
            .try_acquire("sub-b", "consumer-b")
            .await
            .expect("acquire succeeds")
    );
    store
        .release("sub-b", "consumer-b")
        .await
        .expect("a foreign release is ignored");
    assert!(
        !store
            .try_acquire("sub-b", "consumer-b")
            .await
            .expect("the lease is still held")
    );
    store
        .release("sub-b", "consumer-a")
        .await
        .expect("release succeeds");
    assert!(
        store
            .try_acquire("sub-b", "consumer-b")
            .await
            .expect("acquire succeeds")
    );

    // Deleting a subscription cascades to its checkpoints and lease.
    store.delete("sub-b").await.expect("delete succeeds");
    assert!(store.load("sub-b").await.expect("load succeeds").is_none());
    assert!(
        store
            .load_checkpoint("sub-b", "order-1")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .try_acquire("sub-b", "consumer-c")
            .await
            .expect("the lease was dropped")
    );
    assert_eq!(store.list().await.expect("list succeeds").len(), 1);
}
