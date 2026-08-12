//! Strict contract tests for the process-local suspended-flow store:
//! version-fenced continuation transitions, wait-correlation indexing, child
//! result recording, owner heartbeats, discovery queries, and the durable
//! timeout index.

use std::time::{Duration, SystemTime};

use catga_core::flow::suspension_store::FlowQuery;
use catga_core::flow::{
    FlowContinuation, FlowState, FlowStatus, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::memory::MemorySuspendedFlows;
use catga_core::{CatgaError, ErrorCode};

fn running(id: &str) -> FlowState {
    FlowState::new(id, "checkout", [], "worker-a")
}

fn waiting(id: &str, correlation: &str, timeout: Duration) -> FlowContinuation {
    let wait = WaitCondition::new(correlation, WaitPolicy::All, 1, SystemTime::now(), timeout);
    FlowContinuation::waiting(running(id).suspended(), "step", wait)
}

#[tokio::test]
async fn suspended_flows_create_get_and_delete_with_version_fencing() {
    let store = MemorySuspendedFlows::default();

    let continuation = FlowContinuation::new(running("f-1"), "step");
    assert!(
        store
            .create(continuation.clone())
            .await
            .expect("create succeeds")
    );
    assert!(
        !store
            .create(FlowContinuation::new(running("f-1"), "step"))
            .await
            .expect("create succeeds"),
        "a duplicate flow identifier is refused"
    );

    let loaded = store
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("continuation retained");
    assert_eq!(loaded.state().id(), "f-1");
    assert_eq!(loaded.step_name(), "step");
    assert!(loaded.wait().is_none());
    assert!(store.get("missing").await.expect("get succeeds").is_none());

    // Deletion requires the current state version.
    assert!(
        !store.delete("f-1", 7).await.expect("delete succeeds"),
        "a stale expected version is refused"
    );
    assert!(store.delete("f-1", 0).await.expect("delete succeeds"));
    assert!(
        !store.delete("f-1", 0).await.expect("delete succeeds"),
        "a deleted flow is gone"
    );
    assert!(!store.delete("missing", 0).await.expect("delete succeeds"));
}

#[tokio::test]
async fn suspended_flows_update_and_claim_require_exact_versions() {
    let store = MemorySuspendedFlows::default();
    store
        .create(FlowContinuation::new(running("f-1"), "step"))
        .await
        .expect("create succeeds");

    // Updates require the exact version successor.
    let next = FlowContinuation::new(
        running("f-1").next_version().expect("version advances"),
        "step-2",
    );
    assert!(
        !store
            .update(3, next.clone())
            .await
            .expect("update succeeds"),
        "a non-successor version is refused"
    );
    assert!(
        store
            .update(0, next.clone())
            .await
            .expect("update succeeds")
    );
    assert!(
        !store.update(0, next).await.expect("update succeeds"),
        "a stale expected version is refused"
    );

    // A missing flow refuses updates outright.
    let orphan = FlowContinuation::new(
        running("missing").next_version().expect("version advances"),
        "step",
    );
    assert!(!store.update(0, orphan).await.expect("update succeeds"));

    // Claims fence on the exact retained continuation, not only the version.
    let stored = store
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("continuation retained");
    let claimed_next = stored
        .clone()
        .with_state(stored.state().clone().next_version().expect("advances"));
    assert!(
        store
            .claim(&stored, claimed_next)
            .await
            .expect("claim succeeds")
    );
    assert!(
        !store
            .claim(&stored, stored.clone())
            .await
            .expect("claim succeeds"),
        "a stale snapshot can no longer claim"
    );

    // A claim that does not advance the version is refused before lookup.
    let stored = store
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("continuation retained");
    assert!(
        !store
            .claim(&stored, stored.clone())
            .await
            .expect("claim succeeds"),
        "a non-successor claim is refused"
    );

    // Claims never resurrect a missing flow.
    let missing = FlowContinuation::new(running("missing"), "step");
    let missing_next = missing
        .clone()
        .with_state(running("missing").next_version().expect("advances"));
    assert!(
        !store
            .claim(&missing, missing_next)
            .await
            .expect("claim succeeds")
    );
}

#[tokio::test]
async fn suspended_flows_index_wait_correlations_uniquely() {
    let store = MemorySuspendedFlows::default();

    assert!(
        store
            .get_by_wait_correlation("c-1")
            .await
            .expect("lookup succeeds")
            .is_none()
    );

    store
        .create(waiting("f-1", "c-1", Duration::from_secs(30)))
        .await
        .expect("create succeeds");
    let found = store
        .get_by_wait_correlation("c-1")
        .await
        .expect("lookup succeeds")
        .expect("correlation indexed");
    assert_eq!(found.state().id(), "f-1");

    // Two active waits on one correlation are an ambiguous conflict.
    store
        .create(waiting("f-2", "c-1", Duration::from_secs(30)))
        .await
        .expect("create succeeds");
    let error = store
        .get_by_wait_correlation("c-1")
        .await
        .expect_err("an ambiguous correlation conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // Deleting one waiter restores the unique mapping.
    store.delete("f-2", 0).await.expect("delete succeeds");
    let found = store
        .get_by_wait_correlation("c-1")
        .await
        .expect("lookup succeeds")
        .expect("correlation indexed");
    assert_eq!(found.state().id(), "f-1");
}

#[tokio::test]
async fn suspended_flows_record_wait_results_without_version_changes() {
    let store = MemorySuspendedFlows::default();
    let wait = WaitCondition::new(
        "c-1",
        WaitPolicy::All,
        2,
        SystemTime::now(),
        Duration::from_secs(30),
    );
    store
        .create(FlowContinuation::waiting(
            running("f-1").suspended(),
            "step",
            wait,
        ))
        .await
        .expect("create succeeds");
    store
        .create(FlowContinuation::new(running("f-2"), "step"))
        .await
        .expect("create succeeds");

    // Missing flows, stale versions, and non-waiting flows refuse results.
    assert!(
        !store
            .record_wait_success("missing", 0, "child-1", vec![1])
            .await
            .expect("record succeeds")
    );
    assert!(
        !store
            .record_wait_success("f-1", 7, "child-1", vec![1])
            .await
            .expect("record succeeds")
    );
    assert!(
        !store
            .record_wait_success("f-2", 0, "child-1", vec![1])
            .await
            .expect("record succeeds"),
        "a flow without a wait refuses child results"
    );
    assert!(
        !store
            .record_wait_failure(
                "f-2",
                0,
                "child-1",
                CatgaError::new(ErrorCode::Internal, "x")
            )
            .await
            .expect("record succeeds")
    );

    // A success advances the completed count; duplicates are idempotent.
    assert!(
        store
            .record_wait_success("f-1", 0, "child-1", vec![9])
            .await
            .expect("record succeeds")
    );
    let stored = store
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("continuation retained");
    let wait = stored.wait().expect("wait retained");
    assert_eq!(wait.completed_count(), 1);
    assert_eq!(wait.results()[0].child_id(), "child-1");
    assert!(
        store
            .record_wait_success("f-1", 0, "child-1", vec![9])
            .await
            .expect("record succeeds"),
        "a duplicate child result is idempotent"
    );

    // A failure records the child error at the current version.
    assert!(
        store
            .record_wait_failure(
                "f-1",
                0,
                "child-2",
                CatgaError::new(ErrorCode::Timeout, "slow")
            )
            .await
            .expect("record succeeds")
    );
    let stored = store
        .get("f-1")
        .await
        .expect("get succeeds")
        .expect("continuation retained");
    assert_eq!(stored.wait().expect("wait retained").completed_count(), 2);
}

#[tokio::test]
async fn suspended_flows_heartbeat_requires_owner_and_version() {
    let store = MemorySuspendedFlows::default();
    store
        .create(FlowContinuation::new(running("f-1"), "step"))
        .await
        .expect("create succeeds");

    assert!(
        !store
            .heartbeat("missing", "worker-a", 0)
            .await
            .expect("heartbeat succeeds")
    );
    assert!(
        !store
            .heartbeat("f-1", "worker-b", 0)
            .await
            .expect("heartbeat succeeds"),
        "a foreign owner refuses the heartbeat"
    );
    assert!(
        !store
            .heartbeat("f-1", "worker-a", 7)
            .await
            .expect("heartbeat succeeds"),
        "a stale version refuses the heartbeat"
    );
    assert!(
        store
            .heartbeat("f-1", "worker-a", 0)
            .await
            .expect("heartbeat succeeds")
    );
}

#[tokio::test]
async fn suspended_flows_query_filters_and_bounds_summaries() {
    let store = MemorySuspendedFlows::default();
    store
        .create(FlowContinuation::new(running("f-1"), "step"))
        .await
        .expect("create succeeds");
    store
        .create(waiting("f-2", "c-1", Duration::from_secs(30)))
        .await
        .expect("create succeeds");

    // Queries validate their bounds.
    assert!(FlowQuery::new(0, 1).is_err());
    assert!(FlowQuery::new(2, 1).is_err());

    let all = store
        .query(&FlowQuery::new(10, 100).expect("query builds"))
        .await
        .expect("query succeeds");
    assert_eq!(all.len(), 2);

    let suspended = store
        .query(
            &FlowQuery::new(10, 100)
                .expect("query builds")
                .with_status(FlowStatus::Suspended),
        )
        .await
        .expect("query succeeds");
    assert_eq!(suspended.len(), 1);
    assert_eq!(suspended[0].id(), "f-2");
    assert_eq!(suspended[0].flow_type(), "checkout");
    assert_eq!(suspended[0].status(), FlowStatus::Suspended);

    // The result bound truncates the summary page.
    let page = store
        .query(&FlowQuery::new(1, 100).expect("query builds"))
        .await
        .expect("query succeeds");
    assert_eq!(page.len(), 1);

    // A disjoint creation window matches nothing.
    let none = store
        .query(
            &FlowQuery::new(10, 100)
                .expect("query builds")
                .created_between(
                    SystemTime::UNIX_EPOCH,
                    SystemTime::UNIX_EPOCH + Duration::from_secs(1),
                )
                .expect("range builds"),
        )
        .await
        .expect("query succeeds");
    assert!(none.is_empty());
}

#[tokio::test]
async fn suspended_flows_timeout_index_polls_acks_and_releases() {
    let store = MemorySuspendedFlows::default();
    store
        .create(waiting("f-1", "c-1", Duration::from_millis(50)))
        .await
        .expect("create succeeds");

    // A continuation without a wait never enters the due index.
    store
        .create(FlowContinuation::new(running("f-2"), "step"))
        .await
        .expect("create succeeds");

    let far_future = SystemTime::now() + Duration::from_secs(3600);

    // Nothing is due before the deadline.
    let poll = TimedOutFlowPoll::new(SystemTime::UNIX_EPOCH, 10, 100).expect("poll builds");
    assert!(
        store
            .poll_timed_out(&poll)
            .await
            .expect("poll succeeds")
            .is_empty()
    );

    // Past the deadline the flow is claimed exactly once.
    let poll = TimedOutFlowPoll::new(far_future, 10, 100).expect("poll builds");
    let receipts = store.poll_timed_out(&poll).await.expect("poll succeeds");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].flow_id(), "f-1");
    assert!(
        store
            .poll_timed_out(&poll)
            .await
            .expect("poll succeeds")
            .is_empty(),
        "a claimed timeout is not redelivered"
    );

    // A malformed receipt token is a validation failure.
    let error = store
        .ack_timed_out(&TimedOutFlowReceipt::new("f-1", [1_u8, 2]))
        .await
        .expect_err("a malformed token must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    let error = store
        .release_timed_out(&TimedOutFlowReceipt::new("f-1", [1_u8, 2]))
        .await
        .expect_err("a malformed token must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Releasing an unchanged claim makes the timeout visible again.
    store
        .release_timed_out(&receipts[0])
        .await
        .expect("release succeeds");
    let receipts = store.poll_timed_out(&poll).await.expect("poll succeeds");
    assert_eq!(receipts.len(), 1);

    // Acknowledging removes the timeout permanently.
    store
        .ack_timed_out(&receipts[0])
        .await
        .expect("ack succeeds");
    assert!(
        store
            .poll_timed_out(&poll)
            .await
            .expect("poll succeeds")
            .is_empty()
    );
    // Re-acknowledging a consumed token is a no-op.
    store
        .ack_timed_out(&receipts[0])
        .await
        .expect("ack succeeds");

    // Deleting the flow drops its pending timeout.
    store.delete("f-2", 0).await.expect("delete succeeds");
    store
        .create(waiting("f-3", "c-3", Duration::from_millis(50)))
        .await
        .expect("create succeeds");
    let receipts = store.poll_timed_out(&poll).await.expect("poll succeeds");
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].flow_id(), "f-3");
}
