//! Service-backed contract coverage for [`RedisSuspendedFlows`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::flow::{
    FlowContinuation, FlowQuery, FlowState, FlowStatus, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_redis::RedisSuspendedFlows;

#[path = "support/ids.rs"]
mod ids;
#[path = "support/service_url.rs"]
mod service_url;

use ids::unique_prefix;

fn waiting_continuation(flow_id: &str, flow_type: &str, correlation: &str) -> FlowContinuation {
    let now = SystemTime::now();
    FlowContinuation::waiting(
        FlowState::new(flow_id, flow_type, &b""[..], "node-a").suspended(),
        "complete",
        WaitCondition::new(
            correlation,
            WaitPolicy::All,
            1,
            now,
            Duration::from_secs(3_600),
        ),
    )
}

#[tokio::test]
async fn create_get_update_and_delete_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    assert_eq!(store.get("flow-1").await?, None);
    assert!(!store.delete("flow-1", 0).await?);
    assert!(
        !store
            .update(0, waiting_continuation("flow-1", "payment", "corr-1"))
            .await?
    );

    let continuation = waiting_continuation("flow-1", "payment", "corr-1");
    assert!(store.create(continuation.clone()).await?);
    assert!(!store.create(continuation.clone()).await?);

    let loaded = store
        .get("flow-1")
        .await?
        .expect("a created continuation must load");
    assert_eq!(loaded, continuation);
    assert_eq!(loaded.state().status(), FlowStatus::Suspended);
    assert_eq!(loaded.step_name(), "complete");

    // Exact-version updates apply; stale or skipped versions do not.
    let next = loaded
        .clone()
        .with_state(loaded.state().clone().next_version()?);
    assert!(store.update(0, next).await?);
    let replayed = loaded
        .clone()
        .with_state(loaded.state().clone().next_version()?);
    assert!(!store.update(0, replayed).await?);
    let jumped = loaded
        .clone()
        .with_state(loaded.state().clone().next_version()?.next_version()?);
    assert!(!store.update(0, jumped).await?);

    // Deleting requires the exact durable version.
    assert!(!store.delete("flow-1", 0).await?);
    assert!(store.delete("flow-1", 1).await?);
    assert_eq!(store.get("flow-1").await?, None);
    Ok(())
}

#[tokio::test]
async fn claim_compares_the_exact_serialized_expectation() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    let continuation = waiting_continuation("flow-1", "payment", "corr-claim");
    assert!(store.create(continuation.clone()).await?);

    let next = continuation
        .clone()
        .with_state(continuation.state().clone().next_version()?);
    assert!(store.claim(&continuation, next.clone()).await?);

    // The same expectation no longer matches the stored bytes.
    assert!(!store.claim(&continuation, next).await?);

    // Cross-flow claims are rejected before any Redis round trip.
    let other = waiting_continuation("flow-2", "payment", "corr-other");
    let other_next = other
        .clone()
        .with_state(other.state().clone().next_version()?);
    assert!(!store.claim(&continuation, other_next).await?);

    // Claiming a missing flow finds nothing to compare.
    let missing = waiting_continuation("flow-missing", "payment", "corr-missing");
    let missing_next = missing
        .clone()
        .with_state(missing.state().clone().next_version()?);
    assert!(!store.claim(&missing, missing_next).await?);

    // A version regression inside the same flow is rejected early.
    let current = store
        .get("flow-1")
        .await?
        .expect("a created continuation must load");
    assert!(!store.claim(&current, continuation).await?);
    Ok(())
}

#[tokio::test]
async fn heartbeat_requires_the_current_owner() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    // A ready continuation retains its owner and accepts heartbeats.
    let ready = FlowContinuation::new(
        FlowState::new("flow-ready", "payment", &b""[..], "node-a"),
        "resume",
    );
    assert!(store.create(ready).await?);
    assert!(store.heartbeat("flow-ready", "node-a", 0).await?);
    assert!(!store.heartbeat("flow-ready", "node-b", 0).await?);
    assert!(!store.heartbeat("flow-ready", "node-a", 9).await?);
    assert!(!store.heartbeat("flow-missing", "node-a", 0).await?);

    // Suspended flows drop their owner, so heartbeats never apply.
    assert!(
        store
            .create(waiting_continuation("flow-waiting", "payment", "corr-hb"))
            .await?
    );
    assert!(!store.heartbeat("flow-waiting", "node-a", 0).await?);
    Ok(())
}

#[tokio::test]
async fn wait_results_are_recorded_once_per_child() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    assert!(
        store
            .create(waiting_continuation("flow-1", "payment", "corr-results"))
            .await?
    );

    assert!(
        store
            .record_wait_success("flow-1", 0, "child-1", b"payload".to_vec())
            .await?
    );
    // Recording the same child twice is an idempotent success.
    assert!(
        store
            .record_wait_success("flow-1", 0, "child-1", b"payload".to_vec())
            .await?
    );
    // A stale version loses the mutation race.
    assert!(
        !store
            .record_wait_failure(
                "flow-1",
                9,
                "child-2",
                CatgaError::new(ErrorCode::Internal, "boom")
            )
            .await?
    );

    let updated = store
        .get("flow-1")
        .await?
        .expect("a created continuation must load");
    assert_eq!(
        updated.wait().expect("a wait must exist").results().len(),
        1
    );

    // A non-waiting continuation has nothing to record against.
    let ready = FlowContinuation::new(
        FlowState::new("flow-ready", "payment", &b""[..], "node-a"),
        "resume",
    );
    assert!(store.create(ready).await?);
    assert!(
        !store
            .record_wait_success("flow-ready", 0, "child-1", b"payload".to_vec())
            .await?
    );
    assert!(
        !store
            .record_wait_failure(
                "flow-ready",
                0,
                "child-1",
                CatgaError::new(ErrorCode::Internal, "boom")
            )
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn wait_failures_are_recorded_once_per_child() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    assert!(
        store
            .create(waiting_continuation("flow-fail", "payment", "corr-fail"))
            .await?
    );

    assert!(
        store
            .record_wait_failure(
                "flow-fail",
                0,
                "child-1",
                CatgaError::new(ErrorCode::Internal, "boom")
            )
            .await?
    );
    // Re-recording the reported child is an idempotent success: once the stored
    // continuation stops changing, the compare-and-set short-circuits.
    for _ in 0..32 {
        assert!(
            store
                .record_wait_failure(
                    "flow-fail",
                    0,
                    "child-1",
                    CatgaError::new(ErrorCode::Internal, "boom")
                )
                .await?
        );
    }

    let updated = store
        .get("flow-fail")
        .await?
        .expect("a created continuation must load");
    let results = updated.wait().expect("a wait must exist").results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].child_id(), "child-1");
    Ok(())
}

#[tokio::test]
async fn wait_correlation_lookup_scopes_to_a_single_flow() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    assert_eq!(store.get_by_wait_correlation("corr-absent").await?, None);

    assert!(
        store
            .create(waiting_continuation("flow-1", "payment", "corr-shared"))
            .await?
    );
    let found = store
        .get_by_wait_correlation("corr-shared")
        .await?
        .expect("the correlation must resolve");
    assert_eq!(found.state().id(), "flow-1");

    // Two flows sharing one correlation make the lookup ambiguous.
    assert!(
        store
            .create(waiting_continuation("flow-2", "payment", "corr-shared"))
            .await?
    );
    let ambiguous = store.get_by_wait_correlation("corr-shared").await;
    assert!(matches!(ambiguous, Err(error) if error.code() == ErrorCode::Conflict));

    // Resolving the ambiguity restores the lookup.
    assert!(store.delete("flow-2", 0).await?);
    assert!(
        store
            .get_by_wait_correlation("corr-shared")
            .await?
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn query_filters_by_type_status_and_creation_window() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    store
        .create(waiting_continuation("flow-1", "payment", "corr-q1"))
        .await?;
    store
        .create(waiting_continuation("flow-2", "payment", "corr-q2"))
        .await?;
    store
        .create(waiting_continuation("flow-3", "shipping", "corr-q3"))
        .await?;

    let all = store.query(&FlowQuery::new(10, 10)?).await?;
    assert_eq!(all.len(), 3);

    let payments = store
        .query(&FlowQuery::new(10, 10)?.with_flow_type("payment"))
        .await?;
    assert_eq!(payments.len(), 2);
    assert!(
        payments
            .iter()
            .all(|summary| summary.flow_type() == "payment")
    );

    let failed = store
        .query(&FlowQuery::new(10, 10)?.with_status(FlowStatus::Failed))
        .await?;
    assert!(failed.is_empty());

    let future_window = store
        .query(&FlowQuery::new(10, 10)?.created_between(
            SystemTime::now() + Duration::from_secs(3_600),
            SystemTime::now() + Duration::from_secs(7_200),
        )?)
        .await?;
    // The creation window filter rejects every record created in the present.
    assert!(future_window.is_empty());

    // Result and scan budgets bound the page.
    let capped = store.query(&FlowQuery::new(1, 10)?).await?;
    assert_eq!(capped.len(), 1);
    let scanned = store.query(&FlowQuery::new(2, 2)?).await?;
    assert_eq!(scanned.len(), 2);

    Ok(())
}

#[tokio::test]
async fn delayed_continuations_are_not_wait_indexed() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    let delayed = FlowContinuation::new(
        FlowState::new("flow-delayed", "payment", &b""[..], "node-a").suspended(),
        "resume",
    )
    .delayed_until(SystemTime::now() + Duration::from_secs(3_600));
    assert!(store.create(delayed).await?);

    // Without a wait condition there is no timeout deadline to poll.
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(SystemTime::now(), 10, 10)?)
        .await?;
    assert!(receipts.is_empty());

    let loaded = store
        .get("flow-delayed")
        .await?
        .expect("a created continuation must load");
    assert!(loaded.resume_at().is_some());
    assert!(loaded.wait().is_none());
    Ok(())
}

#[tokio::test]
async fn timeout_poll_validation_and_receipt_fencing() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    // A poll timestamp before the Unix epoch is rejected.
    let before_epoch = TimedOutFlowPoll::new(UNIX_EPOCH - Duration::from_secs(1), 1, 10)?;
    let result = store.poll_timed_out(&before_epoch).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));

    // Receipts with non-UTF-8 tokens are rejected before any Redis mutation.
    let forged = TimedOutFlowReceipt::new("flow-1", vec![0xFF, 0xFE]);
    let result = store.ack_timed_out(&forged).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    let result = store.release_timed_out(&forged).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));

    // Acknowledging or releasing an unknown receipt is an idempotent no-op.
    let unknown = TimedOutFlowReceipt::new("flow-absent", b"1:2".to_vec());
    store.ack_timed_out(&unknown).await?;
    store.release_timed_out(&unknown).await?;
    Ok(())
}

#[tokio::test]
async fn timeout_receipts_are_reclaimed_after_their_lease() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSuspendedFlows::connect(&url, unique_prefix("suspended")).await?;

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(3_000);
    let expired = FlowContinuation::waiting(
        FlowState::new("flow-1", "payment", &b""[..], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            "corr-timeout",
            WaitPolicy::All,
            1,
            now - Duration::from_secs(10),
            Duration::from_secs(5),
        ),
    );
    assert!(store.create(expired).await?);

    let poll = TimedOutFlowPoll::new(now, 5, 5)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(receipts.len(), 1);

    // The receipt lease holds the flow until it expires; a poll thirty-one
    // seconds later reclaims it through the inflight-expiry path.
    let later = TimedOutFlowPoll::new(now + Duration::from_secs(31), 5, 5)?;
    let reclaimed = store.poll_timed_out(&later).await?;
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].flow_id(), "flow-1");

    store.ack_timed_out(&reclaimed[0]).await?;
    assert!(store.poll_timed_out(&later).await?.is_empty());
    Ok(())
}
