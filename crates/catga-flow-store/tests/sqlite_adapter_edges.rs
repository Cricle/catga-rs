//! Public SQLite adapter failure and boundary contracts.
//!
//! These tests exercise only the exported store APIs.  They intentionally live
//! outside `src` so the production crates have no embedded test modules.
#![cfg(feature = "sqlite")]

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::flow::{
    DueFlowScheduler, FlowContinuation, FlowScheduler, FlowState, FlowStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

#[tokio::test]
async fn sqlite_adapters_reject_invalid_urls_before_opening_a_pool() {
    const INVALID_URL: &str = "sqlite://\0";

    assert_sqlite_url_error(SqlFlowStore::connect_sqlite(INVALID_URL).await);
    assert_sqlite_url_error(SqlSuspendedFlowStore::connect_sqlite(INVALID_URL).await);
    assert_sqlite_url_error(SqlFlowScheduler::connect_sqlite(INVALID_URL).await);
    assert_sqlite_url_error(SqlDslStepProgressStore::connect_sqlite(INVALID_URL).await);
    assert_sqlite_url_error(SqlStateMachineStore::<u64>::connect_sqlite(INVALID_URL).await);
}

#[tokio::test]
async fn sqlite_timeout_store_rejects_malformed_receipt_tokens_before_settlement() -> CatgaResult<()>
{
    let directory = tempfile::tempdir().map_err(temporary_directory_error)?;
    let url = format!(
        "sqlite://{}",
        directory.path().join("timeout-token.db").display()
    );
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let waiting = FlowContinuation::waiting(
        FlowState::new("invalid-token", "timeout-edge", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            "invalid-token/wait",
            WaitPolicy::All,
            1,
            now - Duration::from_secs(2),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(waiting).await?);

    for token in [vec![], vec![0; 15], vec![0; 17]] {
        let receipt = TimedOutFlowReceipt::new("invalid-token", token);
        assert_eq!(
            store
                .ack_timed_out(&receipt)
                .await
                .expect_err("SQLite must reject a receipt token it could not issue")
                .code(),
            ErrorCode::Validation
        );
        assert_eq!(
            store
                .release_timed_out(&receipt)
                .await
                .expect_err("SQLite must reject an invalid token before releasing a lease")
                .code(),
            ErrorCode::Validation
        );
    }

    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    let first = store
        .poll_timed_out(&poll)
        .await?
        .pop()
        .expect("expired SQLite continuation must produce a lease receipt");
    store
        .ack_timed_out(&TimedOutFlowReceipt::new("invalid-token", [9_u8; 16]))
        .await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());
    store.release_timed_out(&first).await?;
    let second = store
        .poll_timed_out(&poll)
        .await?
        .pop()
        .expect("released SQLite receipt must be reclaimable");
    assert_ne!(first.token(), second.token());
    store.ack_timed_out(&first).await?;
    store.ack_timed_out(&second).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn sqlite_scheduler_preserves_pre_epoch_deadlines_and_validates_leases() -> CatgaResult<()> {
    let directory = tempfile::tempdir().map_err(temporary_directory_error)?;
    let url = format!(
        "sqlite://{}",
        directory.path().join("scheduler-edge.db").display()
    );
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;

    let due_at = SystemTime::UNIX_EPOCH - Duration::from_nanos(1);
    let schedule_id = scheduler
        .schedule_resume("pre-epoch-flow", "resume", due_at)
        .await?;

    assert!(
        scheduler
            .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::from_secs(1), 0)
            .await?
            .is_empty(),
        "a zero bound must not claim or validate an otherwise due item"
    );
    assert_eq!(
        scheduler
            .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::ZERO, 1)
            .await
            .expect_err("zero lease duration cannot establish an owner")
            .code(),
        ErrorCode::Validation
    );

    let claimed = scheduler
        .claim_due("worker", SystemTime::UNIX_EPOCH, Duration::from_secs(1), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), schedule_id.as_ref());
    assert_eq!(claimed[0].due_at(), due_at);
    assert!(
        !scheduler
            .ack_due("other-worker", schedule_id.as_ref())
            .await?
    );
    assert!(
        !scheduler
            .release_due("other-worker", schedule_id.as_ref())
            .await?
    );
    assert!(
        !scheduler
            .renew_due(
                "other-worker",
                schedule_id.as_ref(),
                SystemTime::UNIX_EPOCH,
                Duration::from_secs(1),
            )
            .await?
    );
    assert_eq!(
        scheduler
            .renew_due(
                "worker",
                schedule_id.as_ref(),
                SystemTime::UNIX_EPOCH,
                Duration::ZERO,
            )
            .await
            .expect_err("zero renewal must be rejected")
            .code(),
        ErrorCode::Validation
    );
    assert!(scheduler.ack_due("worker", schedule_id.as_ref()).await?);
    Ok(())
}

#[tokio::test]
async fn sqlite_flow_store_fences_claims_heartbeats_and_concurrent_transitions() -> CatgaResult<()>
{
    let directory = tempfile::tempdir().map_err(temporary_directory_error)?;
    let url = format!(
        "sqlite://{}",
        directory.path().join("flow-fences.db").display()
    );
    let store = Arc::new(SqlFlowStore::connect_sqlite(&url).await?);
    store.migrate().await?;

    assert!(store.get("absent").await?.is_none());
    let absent = FlowState::new("absent", "payment", [], "owner-a")
        .done(0)
        .next_version()?;
    assert!(!store.update(0, absent).await?);

    let stale =
        FlowState::new("stale", "payment", [], "owner-a").heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await?);
    let claimed = store
        .try_claim("payment", "owner-b", Duration::ZERO)
        .await?
        .expect("a stale running flow must be claimable");
    assert_eq!(claimed.owner(), Some("owner-b"));
    assert!(
        store
            .try_claim("other-flow-type", "owner-c", Duration::ZERO)
            .await?
            .is_none()
    );
    assert!(
        !store
            .heartbeat("stale", "owner-a", claimed.version())
            .await?
    );
    assert!(
        !store
            .heartbeat("stale", "owner-b", claimed.version() - 1)
            .await?
    );
    assert!(
        store
            .heartbeat("stale", "owner-b", claimed.version())
            .await?
    );

    let initial = FlowState::new("concurrent", "payment", [], "owner-a");
    assert!(store.create(initial.clone()).await?);
    let first = initial.clone().done(1).next_version()?;
    let second = initial.done(2).next_version()?;
    let (first, second) = tokio::join!(store.update(0, first), store.update(0, second));
    assert_eq!(usize::from(first?) + usize::from(second?), 1);
    Ok(())
}

fn assert_sqlite_url_error<T>(result: Result<T, CatgaError>) {
    let error = match result {
        Ok(_) => panic!("invalid SQLite URL unexpectedly opened a store"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

fn temporary_directory_error(error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, "create SQLite adapter test directory")
        .with_details(error.to_string())
}
