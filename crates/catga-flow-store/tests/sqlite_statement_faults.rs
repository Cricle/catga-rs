//! SQLite statement-failure coverage for every write path.
//!
//! Each store maps database failures on its statements to retryable availability errors through
//! dedicated boundaries. These tests install `BEFORE ... RAISE(ABORT)` triggers so every insert,
//! update, and delete statement fails deterministically, proving each boundary surfaces
//! [`ErrorCode::Unavailable`] instead of panicking or misreporting the write.
#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowScheduler,
    FlowState, FlowStore, StateMachineSnapshot, StateMachineStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// A snapshot state kept dependency-free for the statement-fault checks.
#[derive(Clone, Debug, Eq, catga_core::MemoryPackable, PartialEq)]
struct FaultState {
    paid: bool,
    quantity: u32,
}

/// Opens a scratch SQLite database and returns its URL plus a raw administrative pool used to
/// install the fault triggers.
async fn harness(name: &str) -> CatgaResult<(tempfile::TempDir, Box<str>, SqlitePool)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            "create SQLite statement-fault directory",
        )
        .with_details(error.to_string())
    })?;
    let url: Box<str> = format!("sqlite://{}", directory.path().join(name).display())
        .as_str()
        .into();
    let options = SqliteConnectOptions::from_str(&url)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    Ok((directory, url, pool))
}

/// Installs a trigger that aborts every `event` (`INSERT`, `UPDATE`, or `DELETE`) on `table`,
/// emulating a database-level statement failure.
async fn abort_statement(pool: &SqlitePool, table: &str, event: &str) -> CatgaResult<()> {
    let statement = format!(
        "CREATE TRIGGER catga_test_abort_{event}_{table} BEFORE {event} ON {table} \
         BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END"
    );
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    Ok(())
}

/// Asserts one operation failed with the store's retryable availability boundary.
fn expect_unavailable<T: std::fmt::Debug>(result: CatgaResult<T>, operation: &str) {
    match result {
        Ok(value) => {
            panic!("{operation} unexpectedly succeeded over an aborted statement: {value:?}")
        }
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Unavailable,
            "{operation} must surface the aborted statement as an availability failure"
        ),
    }
}

/// Flow-state inserts and updates must surface aborted statements as unavailable.
#[tokio::test]
async fn sqlite_flow_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("flow-statements.db").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    abort_statement(&pool, "catga_flow_states", "INSERT").await?;
    let state = FlowState::new("fault-flow", "payment", [], "node-a");
    expect_unavailable(store.create(state.clone()).await, "flow create");
    expect_unavailable(
        store.create_batch(vec![state.clone()]).await,
        "flow batch create",
    );
    drop(_directory);

    let (_directory, url, pool) = harness("flow-statements-update.db").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let state = FlowState::new("fault-flow", "payment", [], "node-a");
    assert!(store.create(state.clone()).await?);
    abort_statement(&pool, "catga_flow_states", "UPDATE").await?;
    expect_unavailable(
        store.update(0, state.clone().next_version()?).await,
        "flow update",
    );
    expect_unavailable(
        store.heartbeat("fault-flow", "node-a", 0).await,
        "flow heartbeat",
    );
    expect_unavailable(
        store.try_claim("payment", "node-b", Duration::ZERO).await,
        "flow stale claim",
    );
    Ok(())
}

/// Continuation inserts, updates, and deletes must surface aborted statements as unavailable.
#[tokio::test]
async fn sqlite_suspended_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("suspended-statements.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    abort_statement(&pool, "catga_flow_continuations", "INSERT").await?;
    let continuation = FlowContinuation::new(
        FlowState::new("fault-flow", "payment", [], "node-a"),
        "resume",
    );
    expect_unavailable(store.create(continuation).await, "continuation create");
    drop(_directory);

    let (_directory, url, pool) = harness("suspended-statements-update.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
    let waiting = FlowContinuation::waiting(
        FlowState::new("fault-flow", "payment", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            "fault-correlator",
            WaitPolicy::All,
            1,
            now - Duration::from_secs(5),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(waiting.clone()).await?);
    abort_statement(&pool, "catga_flow_continuations", "UPDATE").await?;
    let next_state = waiting.state().clone().next_version()?;
    let next = waiting.clone().with_state(next_state);
    expect_unavailable(store.update(0, next.clone()).await, "continuation update");
    expect_unavailable(store.claim(&waiting, next).await, "continuation claim");
    expect_unavailable(
        store
            .record_wait_success("fault-flow", 0, "child-a", Vec::new())
            .await,
        "continuation wait success",
    );
    expect_unavailable(
        store
            .record_wait_failure(
                "fault-flow",
                0,
                "child-a",
                CatgaError::new(ErrorCode::Internal, "forced child failure"),
            )
            .await,
        "continuation wait failure",
    );
    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    expect_unavailable(
        store.poll_timed_out(&poll).await,
        "continuation timeout poll",
    );
    drop(_directory);

    let (_directory, url, pool) = harness("suspended-statements-delete.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let continuation = FlowContinuation::new(
        FlowState::new("fault-flow", "payment", [], "node-a"),
        "resume",
    );
    assert!(store.create(continuation).await?);
    abort_statement(&pool, "catga_flow_continuations", "DELETE").await?;
    expect_unavailable(store.delete("fault-flow", 0).await, "continuation delete");
    Ok(())
}

/// Schedule inserts, lease updates, and acknowledgements must surface aborted statements.
#[tokio::test]
async fn sqlite_scheduler_surfaces_aborted_statements() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("scheduler-statements.db").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;

    abort_statement(&pool, "catga_flow_schedules", "INSERT").await?;
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    expect_unavailable(
        scheduler
            .schedule_resume("fault-flow", "resume", due_at)
            .await,
        "schedule resume insert",
    );
    drop(_directory);

    let (_directory, url, pool) = harness("scheduler-statements-update.db").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    let schedule_a = scheduler
        .schedule_resume("fault-flow", "resume-a", due_at)
        .await?;
    let schedule_b = scheduler
        .schedule_resume("fault-flow", "resume-b", due_at)
        .await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(120);
    let claimed = scheduler
        .claim_due("node-a", now, Duration::from_secs(30), 4)
        .await?;
    assert_eq!(claimed.len(), 2, "both due schedules must be leased");
    abort_statement(&pool, "catga_flow_schedules", "UPDATE").await?;
    expect_unavailable(
        scheduler.release_due("node-a", &schedule_a).await,
        "schedule release",
    );
    expect_unavailable(
        scheduler
            .renew_due("node-a", &schedule_b, now, Duration::from_secs(30))
            .await,
        "schedule renew",
    );
    let _schedule_c = scheduler
        .schedule_resume("fault-flow", "resume-c", due_at)
        .await?;
    expect_unavailable(
        scheduler
            .claim_due("node-a", now, Duration::from_secs(30), 4)
            .await,
        "schedule due claim",
    );
    drop(_directory);

    let (_directory, url, pool) = harness("scheduler-statements-delete.db").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    let _schedule_a = scheduler
        .schedule_resume("fault-flow", "resume-a", due_at)
        .await?;
    let schedule_b = scheduler
        .schedule_resume("fault-flow", "resume-b", due_at)
        .await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(120);
    let claimed = scheduler
        .claim_due("node-a", now, Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1, "one due schedule must be leased");
    let claimed_id = claimed[0].schedule_id().to_string();
    let unclaimed_id = if claimed_id == _schedule_a.as_ref() {
        schedule_b.clone()
    } else {
        _schedule_a.clone()
    };
    abort_statement(&pool, "catga_flow_schedules", "DELETE").await?;
    expect_unavailable(
        scheduler.ack_due("node-a", &claimed_id).await,
        "schedule acknowledgement",
    );
    expect_unavailable(
        scheduler.cancel_resume(&unclaimed_id).await,
        "schedule cancel",
    );
    Ok(())
}

/// DSL progress inserts, updates, and deletes must surface aborted statements.
#[tokio::test]
async fn sqlite_dsl_progress_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("progress-statements.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    abort_statement(&pool, "catga_dsl_step_progress", "INSERT").await?;
    let progress = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
    expect_unavailable(store.create(progress).await, "progress create");
    drop(_directory);

    let (_directory, url, pool) = harness("progress-statements-update.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let progress = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
    assert!(store.create(progress.clone()).await?);
    abort_statement(&pool, "catga_dsl_step_progress", "UPDATE").await?;
    expect_unavailable(
        store
            .update(0, progress.next_version(b"next".as_slice())?)
            .await,
        "progress update",
    );
    drop(_directory);

    let (_directory, url, pool) = harness("progress-statements-delete.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let progress = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
    assert!(store.create(progress).await?);
    abort_statement(&pool, "catga_dsl_step_progress", "DELETE").await?;
    expect_unavailable(store.delete("fault-flow", 0).await, "progress delete");
    Ok(())
}

/// Snapshot inserts and updates must surface aborted statements.
#[tokio::test]
async fn sqlite_state_machine_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("machine-statements.db").await?;
    let store = SqlStateMachineStore::<FaultState>::connect_sqlite(&url).await?;
    store.migrate().await?;

    abort_statement(&pool, "catga_state_machine_snapshots", "INSERT").await?;
    let snapshot = StateMachineSnapshot::new(
        "fault-machine",
        FaultState {
            paid: false,
            quantity: 0,
        },
    );
    expect_unavailable(store.create(snapshot).await, "snapshot create");
    drop(_directory);

    let (_directory, url, pool) = harness("machine-statements-update.db").await?;
    let store = SqlStateMachineStore::<FaultState>::connect_sqlite(&url).await?;
    store.migrate().await?;
    let snapshot = StateMachineSnapshot::new(
        "fault-machine",
        FaultState {
            paid: false,
            quantity: 0,
        },
    );
    assert!(store.create(snapshot.clone()).await?);
    abort_statement(&pool, "catga_state_machine_snapshots", "UPDATE").await?;
    expect_unavailable(
        store
            .update(
                0,
                snapshot.next_version(FaultState {
                    paid: true,
                    quantity: 1,
                })?,
            )
            .await,
        "snapshot update",
    );
    Ok(())
}
