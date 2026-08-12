//! SQLite "conflicting row vanished" fault-injection coverage.
//!
//! Every store's create path follows its `INSERT ... ON CONFLICT DO NOTHING` with an identity
//! re-read that distinguishes an idempotent duplicate from a hash collision. Normally the re-read
//! can only miss when a concurrent delete lands between the two statements; these tests inject the
//! same outcome deterministically through a `BEFORE INSERT ... RAISE(IGNORE)` trigger, so each
//! store must surface its transient "disappeared after a conflicting create" guard instead of
//! misreporting the write as a success or a duplicate.
#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, FlowContinuation, FlowScheduler, FlowState, FlowStore,
    StateMachineSnapshot, StateMachineStore, SuspendedFlowStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// A snapshot state kept dependency-free for the vanish-fault guard checks.
#[derive(Clone, Debug, Eq, catga_core::MemoryPackable, PartialEq)]
struct VanishState {
    paid: bool,
    quantity: u32,
}

/// Opens a scratch SQLite database and returns its URL plus a raw administrative pool used to
/// install the fault trigger.
async fn harness(name: &str) -> CatgaResult<(tempfile::TempDir, Box<str>, SqlitePool)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create SQLite vanish-fault directory")
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

/// Installs a trigger that silently drops every insert into `table`, emulating a row that
/// vanishes between a store's conflict-checked insert and its identity re-read.
async fn vanish_on_insert(pool: &SqlitePool, table: &str) -> CatgaResult<()> {
    let statement = format!(
        "CREATE TRIGGER catga_test_vanish_{table} BEFORE INSERT ON {table} \
         BEGIN SELECT RAISE(IGNORE); END"
    );
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    Ok(())
}

/// Asserts one operation failed with the store's retryable lost-race boundary.
fn expect_transient<T: std::fmt::Debug>(result: CatgaResult<T>, operation: &str) {
    match result {
        Ok(value) => panic!("{operation} unexpectedly succeeded over a vanished row: {value:?}"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Transient,
            "{operation} must surface the vanished row as a retryable race"
        ),
    }
}

/// The flow store's single and batched creates must report the vanished row as transient.
#[tokio::test]
async fn sqlite_flow_store_surfaces_a_vanished_conflicting_row() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("flow-vanish.db").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    vanish_on_insert(&pool, "catga_flow_states").await?;

    let state = FlowState::new("vanish-flow", "payment", [], "node-a");
    expect_transient(
        store.create(state.clone()).await,
        "flow create over a vanishing row",
    );
    expect_transient(
        store.create_batch(vec![state]).await,
        "flow batch create over a vanishing row",
    );
    Ok(())
}

/// The continuation store's create must report the vanished row as transient.
#[tokio::test]
async fn sqlite_suspended_store_surfaces_a_vanished_conflicting_row() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("suspended-vanish.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    vanish_on_insert(&pool, "catga_flow_continuations").await?;

    let continuation = FlowContinuation::new(
        FlowState::new("vanish-flow", "payment", [], "node-a"),
        "resume",
    );
    expect_transient(
        store.create(continuation).await,
        "continuation create over a vanishing row",
    );
    Ok(())
}

/// The DSL progress store's create must report the vanished row as transient.
#[tokio::test]
async fn sqlite_dsl_progress_store_surfaces_a_vanished_conflicting_row() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("progress-vanish.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    vanish_on_insert(&pool, "catga_dsl_step_progress").await?;

    let progress = DslStepProgress::new("vanish-flow", 0, b"payload".as_slice());
    expect_transient(
        store.create(progress).await,
        "progress create over a vanishing row",
    );
    Ok(())
}

/// The state-machine store's create must report the vanished row as transient.
#[tokio::test]
async fn sqlite_state_machine_store_surfaces_a_vanished_conflicting_row() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("machine-vanish.db").await?;
    let store = SqlStateMachineStore::<VanishState>::connect_sqlite(&url).await?;
    store.migrate().await?;
    vanish_on_insert(&pool, "catga_state_machine_snapshots").await?;

    let snapshot = StateMachineSnapshot::new(
        "vanish-machine",
        VanishState {
            paid: false,
            quantity: 0,
        },
    );
    expect_transient(
        store.create(snapshot).await,
        "snapshot create over a vanishing row",
    );
    Ok(())
}

/// The scheduler's resume insert must report the vanished row as transient.
#[tokio::test]
async fn sqlite_scheduler_surfaces_a_vanished_conflicting_row() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("scheduler-vanish.db").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;
    vanish_on_insert(&pool, "catga_flow_schedules").await?;

    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    expect_transient(
        scheduler
            .schedule_resume("vanish-flow", "resume", due_at)
            .await,
        "schedule resume over a vanishing row",
    );
    Ok(())
}
