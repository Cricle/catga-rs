//! Adapter failure-path coverage that never requires a live service.
//!
//! Two deterministic fault classes are exercised here: operations issued before their schema
//! migration runs must surface the database's missing-table failure, and migrations over an
//! incompatible existing relation must fail instead of silently adopting the wrong shape. Server
//! dialect constructors additionally prove pool-option validation happens before any network I/O.
#![cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]

#[cfg(feature = "sqlite")]
use std::str::FromStr;
use std::time::{Duration, SystemTime};

#[cfg(feature = "sqlite")]
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
#[cfg(feature = "sqlite")]
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore,
};
#[cfg(feature = "sqlite")]
use catga_core::{CatgaError, CatgaResult, ErrorCode};
#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
use catga_flow_store::SqlFlowStoreOptions;
#[cfg(feature = "sqlite")]
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

/// A snapshot state kept dependency-free for migration and schema-fault checks.
#[cfg(feature = "sqlite")]
#[derive(Clone, Debug, Eq, catga_core::MemoryPackable, PartialEq)]
struct FaultState {
    paid: bool,
    quantity: u32,
}

/// Creates a scratch directory and a SQLite database URL inside it.
#[cfg(feature = "sqlite")]
fn harness(name: &str) -> CatgaResult<(tempfile::TempDir, Box<str>)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create SQLite fault-test directory")
            .with_details(error.to_string())
    })?;
    let url: Box<str> = format!("sqlite://{}", directory.path().join(name).display())
        .as_str()
        .into();
    Ok((directory, url))
}

/// Asserts one operation failed with the store's retryable availability boundary.
#[cfg(feature = "sqlite")]
fn expect_unavailable<T: std::fmt::Debug>(result: CatgaResult<T>, operation: &str) {
    match result {
        Ok(value) => panic!("{operation} unexpectedly succeeded before migration: {value:?}"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Unavailable,
            "{operation} must surface the missing schema as an availability failure"
        ),
    }
}

/// Every public mutation and read must fail cleanly while its table does not exist.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_stores_surface_schema_errors_before_migration() -> CatgaResult<()> {
    let (_directory, url) = harness("not-migrated.db")?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);

    let flow = SqlFlowStore::connect_sqlite(&url).await?;
    let state = FlowState::new("fault-flow", "faults", [], "node-a");
    expect_unavailable(flow.create(state.clone()).await, "flow create");
    expect_unavailable(
        flow.create_batch(vec![state.clone()]).await,
        "flow create batch",
    );
    expect_unavailable(flow.get("fault-flow").await, "flow get");
    expect_unavailable(flow.update(0, state.next_version()?).await, "flow update");
    expect_unavailable(
        flow.try_claim("faults", "node-b", Duration::from_secs(1))
            .await,
        "flow claim",
    );
    expect_unavailable(
        flow.heartbeat("fault-flow", "node-a", 0).await,
        "flow heartbeat",
    );

    let suspended = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    let continuation = FlowContinuation::new(
        FlowState::new("fault-flow", "faults", [], "node-a"),
        "resume",
    );
    expect_unavailable(
        suspended.create(continuation.clone()).await,
        "continuation create",
    );
    expect_unavailable(suspended.get("fault-flow").await, "continuation get");
    expect_unavailable(
        suspended.get_by_wait_correlation("fault-correlation").await,
        "continuation wait-correlation lookup",
    );
    expect_unavailable(
        suspended.query(&FlowQuery::new(1, 4)?).await,
        "continuation query",
    );
    expect_unavailable(
        suspended.delete("fault-flow", 0).await,
        "continuation delete",
    );
    expect_unavailable(
        suspended
            .update(
                0,
                continuation.with_state(
                    FlowState::new("fault-flow", "faults", [], "node-a").next_version()?,
                ),
            )
            .await,
        "continuation update",
    );
    expect_unavailable(
        suspended.heartbeat("fault-flow", "node-a", 0).await,
        "continuation heartbeat",
    );
    expect_unavailable(
        suspended
            .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 1)?)
            .await,
        "timeout poll",
    );
    let receipt = TimedOutFlowReceipt::new("fault-flow", [7_u8; 16]);
    expect_unavailable(suspended.ack_timed_out(&receipt).await, "timeout ack");
    expect_unavailable(
        suspended.release_timed_out(&receipt).await,
        "timeout release",
    );

    let progress = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    let step = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
    expect_unavailable(progress.create(step.clone()).await, "progress create");
    expect_unavailable(progress.get("fault-flow", 0).await, "progress get");
    expect_unavailable(
        progress
            .update(0, step.next_version(b"next".as_slice())?)
            .await,
        "progress update",
    );
    expect_unavailable(progress.delete("fault-flow", 0).await, "progress delete");

    let snapshots = SqlStateMachineStore::<FaultState>::connect_sqlite(&url).await?;
    let snapshot = StateMachineSnapshot::new(
        "fault-machine",
        FaultState {
            paid: false,
            quantity: 0,
        },
    );
    expect_unavailable(snapshots.create(snapshot.clone()).await, "snapshot create");
    expect_unavailable(snapshots.get("fault-machine").await, "snapshot get");
    expect_unavailable(
        snapshots
            .update(
                0,
                snapshot.next_version(FaultState {
                    paid: true,
                    quantity: 0,
                })?,
            )
            .await,
        "snapshot update",
    );

    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    expect_unavailable(
        scheduler.schedule_resume("fault-flow", "resume", now).await,
        "schedule resume",
    );
    expect_unavailable(
        scheduler
            .claim_due("worker", now, Duration::from_secs(1), 1)
            .await,
        "claim due",
    );
    expect_unavailable(scheduler.cancel_resume("fault-flow").await, "cancel resume");
    expect_unavailable(scheduler.ack_due("worker", "fault-flow").await, "ack due");
    expect_unavailable(
        scheduler.release_due("worker", "fault-flow").await,
        "release due",
    );
    expect_unavailable(
        scheduler
            .renew_due("worker", "fault-flow", now, Duration::from_secs(1))
            .await,
        "renew due",
    );
    Ok(())
}

/// A pre-existing relation with a store table's name must fail its migration loudly.
///
/// Only the flow, continuation, and scheduler migrations follow their `CREATE TABLE IF NOT
/// EXISTS` with index or backfill statements, so only they can detect the decoy; the
/// single-statement progress and snapshot migrations legally no-op over it.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_migrations_fail_over_incompatible_existing_relations() -> CatgaResult<()> {
    let (_directory, url) = harness("decoy.db")?;
    let connect_options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
        .create_if_missing(true);
    let decoys = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    for table in [
        "catga_flow_states",
        "catga_flow_continuations",
        "catga_flow_schedules",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE VIEW {table} AS SELECT 1 AS decoy"
        )))
        .execute(&decoys)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    }

    let flow = SqlFlowStore::connect_sqlite(&url).await?;
    assert!(
        flow.migrate().await.is_err(),
        "flow migration over an incompatible relation must fail"
    );
    let suspended = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    assert!(
        suspended.migrate().await.is_err(),
        "continuation migration over an incompatible relation must fail"
    );
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    assert!(
        scheduler.migrate().await.is_err(),
        "scheduler migration over an incompatible relation must fail"
    );
    Ok(())
}

/// The summary scan stops as soon as the caller's result bound is satisfied.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_suspended_query_stops_at_the_result_bound() -> CatgaResult<()> {
    let (_directory, url) = harness("result-bound.db")?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    for index in 0..3_u8 {
        let id = format!("bounded-{index}");
        let continuation = FlowContinuation::new(
            FlowState::new(id.as_str(), "bounded-query", [], "node-a"),
            "resume",
        );
        assert!(store.create(continuation).await?);
    }
    let summaries = store.query(&FlowQuery::new(1, 8)?).await?;
    assert_eq!(
        summaries.len(),
        1,
        "the query must stop at the caller's result bound even with more matches on disk"
    );
    Ok(())
}

/// A URL the SQLite driver cannot parse must fail before any file system or pool work.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_adapters_reject_unparseable_urls() {
    const INVALID_URL: &str = "sqlite://fault.db?catga_bogus_parameter=1";

    let flow = SqlFlowStore::connect_sqlite(INVALID_URL).await;
    assert!(flow.is_err(), "flow store must reject an unparseable URL");
    let suspended = SqlSuspendedFlowStore::connect_sqlite(INVALID_URL).await;
    assert!(
        suspended.is_err(),
        "suspended store must reject an unparseable URL"
    );
    let progress = SqlDslStepProgressStore::connect_sqlite(INVALID_URL).await;
    assert!(
        progress.is_err(),
        "progress store must reject an unparseable URL"
    );
    let snapshots = SqlStateMachineStore::<FaultState>::connect_sqlite(INVALID_URL).await;
    assert!(
        snapshots.is_err(),
        "snapshot store must reject an unparseable URL"
    );
    let scheduler = SqlFlowScheduler::connect_sqlite(INVALID_URL).await;
    assert!(
        scheduler.is_err(),
        "scheduler must reject an unparseable URL"
    );
}

/// A zero connection limit is rejected before any network I/O on every server dialect.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_flow_store_rejects_zero_pool_capacity_before_network_io() {
    let options = SqlFlowStoreOptions::new().max_connections(0);
    let result =
        SqlFlowStore::connect_mysql_with_options("mysql://127.0.0.1:1/unused", options).await;
    match result {
        Ok(_) => panic!("a zero MySQL connection limit must be rejected before connecting"),
        Err(error) => assert_eq!(error.code(), catga_core::ErrorCode::Validation),
    }
}

/// A zero connection limit is rejected before any network I/O on every server dialect.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_flow_store_rejects_zero_pool_capacity_before_network_io() {
    let options = SqlFlowStoreOptions::new().max_connections(0);
    let result =
        SqlFlowStore::connect_postgres_with_options("postgres://127.0.0.1:1/unused", options).await;
    match result {
        Ok(_) => panic!("a zero PostgreSQL connection limit must be rejected before connecting"),
        Err(error) => assert_eq!(error.code(), catga_core::ErrorCode::Validation),
    }
}

/// A zero connection limit is rejected before any network I/O on every server dialect.
#[cfg(feature = "mssql")]
#[tokio::test]
async fn mssql_flow_store_rejects_zero_pool_capacity_before_network_io() {
    let options = SqlFlowStoreOptions::new().max_connections(0);
    let result = SqlFlowStore::connect_mssql_with_options(
        "server=tcp:127.0.0.1,1;User Id=sa;Password=unused;TrustServerCertificate=true",
        options,
    )
    .await;
    match result {
        Ok(_) => panic!("a zero SQL Server connection limit must be rejected before connecting"),
        Err(error) => assert_eq!(error.code(), catga_core::ErrorCode::Validation),
    }
}
