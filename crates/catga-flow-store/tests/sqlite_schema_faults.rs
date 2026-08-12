//! SQLite schema-failure and legacy-upgrade coverage for every migration path.
//!
//! Store migrations must turn DDL conflicts into retryable availability errors, and the
//! continuation migration must upgrade pre-precision schemas in place. These tests pre-create
//! conflicting views so every `CREATE TABLE`/`CREATE INDEX` statement fails deterministically,
//! and they rebuild a legacy continuation table so every guarded `ALTER TABLE` branch runs.
#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use catga_core::flow::{
    FlowContinuation, FlowState, SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt,
    TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// Opens a scratch SQLite database and returns its URL plus a raw administrative pool used to
/// install the schema conflicts.
async fn harness(name: &str) -> CatgaResult<(tempfile::TempDir, Box<str>, SqlitePool)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create SQLite schema-fault directory")
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

/// Runs one raw DDL statement on the administrative pool.
async fn admin_ddl(pool: &SqlitePool, sql: &str) -> CatgaResult<()> {
    sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

/// Pre-creates a view under the supplied object name so the next `CREATE TABLE IF NOT EXISTS`
/// or `CREATE INDEX IF NOT EXISTS` statement for that name fails with a namespace collision.
async fn conflict_view(pool: &SqlitePool, object: &str) -> CatgaResult<()> {
    admin_ddl(
        pool,
        &format!("CREATE VIEW {object} AS SELECT 1 AS collision"),
    )
    .await
}

/// Asserts one migration failed with the store's retryable availability boundary.
fn expect_unavailable<T>(result: CatgaResult<T>, operation: &str) {
    match result {
        Ok(_) => panic!("{operation} unexpectedly succeeded over a conflicting schema"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Unavailable,
            "{operation} must surface the schema conflict as an availability failure"
        ),
    }
}

/// Table-creation conflicts in every SQLite migration must surface as unavailable.
#[tokio::test]
async fn sqlite_migrations_surface_table_conflicts() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("schema-table-flow.db").await?;
    conflict_view(&pool, "catga_flow_states").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    expect_unavailable(store.migrate().await, "flow state migration");
    drop((_directory, pool, store));

    let (_directory, url, pool) = harness("schema-table-suspended.db").await?;
    conflict_view(&pool, "catga_flow_continuations").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    expect_unavailable(store.migrate().await, "continuation migration");
    drop((_directory, pool, store));

    let (_directory, url, pool) = harness("schema-table-scheduler.db").await?;
    conflict_view(&pool, "catga_flow_schedules").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    expect_unavailable(scheduler.migrate().await, "scheduler migration");
    Ok(())
}

/// Index-creation conflicts in every SQLite migration must surface as unavailable.
#[tokio::test]
async fn sqlite_migrations_surface_index_conflicts() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("schema-index-flow.db").await?;
    conflict_view(&pool, "catga_flow_states_stale_idx").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    expect_unavailable(store.migrate().await, "flow state index migration");
    drop((_directory, pool, store));

    let (_directory, url, pool) = harness("schema-index-scheduler.db").await?;
    conflict_view(&pool, "catga_flow_schedules_due_idx").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    expect_unavailable(scheduler.migrate().await, "scheduler index migration");
    drop((_directory, pool, scheduler));

    for (suffix, name) in [
        ("query", "catga_flow_continuations_query_idx"),
        ("order", "catga_flow_continuations_order_idx"),
        ("due", "catga_flow_continuations_due_idx"),
        ("wait", "catga_flow_continuations_wait_correlation_idx"),
    ] {
        let (_directory, url, pool) =
            harness(&format!("schema-index-suspended-{suffix}.db")).await?;
        conflict_view(&pool, name).await?;
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        expect_unavailable(store.migrate().await, "continuation index migration");
    }
    Ok(())
}

/// The continuation migration must upgrade a pre-precision legacy table in place.
#[tokio::test]
async fn sqlite_continuation_migration_upgrades_legacy_schema() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("schema-legacy-upgrade.db").await?;
    admin_ddl(
        &pool,
        "CREATE TABLE catga_flow_continuations (\
             flow_key BLOB PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
             flow_type TEXT NOT NULL, status INTEGER NOT NULL, version INTEGER NOT NULL, \
             created_at_ms INTEGER NOT NULL, deadline_ms INTEGER NULL, \
             revision INTEGER NOT NULL, due_token BLOB NULL, lease_until_ms INTEGER NULL, \
             payload BLOB NOT NULL)",
    )
    .await?;
    admin_ddl(
        &pool,
        "INSERT INTO catga_flow_continuations \
         (flow_key, flow_id, flow_type, status, version, created_at_ms, revision, payload) \
         VALUES (x'01', 'legacy-flow', 'payment', 1, 0, 5000, 0, x'00')",
    )
    .await?;

    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    for column in [
        "created_at_subsec_ns",
        "updated_at_ms",
        "updated_at_subsec_ns",
        "wait_correlation",
    ] {
        let present: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') WHERE name = ?",
        )
        .bind(column)
        .fetch_one(&pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        assert_eq!(present, 1, "legacy upgrade must add the {column} column");
    }
    let backfilled: i64 = sqlx::query_scalar(
        "SELECT updated_at_ms FROM catga_flow_continuations WHERE flow_id = 'legacy-flow'",
    )
    .fetch_one(&pool)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    assert_eq!(
        backfilled, 5000,
        "legacy rows must carry their creation time as the initial update time"
    );

    // The upgraded schema must remain writable through the public store surface.
    let continuation = FlowContinuation::new(
        FlowState::new("upgraded-flow", "payment", [], "node-a"),
        "resume",
    );
    assert!(store.create(continuation).await?);
    assert!(store.get("upgraded-flow").await?.is_some());
    // A second migration over the upgraded schema must stay idempotent.
    store.migrate().await?;
    Ok(())
}

/// Timeout receipt settlement must surface aborted statements and reject malformed tokens.
#[tokio::test]
async fn sqlite_timeout_receipt_settlement_surfaces_faults() -> CatgaResult<()> {
    let (_directory, url, pool) = harness("schema-timeout-receipts.db").await?;
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
    assert!(store.create(waiting).await?);
    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(receipts.len(), 1, "the expired wait must lease one receipt");
    let receipt = receipts.into_iter().next().expect("one leased receipt");

    admin_ddl(
        &pool,
        "CREATE TRIGGER catga_test_abort_update BEFORE UPDATE ON catga_flow_continuations \
         BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END",
    )
    .await?;
    expect_unavailable(
        store.ack_timed_out(&receipt).await,
        "timeout receipt acknowledgement",
    );
    expect_unavailable(
        store.release_timed_out(&receipt).await,
        "timeout receipt release",
    );

    let malformed = TimedOutFlowReceipt::new("fault-flow", vec![7u8; 3]);
    match store.ack_timed_out(&malformed).await {
        Ok(()) => panic!("a malformed receipt token must not settle"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Validation,
            "malformed receipt tokens must fail validation before touching the database"
        ),
    }
    Ok(())
}

/// Connections to a read-only database must surface as unavailable before any statement runs.
#[tokio::test]
async fn sqlite_connections_surface_read_only_databases() -> CatgaResult<()> {
    // The stores open with WAL journaling, which a read-only database rejects immediately, so
    // every connection attempt must surface the store's availability boundary.
    let (_directory, url, pool) = harness("schema-readonly.db").await?;
    admin_ddl(&pool, "CREATE TABLE placeholder (id INTEGER)").await?;
    drop(pool);
    let read_only = format!("{url}?mode=ro");

    expect_unavailable(
        SqlFlowStore::connect_sqlite(&read_only).await,
        "read-only flow state connection",
    );
    expect_unavailable(
        SqlSuspendedFlowStore::connect_sqlite(&read_only).await,
        "read-only continuation connection",
    );
    expect_unavailable(
        SqlFlowScheduler::connect_sqlite(&read_only).await,
        "read-only scheduler connection",
    );
    expect_unavailable(
        SqlStateMachineStore::<FlowState>::connect_sqlite(&read_only).await,
        "read-only state-machine connection",
    );
    expect_unavailable(
        SqlDslStepProgressStore::connect_sqlite(&read_only).await,
        "read-only DSL progress connection",
    );
    Ok(())
}
