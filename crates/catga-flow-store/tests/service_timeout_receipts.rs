//! Timeout-receipt settlement coverage against live MySQL/PostgreSQL plus migration stragglers.
//!
//! The shared timeout receipt leasing leases, acknowledges, releases, and re-scans due waits on
//! each service dialect. The remaining checks force the last migration fault boundaries: the
//! generic scheduler migration over a conflicting PostgreSQL schema and the SQLite continuation
//! backfill over a table whose updates abort.
#![cfg(any(feature = "mysql", feature = "postgres"))]

use std::str::FromStr;
use std::time::{Duration, SystemTime};

use catga_core::flow::{
    FlowContinuation, FlowState, SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore,
    WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{SqlFlowScheduler, SqlSuspendedFlowStore};

#[path = "sql_contracts.rs"]
mod sql_contracts;

/// Builds one due continuation with a unique identity under the supplied correlator.
fn due_continuation(flow_id: &str, now: SystemTime) -> CatgaResult<FlowContinuation> {
    Ok(FlowContinuation::waiting(
        FlowState::new(flow_id, "payment", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            format!("{flow_id}-correlator"),
            WaitPolicy::All,
            1,
            now - Duration::from_secs(5),
            Duration::from_secs(1),
        ),
    ))
}

/// Leases, settles, and re-scans timeout receipts through one suspended store.
async fn exercise_timeout_receipts(store: &SqlSuspendedFlowStore) -> CatgaResult<()> {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
    let suffix = uuid::Uuid::new_v4().simple();
    let first_id: Box<str> = format!("receipt-first-{suffix}").into();
    let second_id: Box<str> = format!("receipt-second-{suffix}").into();
    assert!(store.create(due_continuation(&first_id, now)?).await?);
    assert!(store.create(due_continuation(&second_id, now)?).await?);

    let poll = TimedOutFlowPoll::new(now, 2, 2)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(receipts.len(), 2, "both expired waits must lease receipts");
    let released_id: Box<str> = receipts[1].flow_id().into();
    store.ack_timed_out(&receipts[0]).await?;
    store.ack_timed_out(&receipts[0]).await?;
    store.release_timed_out(&receipts[1]).await?;

    // The released receipt must be due again while the acknowledged one stays settled.
    let poll = TimedOutFlowPoll::new(now, 2, 2)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(
        receipts.len(),
        1,
        "only the released receipt must lease again"
    );
    assert_eq!(receipts[0].flow_id(), released_id.as_ref());
    store.ack_timed_out(&receipts[0]).await?;

    // Nothing remains due, so the scan must commit an empty lease batch.
    let poll = TimedOutFlowPoll::new(now, 2, 2)?;
    assert!(
        store.poll_timed_out(&poll).await?.is_empty(),
        "settled receipts must not lease again"
    );
    Ok(())
}

/// MySQL timeout receipts must lease, settle, and rescan over an isolated database.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_timeout_receipts_lease_and_settle() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let database = format!("catga_receipts_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{database}`")))
        .execute(&admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = url::Url::parse(base.as_ref())
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(format!("/{database}").as_str());
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        exercise_timeout_receipts(&store).await
    }
    .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS `{database}`"
    )))
    .execute(&admin)
    .await;
    result
}

/// PostgreSQL timeout receipts must lease, settle, and rescan over an isolated database.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_timeout_receipts_lease_and_settle() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let database = format!("catga_receipts_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = url::Url::parse(base.as_ref())
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(format!("/{database}").as_str());
    let result = async {
        let store = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        exercise_timeout_receipts(&store).await
    }
    .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await;
    result
}

/// The generic scheduler migration must surface PostgreSQL schema conflicts as unavailable.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_scheduler_migration_surfaces_schema_conflicts() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let database = format!("catga_sched_conflict_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = url::Url::parse(base.as_ref())
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(format!("/{database}").as_str());
    let result = async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        sqlx::query("CREATE VIEW catga_flow_schedules AS SELECT 1 AS collision")
            .execute(&pool)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        let scheduler = SqlFlowScheduler::connect_postgres(url.as_str()).await?;
        match scheduler.migrate().await {
            Ok(()) => {
                panic!("scheduler migration unexpectedly succeeded over a conflicting schema")
            }
            Err(error) => assert_eq!(
                error.code(),
                ErrorCode::Unavailable,
                "the schema conflict must surface as an availability failure"
            ),
        }
        Ok(())
    }
    .await;
    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE IF EXISTS {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await;
    result
}

/// The SQLite continuation backfill must surface aborted legacy updates as unavailable.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_legacy_backfill_surfaces_aborted_updates() -> CatgaResult<()> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            "create SQLite backfill-fault directory",
        )
        .with_details(error.to_string())
    })?;
    let url: Box<str> = format!(
        "sqlite://{}",
        directory.path().join("backfill-fault.db").display()
    )
    .as_str()
    .into();
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
        .create_if_missing(true);
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    sqlx::query(
        "CREATE TABLE catga_flow_continuations (\
             flow_key BLOB PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
             flow_type TEXT NOT NULL, status INTEGER NOT NULL, version INTEGER NOT NULL, \
             created_at_ms INTEGER NOT NULL, deadline_ms INTEGER NULL, \
             revision INTEGER NOT NULL, due_token BLOB NULL, lease_until_ms INTEGER NULL, \
             payload BLOB NOT NULL)",
    )
    .execute(&pool)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    sqlx::query(
        "INSERT INTO catga_flow_continuations \
         (flow_key, flow_id, flow_type, status, version, created_at_ms, revision, payload) \
         VALUES (?, 'legacy-flow', 'payment', 1, 0, 5000, 0, ?)",
    )
    .bind([1u8].as_slice())
    .bind([0u8].as_slice())
    .execute(&pool)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    sqlx::query(
        "CREATE TRIGGER catga_test_abort_backfill BEFORE UPDATE ON catga_flow_continuations \
         BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END",
    )
    .execute(&pool)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;

    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    match store.migrate().await {
        Ok(()) => panic!("legacy backfill unexpectedly succeeded over an aborted update"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Unavailable,
            "the aborted backfill must surface as an availability failure"
        ),
    }
    Ok(())
}
