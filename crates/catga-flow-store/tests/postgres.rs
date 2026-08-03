//! PostgreSQL real-service integration coverage.
#![cfg(feature = "postgres")]

use catga_core::flow::{
    FlowContinuation, FlowQuery, FlowState, FlowStatus, FlowStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    borrow::Cow,
    time::{Duration, SystemTime},
};
use url::Url;

#[path = "sql_contracts.rs"]
mod sql_contracts;

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_all_store_migrations_serialize_concurrent_initial_ddl() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let database = format!("catga_migration_race_{}", uuid::Uuid::new_v4().simple());
    let isolated_url = isolated_database_url(url.as_ref(), database.as_str())?;
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(url.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    create_database(&admin, database.as_str()).await?;

    let result = async {
        migrate_flow_store_pair(isolated_url.as_str()).await?;
        migrate_suspended_store_pair(isolated_url.as_str()).await?;
        migrate_state_machine_store_pair(isolated_url.as_str()).await?;
        migrate_scheduler_pair(isolated_url.as_str()).await?;
        migrate_dsl_progress_store_pair(isolated_url.as_str()).await
    }
    .await;
    let cleanup = drop_database(&admin, database.as_str()).await;
    result?;
    cleanup
}

fn isolated_database_url(url: &str, database: &str) -> CatgaResult<String> {
    let mut parsed = Url::parse(url).map_err(|error| {
        CatgaError::new(
            ErrorCode::Validation,
            format!("parse PostgreSQL E2E URL for isolated migration database: {error}"),
        )
    })?;
    parsed.set_path(format!("/{database}").as_str());
    Ok(parsed.into())
}

async fn create_database(admin: &PgPool, database: &str) -> CatgaResult<()> {
    // `database` is generated from a UUID in this test, so it is a valid identifier rather than
    // externally supplied SQL. PostgreSQL does not allow `CREATE DATABASE` identifiers to bind.
    let sql = format!("CREATE DATABASE {database}");
    sqlx::query(sqlx::AssertSqlSafe(Cow::Owned(sql)))
        .execute(admin)
        .await
        .map(|_| ())
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))
}

async fn drop_database(admin: &PgPool, database: &str) -> CatgaResult<()> {
    let sql = format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)");
    sqlx::query(sqlx::AssertSqlSafe(Cow::Owned(sql)))
        .execute(admin)
        .await
        .map(|_| ())
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))
}

async fn migrate_flow_store_pair(url: &str) -> CatgaResult<()> {
    let (first, second) = tokio::join!(
        SqlFlowStore::connect_postgres(url),
        SqlFlowStore::connect_postgres(url),
    );
    let first = first?;
    let second = second?;
    let (first, second) = tokio::join!(first.migrate(), second.migrate());
    first?;
    second
}

async fn migrate_suspended_store_pair(url: &str) -> CatgaResult<()> {
    let (first, second) = tokio::join!(
        SqlSuspendedFlowStore::connect_postgres(url),
        SqlSuspendedFlowStore::connect_postgres(url),
    );
    let first = first?;
    let second = second?;
    let (first, second) = tokio::join!(first.migrate(), second.migrate());
    first?;
    second
}

async fn migrate_state_machine_store_pair(url: &str) -> CatgaResult<()> {
    let (first, second) = tokio::join!(
        SqlStateMachineStore::<sql_contracts::ContractState>::connect_postgres(url),
        SqlStateMachineStore::<sql_contracts::ContractState>::connect_postgres(url),
    );
    let first = first?;
    let second = second?;
    let (first, second) = tokio::join!(first.migrate(), second.migrate());
    first?;
    second
}

async fn migrate_scheduler_pair(url: &str) -> CatgaResult<()> {
    let (first, second) = tokio::join!(
        SqlFlowScheduler::connect_postgres(url),
        SqlFlowScheduler::connect_postgres(url),
    );
    let first = first?;
    let second = second?;
    let (first, second) = tokio::join!(first.migrate(), second.migrate());
    first?;
    second
}

async fn migrate_dsl_progress_store_pair(url: &str) -> CatgaResult<()> {
    let (first, second) = tokio::join!(
        SqlDslStepProgressStore::connect_postgres(url),
        SqlDslStepProgressStore::connect_postgres(url),
    );
    let first = first?;
    let second = second?;
    let (first, second) = tokio::join!(first.migrate(), second.migrate());
    first?;
    second
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_flow_and_continuation_contracts() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let flow = SqlFlowStore::connect_postgres(url.as_ref()).await?;
    flow.migrate().await?;
    let id = format!("postgres-flow-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), "postgres-contract", [], "node-a");
    assert!(flow.create(initial.clone()).await?);
    assert!(flow.update(0, initial.clone().next_version()?).await?);
    let stale = FlowState::new(format!("{id}-stale"), "postgres-contract", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flow.create(stale).await?);
    assert!(
        flow.try_claim("postgres-contract", "node-b", Duration::from_secs(1))
            .await?
            .is_some()
    );

    let store = SqlSuspendedFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    sql_contracts::suspended_flow_contract(&store, "postgres-e2e").await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
    let continuation = FlowContinuation::waiting(
        FlowState::new(format!("{id}-wait"), "postgres-contract", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            format!("{id}/wait"),
            WaitPolicy::All,
            1,
            now - Duration::from_secs(2),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(continuation).await?);
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 4)?)
        .await?;
    let receipt = receipts.first().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "PostgreSQL timeout contract returned no receipt",
        )
    })?;
    store.release_timed_out(receipt).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_suspended_query_filters_before_its_scan_limit() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("postgres-filtered-suspended-{}", uuid::Uuid::new_v4());
    assert!(
        store
            .create(FlowContinuation::new(
                FlowState::new(format!("{id}-old"), "unrelated", [], "node-a"),
                "finish",
            ))
            .await?
    );

    let range_start = SystemTime::now();
    assert!(
        store
            .create(FlowContinuation::new(
                FlowState::new(format!("{id}-matching"), "payment", [], "node-a").suspended(),
                "finish",
            ))
            .await?
    );
    let range_end = SystemTime::now() + Duration::from_secs(1);
    let summaries = store
        .query(
            &FlowQuery::new(1, 1)?
                .with_status(FlowStatus::Suspended)
                .with_flow_type("payment")
                .created_between(range_start, range_end)?,
        )
        .await?;

    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id(), format!("{id}-matching"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_suspended_store_looks_up_indexed_wait_correlations() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("postgres-correlation-{}", uuid::Uuid::new_v4());
    let correlation = format!("{id}/one");
    let waiting = FlowContinuation::waiting(
        FlowState::new(format!("{id}-one"), "postgres-contract", [], "node-a").suspended(),
        "finish",
        WaitCondition::new(
            correlation.as_str(),
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(waiting.clone()).await?);
    let found = store
        .get_by_wait_correlation(correlation.as_str())
        .await?
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "PostgreSQL indexed wait was not found")
        })?;
    assert_eq!(found.state().id(), waiting.state().id());
    assert!(
        store
            .get_by_wait_correlation("postgres-correlation/missing")
            .await?
            .is_none()
    );

    let ready = waiting
        .clone()
        .ready()
        .with_state(waiting.state().clone().next_version()?);
    assert!(store.update(0, ready).await?);
    assert!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await?
            .is_none()
    );

    let shared = format!("{id}/shared");
    for suffix in ["two", "three"] {
        assert!(
            store
                .create(FlowContinuation::waiting(
                    FlowState::new(format!("{id}-{suffix}"), "postgres-contract", [], "node-a")
                        .suspended(),
                    "finish",
                    WaitCondition::new(
                        shared.as_str(),
                        WaitPolicy::All,
                        1,
                        SystemTime::now(),
                        Duration::from_secs(30),
                    ),
                ))
                .await?
        );
    }
    assert_eq!(
        store
            .get_by_wait_correlation(shared.as_str())
            .await
            .expect_err("ambiguous PostgreSQL correlation must not select a continuation")
            .code(),
        ErrorCode::Conflict
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_state_machine_store_preserves_snapshots_and_version_cas() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store =
        SqlStateMachineStore::<sql_contracts::ContractState>::connect_postgres(url.as_ref())
            .await?;
    store.migrate().await?;
    store.migrate().await?;
    sql_contracts::state_machine_contract(&store, "postgres-e2e").await
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_dsl_progress_store_preserves_checkpoint_recovery() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlDslStepProgressStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    store.migrate().await?;
    sql_contracts::dsl_progress_contract(&store, "postgres-e2e").await?;
    sql_contracts::dsl_flow_restart_contract(&store, "postgres-e2e").await
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_flow_scheduler_is_idempotent_and_lease_fenced() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let scheduler = SqlFlowScheduler::connect_postgres(url.as_ref()).await?;
    scheduler.migrate().await?;
    scheduler.migrate().await?;
    sql_contracts::scheduler_contract(&scheduler, "postgres-e2e").await
}
