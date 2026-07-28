//! SQL Server real-service integration coverage.
#![cfg(feature = "mssql")]

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStatus, FlowStore, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

type MssqlAdminPool = bb8::Pool<bb8_tiberius::ConnectionManager>;

#[path = "sql_contracts.rs"]
mod sql_contracts;

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_continuation_migration_is_concurrent_and_repeatable() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (first, second) = tokio::join!(
        SqlSuspendedFlowStore::connect_mssql(url.as_ref()),
        SqlSuspendedFlowStore::connect_mssql(url.as_ref()),
    );
    let first = first?;
    let second = second?;
    let (first_result, second_result) = tokio::join!(first.migrate(), second.migrate());
    first_result?;
    second_result?;
    first.migrate().await
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_scheduler_is_idempotent_and_lease_fenced() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let scheduler = SqlFlowScheduler::connect_mssql(url.as_ref()).await?;
    scheduler.migrate().await?;
    scheduler.migrate().await?;
    sql_contracts::scheduler_contract(&scheduler, "mssql-e2e").await
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_and_continuation_contracts() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let flow = SqlFlowStore::connect_mssql(url.as_ref()).await?;
    flow.migrate().await?;
    let id = format!("mssql-flow-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), "mssql-contract", [], "node-a");
    assert!(flow.create(initial.clone()).await?);
    assert!(flow.update(0, initial.clone().next_version()?).await?);
    let stale = FlowState::new(format!("{id}-stale"), "mssql-contract", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flow.create(stale).await?);
    assert!(
        flow.try_claim("mssql-contract", "node-b", Duration::from_secs(1))
            .await?
            .is_some()
    );

    let store = SqlSuspendedFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    sql_contracts::suspended_flow_contract(&store, "mssql-e2e").await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
    let continuation = FlowContinuation::waiting(
        FlowState::new(format!("{id}-wait"), "mssql-contract", [], "node-a").suspended(),
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
    let correlation = format!("{id}/wait");
    let waiting = store
        .get_by_wait_correlation(&correlation)
        .await?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "SQL Server continuation correlation lookup returned no continuation",
            )
        })?;
    assert_eq!(waiting.state().id(), format!("{id}-wait"));
    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 4)?)
        .await?;
    let receipt = receipts.first().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQL Server timeout contract returned no receipt",
        )
    })?;
    store.release_timed_out(receipt).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_suspended_query_filters_before_its_scan_limit() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("mssql-filtered-suspended-{}", uuid::Uuid::new_v4());
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
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_state_machine_store_preserves_snapshots_and_version_cas() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store =
        SqlStateMachineStore::<sql_contracts::ContractState>::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    store.migrate().await?;
    sql_contracts::state_machine_contract(&store, "mssql-e2e").await
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_dsl_progress_store_preserves_checkpoint_recovery() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlDslStepProgressStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    store.migrate().await?;
    sql_contracts::dsl_progress_contract(&store, "mssql-e2e").await
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_dsl_progress_rejects_absent_and_concurrent_stale_updates() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlDslStepProgressStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let flow_id = format!("mssql-progress-cas-{}", uuid::Uuid::new_v4());
    let initial = DslStepProgress::new(flow_id.as_str(), 3, b"initial".as_slice());
    let absent_next = initial.clone().next_version(b"missing".as_slice())?;
    assert!(store.get(flow_id.as_str(), 3).await?.is_none());
    assert!(!store.update(initial.version(), absent_next).await?);

    assert!(store.create(initial.clone()).await?);
    let first = initial.clone().next_version(b"first".as_slice())?;
    let second = initial.next_version(b"second".as_slice())?;
    let (first_result, second_result) = tokio::join!(
        store.update(0, first.clone()),
        store.update(0, second.clone())
    );
    assert_eq!(usize::from(first_result?) + usize::from(second_result?), 1);

    let persisted = store
        .get(flow_id.as_str(), 3)
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "SQL Server progress disappeared"))?;
    assert!(persisted == first || persisted == second);
    assert!(store.delete(flow_id.as_str(), 3).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_store_handles_missing_rows_duplicate_creates_and_fresh_claims()
-> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("mssql-flow-edges-{}", uuid::Uuid::new_v4());
    let flow_type = format!("mssql-flow-edges-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), flow_type.as_str(), [], "node-a");
    let absent_next = initial.clone().next_version()?;
    assert!(store.get(id.as_str()).await?.is_none());
    assert!(!store.update(initial.version(), absent_next).await?);

    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert!(
        store
            .try_claim(flow_type.as_str(), "node-b", Duration::from_secs(60))
            .await?
            .is_none()
    );
    assert!(
        !store
            .heartbeat(id.as_str(), "node-b", initial.version())
            .await?
    );
    assert!(
        store
            .heartbeat(id.as_str(), "node-a", initial.version())
            .await?
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_scheduler_handles_zero_limits_expired_leases_and_unclaimed_cancellation()
-> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, isolated_url, database) = create_mssql_test_database(url.as_ref()).await?;
    let result = async {
        let scheduler = SqlFlowScheduler::connect_mssql(isolated_url.as_str()).await?;
        scheduler.migrate().await?;
        let prefix = format!("mssql-scheduler-{}", uuid::Uuid::new_v4());
        let due = SystemTime::now() - Duration::from_secs(2);

        let cancelled = scheduler
            .schedule_resume(prefix.as_str(), "cancelled", due)
            .await?;
        assert!(scheduler.cancel_resume(cancelled.as_ref()).await?);
        assert!(!scheduler.cancel_resume(cancelled.as_ref()).await?);

        let schedule_id = scheduler
            .schedule_resume(prefix.as_str(), "leased", due)
            .await?;
        assert!(
            scheduler
                .claim_due("worker-a", due, Duration::from_secs(1), 0)
                .await?
                .is_empty()
        );
        let claimed = scheduler
            .claim_due("worker-a", due, Duration::from_secs(1), 1)
            .await?;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].schedule_id(), schedule_id.as_ref());
        assert!(
            !scheduler
                .release_due("worker-b", schedule_id.as_ref())
                .await?
        );
        assert!(
            !scheduler
                .renew_due(
                    "worker-b",
                    schedule_id.as_ref(),
                    due + Duration::from_secs(1),
                    Duration::from_secs(1),
                )
                .await?
        );

        let recovered = scheduler
            .claim_due(
                "worker-c",
                due + Duration::from_secs(2),
                Duration::from_secs(30),
                1,
            )
            .await?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].schedule_id(), schedule_id.as_ref());
        assert!(!scheduler.cancel_resume(schedule_id.as_ref()).await?);
        assert!(
            scheduler
                .release_due("worker-c", schedule_id.as_ref())
                .await?
        );
        assert!(scheduler.cancel_resume(schedule_id.as_ref()).await?);
        Ok(())
    }
    .await;
    let cleanup = drop_mssql_test_database(&admin, database.as_str()).await;
    result?;
    cleanup
}

async fn create_mssql_test_database(url: &str) -> CatgaResult<(MssqlAdminPool, String, String)> {
    let database = format!("catga_e2e_{}", uuid::Uuid::new_v4().simple());
    let manager = bb8_tiberius::ConnectionManager::build(url).map_err(|error| {
        CatgaError::new(
            ErrorCode::Unavailable,
            format!("build SQL Server E2E admin connection: {error}"),
        )
    })?;
    let admin = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("connect SQL Server E2E admin database: {error}"),
            )
        })?;
    {
        let mut connection = admin.get().await.map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("acquire SQL Server E2E admin connection: {error}"),
            )
        })?;
        let create = format!("CREATE DATABASE [{database}]");
        connection
            .simple_query(create.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?
            .into_first_result()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    }
    // ADO connection parsing stores the final value of a repeated key, so this preserves every
    // operator-supplied setting while redirecting only this test to its isolated database.
    Ok((admin, format!("{url};Database={database}"), database))
}

async fn drop_mssql_test_database(admin: &MssqlAdminPool, database: &str) -> CatgaResult<()> {
    let mut connection = admin.get().await.map_err(|error| {
        CatgaError::new(
            ErrorCode::Unavailable,
            format!("acquire SQL Server E2E cleanup connection: {error}"),
        )
    })?;
    let drop = format!(
        "ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]"
    );
    connection
        .simple_query(drop.as_str())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?
        .into_first_result()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    Ok(())
}
