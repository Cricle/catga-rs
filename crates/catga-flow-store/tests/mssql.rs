//! SQL Server real-service integration coverage.
#![cfg(feature = "mssql")]

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowState, FlowStatus, FlowStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

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
