//! SQL Server integration coverage, enabled only when `CATGA_MSSQL_URL` is configured.
#![cfg(feature = "mssql")]

use std::{
    env,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DueFlowScheduler, FlowContinuation, FlowScheduler, FlowState, FlowStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_flow_store::{SqlFlowScheduler, SqlFlowStore, SqlSuspendedFlowStore};

#[tokio::test]
async fn mssql_scheduler_is_idempotent_and_lease_fenced() -> CatgaResult<()> {
    let Ok(url) = env::var("CATGA_MSSQL_URL") else {
        return Ok(());
    };
    let scheduler = SqlFlowScheduler::connect_mssql(&url).await?;
    scheduler.migrate().await?;
    let target = format!("mssql-schedule-{}", uuid::Uuid::new_v4());
    let due = SystemTime::now() + Duration::from_secs(5);
    let (first, second) = tokio::join!(
        scheduler.schedule_resume(&target, "resume", due),
        scheduler.schedule_resume(&target, "resume", due),
    );
    let first = first?;
    assert_eq!(first, second?);
    let claimed = scheduler
        .claim_due("worker-a", due, Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(!scheduler.cancel_resume(first.as_ref()).await?);
    assert!(
        scheduler
            .release_due("worker-a", claimed[0].schedule_id())
            .await?
    );
    let reclaimed = scheduler
        .claim_due(
            "worker-b",
            due + Duration::from_secs(1),
            Duration::from_secs(30),
            1,
        )
        .await?;
    assert_eq!(reclaimed.len(), 1);
    assert!(
        scheduler
            .ack_due("worker-b", reclaimed[0].schedule_id())
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn mssql_flow_and_continuation_contracts() -> CatgaResult<()> {
    let Ok(url) = env::var("CATGA_MSSQL_URL") else {
        return Ok(());
    };
    let flow = SqlFlowStore::connect_mssql(&url).await?;
    flow.migrate().await?;
    let id = format!("mssql-flow-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), "mssql-contract", [], "node-a");
    assert!(flow.create(initial.clone()).await?);
    assert!(flow.update(0, initial.clone().next_version()).await?);
    let stale = FlowState::new(format!("{id}-stale"), "mssql-contract", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flow.create(stale).await?);
    assert!(
        flow.try_claim("mssql-contract", "node-b", Duration::from_secs(1))
            .await?
            .is_some()
    );

    let store = SqlSuspendedFlowStore::connect_mssql(&url).await?;
    store.migrate().await?;
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
