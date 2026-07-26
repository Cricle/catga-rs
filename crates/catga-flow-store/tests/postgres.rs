//! PostgreSQL integration coverage, enabled only when `CATGA_POSTGRES_URL` is configured.
#![cfg(feature = "postgres")]

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, FlowStore, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_flow_store::{SqlFlowStore, SqlSuspendedFlowStore};
use std::{
    env,
    time::{Duration, SystemTime},
};

#[tokio::test]
async fn postgres_flow_and_continuation_contracts() -> CatgaResult<()> {
    let Ok(url) = env::var("CATGA_POSTGRES_URL") else {
        return Ok(());
    };
    let flow = SqlFlowStore::connect_postgres(&url).await?;
    flow.migrate().await?;
    let id = format!("postgres-flow-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), "postgres-contract", [], "node-a");
    assert!(flow.create(initial.clone()).await?);
    assert!(flow.update(0, initial.clone().next_version()).await?);
    let stale = FlowState::new(format!("{id}-stale"), "postgres-contract", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flow.create(stale).await?);
    assert!(
        flow.try_claim("postgres-contract", "node-b", Duration::from_secs(1))
            .await?
            .is_some()
    );

    let store = SqlSuspendedFlowStore::connect_postgres(&url).await?;
    store.migrate().await?;
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
