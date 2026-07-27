//! MySQL integration coverage, enabled only when `CATGA_MYSQL_URL` is configured.
#![cfg(feature = "mysql")]

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
async fn mysql_flow_and_continuation_contracts() -> CatgaResult<()> {
    let Ok(url) = env::var("CATGA_MYSQL_URL") else {
        return Ok(());
    };
    let flow = SqlFlowStore::connect_mysql(&url).await?;
    flow.migrate().await?;
    let id = format!("mysql-flow-{}", uuid::Uuid::new_v4());
    let initial = FlowState::new(id.as_str(), "mysql-contract", [], "node-a");
    assert!(flow.create(initial.clone()).await?);
    assert!(flow.update(0, initial.clone().next_version()).await?);
    let stale = FlowState::new(format!("{id}-stale"), "mysql-contract", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flow.create(stale).await?);
    assert!(
        flow.try_claim("mysql-contract", "node-b", Duration::from_secs(1))
            .await?
            .is_some()
    );

    let store = SqlSuspendedFlowStore::connect_mysql(&url).await?;
    store.migrate().await?;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);
    let continuation = FlowContinuation::waiting(
        FlowState::new(format!("{id}-wait"), "mysql-contract", [], "node-a").suspended(),
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
            "MySQL timeout contract returned no receipt",
        )
    })?;
    store.release_timed_out(receipt).await?;
    Ok(())
}

#[tokio::test]
async fn mysql_suspended_store_looks_up_indexed_wait_correlations() -> CatgaResult<()> {
    let Ok(url) = env::var("CATGA_MYSQL_URL") else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mysql(&url).await?;
    store.migrate().await?;
    let id = format!("mysql-correlation-{}", uuid::Uuid::new_v4());
    let correlation = format!("{id}/{}", "x".repeat(1_024));
    let waiting = FlowContinuation::waiting(
        FlowState::new(format!("{id}-one"), "mysql-contract", [], "node-a").suspended(),
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
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "MySQL indexed wait was not found"))?;
    assert_eq!(found.state().id(), waiting.state().id());
    assert!(
        store
            .get_by_wait_correlation("mysql-correlation/missing")
            .await?
            .is_none()
    );

    let ready = waiting
        .clone()
        .ready()
        .with_state(waiting.state().clone().next_version());
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
                    FlowState::new(format!("{id}-{suffix}"), "mysql-contract", [], "node-a")
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
            .expect_err("ambiguous MySQL correlation must not select a continuation")
            .code(),
        ErrorCode::Conflict
    );
    Ok(())
}
