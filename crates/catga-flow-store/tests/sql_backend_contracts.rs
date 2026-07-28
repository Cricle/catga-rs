//! Cross-dialect SQL FlowStore boundary contracts.
//!
//! The real-service cases are ignored by default and require the corresponding
//! `CATGA_*_URL` environment variable.  CI enables them through the Docker E2E
//! profile; the URL parser tests always run and must fail before network I/O.

#![cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]

use std::{
    env,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowState, FlowStore, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_flow_store::{SqlFlowStore, SqlSuspendedFlowStore};
use tokio::sync::Mutex;

// Timeout receipts are leased from one backend-wide due queue. These E2E contracts use a
// bounded global poll, so they must not claim one another's deliberately expired records.
static SQL_TIMEOUT_POLL_LOCK: Mutex<()> = Mutex::const_new(());

/// Returns the configured external service URL or skips a locally disabled E2E case.
fn external_url(variable: &str) -> CatgaResult<Option<Box<str>>> {
    match env::var(variable) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value.into_boxed_str())),
        _ if env::var_os("CATGA_REQUIRE_EXTERNAL_SERVICES")
            .is_some_and(|value| !value.is_empty()) =>
        {
            Err(CatgaError::new(
                ErrorCode::Unavailable,
                format!("{variable} must be configured when SQL E2E is required"),
            ))
        }
        _ => Ok(None),
    }
}

/// Makes sure connection-string parser failures retain a stable retryable boundary error.
fn assert_invalid_url(error: CatgaError, backend: &str) {
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(error.is_retryable());
    assert!(error.message().contains(backend));
}

/// Validates the durable contract shared by all real SQL servers.
///
/// This exercises two easy-to-regress boundaries: concurrent same-identity admission must be
/// idempotent, and rows whose deadline is SQL `NULL` must not be selected by an expired-wait
/// scan.  The latter prevents a generic suspended continuation from being treated as timed out.
async fn suspended_null_and_cas_contract<S>(store: &S, backend: &str) -> CatgaResult<()>
where
    S: SuspendedFlowStore + TimedOutFlowStore + Sync,
{
    let _timeout_poll_guard = SQL_TIMEOUT_POLL_LOCK.lock().await;
    let prefix = format!("{backend}-sql-boundary-{}", uuid::Uuid::new_v4());
    let plain_id = format!("{prefix}/plain");
    let plain = FlowContinuation::new(
        FlowState::new(plain_id.as_str(), "sql-boundary", [], "node-a").suspended(),
        "resume",
    );

    let (first, second) = tokio::join!(store.create(plain.clone()), store.create(plain.clone()));
    assert_eq!(usize::from(first?) + usize::from(second?), 1);

    let claimed = plain
        .clone()
        .with_state(plain.state().clone().claimed_by("owner-a").next_version()?);
    let (first, second) = tokio::join!(
        store.claim(&plain, claimed.clone()),
        store.claim(&plain, claimed.clone()),
    );
    assert_eq!(usize::from(first?) + usize::from(second?), 1);
    assert_eq!(store.get(plain_id.as_str()).await?, Some(claimed));

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
    let waiting_id = format!("{prefix}/waiting");
    let waiting = FlowContinuation::waiting(
        FlowState::new(waiting_id.as_str(), "sql-boundary", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            format!("{prefix}/correlation"),
            WaitPolicy::All,
            1,
            now - Duration::from_secs(2),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(waiting).await?);

    let receipts = store
        .poll_timed_out(&TimedOutFlowPoll::new(now, 4, 16)?)
        .await?;
    assert!(
        receipts
            .iter()
            .any(|receipt| receipt.flow_id() == waiting_id),
        "the expired wait created by this test must be discoverable"
    );
    assert!(
        receipts.iter().all(|receipt| receipt.flow_id() != plain_id),
        "a continuation without a wait must retain a SQL NULL deadline and never be discovered"
    );
    for receipt in receipts {
        store.release_timed_out(&receipt).await?;
    }
    Ok(())
}

/// Exercises the public plain-flow rejection and owner-fencing boundaries.
///
/// The records use a test-unique namespace because MySQL and PostgreSQL E2E tests share a
/// long-lived service database and run concurrently. The plain FlowStore API intentionally has
/// no deletion operation, so these rows remain harmlessly isolated rather than risking broad
/// cleanup against another test's data.
async fn flow_store_rejection_and_heartbeat_contract<S>(store: &S, backend: &str) -> CatgaResult<()>
where
    S: FlowStore + Sync,
{
    let prefix = format!("{backend}-flow-boundary-{}", uuid::Uuid::new_v4());
    let missing_id = format!("{prefix}/missing");
    let flow_id = format!("{prefix}/flow");
    let flow_type = format!("{prefix}/type");
    let initial = FlowState::new(flow_id.as_str(), flow_type.as_str(), [], "node-a");

    assert!(store.get(missing_id.as_str()).await?.is_none());
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert!(!store.update(initial.version(), initial.clone()).await?);
    assert!(
        !store
            .update(
                0,
                FlowState::new(missing_id.as_str(), flow_type.as_str(), [], "node-a")
                    .next_version()?,
            )
            .await?
    );

    let progressed = initial.clone().next_version()?;
    assert!(store.update(initial.version(), progressed.clone()).await?);
    assert!(!store.update(initial.version(), progressed).await?);

    let fresh_id = format!("{prefix}/fresh");
    assert!(
        store
            .create(FlowState::new(
                fresh_id.as_str(),
                flow_type.as_str(),
                [],
                "node-a",
            ))
            .await?
    );
    assert!(
        store
            .try_claim(flow_type.as_str(), "worker-a", Duration::from_secs(1))
            .await?
            .is_none()
    );

    let stale_id = format!("{prefix}/stale");
    assert!(
        store
            .create(
                FlowState::new(stale_id.as_str(), flow_type.as_str(), [], "node-a")
                    .heartbeated_at(SystemTime::UNIX_EPOCH),
            )
            .await?
    );
    assert!(
        store
            .try_claim("other-flow-type", "worker-a", Duration::from_secs(1))
            .await?
            .is_none()
    );
    let claimed = store
        .try_claim(flow_type.as_str(), "worker-a", Duration::from_secs(1))
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "stale flow was not claimed"))?;
    assert_eq!(claimed.id(), stale_id);
    assert_eq!(claimed.owner(), Some("worker-a"));
    assert!(
        !store
            .heartbeat(stale_id.as_str(), "worker-b", claimed.version())
            .await?
    );
    assert!(
        !store
            .heartbeat(stale_id.as_str(), "worker-a", claimed.version() + 1)
            .await?
    );
    assert!(
        store
            .heartbeat(stale_id.as_str(), "worker-a", claimed.version())
            .await?
    );
    assert!(
        !store
            .heartbeat(missing_id.as_str(), "worker-a", claimed.version())
            .await?
    );
    let heartbeated = store
        .get(stale_id.as_str())
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed flow disappeared"))?;
    assert_eq!(heartbeated.version(), claimed.version());
    assert_eq!(heartbeated.owner(), Some("worker-a"));
    assert!(
        store
            .try_claim(flow_type.as_str(), "worker-b", Duration::from_secs(1))
            .await?
            .is_none()
    );
    Ok(())
}

/// Exercises timeout receipt fencing, release recovery, and acknowledgement through public APIs.
async fn timeout_receipt_recovery_contract<S>(store: &S, backend: &str) -> CatgaResult<()>
where
    S: SuspendedFlowStore + TimedOutFlowStore + Sync,
{
    let _timeout_poll_guard = SQL_TIMEOUT_POLL_LOCK.lock().await;
    let prefix = format!("{backend}-timeout-boundary-{}", uuid::Uuid::new_v4());
    let flow_id = format!("{prefix}/waiting");
    // Use the earliest valid timeout instant. Other E2E cases use later fixed deadlines, so this
    // bounded global due-index poll cannot lease their records in the shared service database.
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let continuation = FlowContinuation::waiting(
        FlowState::new(flow_id.as_str(), "timeout-boundary", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            format!("{prefix}/correlation"),
            WaitPolicy::All,
            1,
            SystemTime::UNIX_EPOCH,
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(continuation).await?);

    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(receipts.len(), 1);
    let receipt = receipts
        .into_iter()
        .next()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "expired flow was not leased"))?;
    assert_eq!(receipt.flow_id(), flow_id);
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    let invalid = TimedOutFlowReceipt::new(receipt.flow_id(), []);
    assert_eq!(
        store
            .release_timed_out(&invalid)
            .await
            .expect_err("malformed timeout receipt must be rejected")
            .code(),
        ErrorCode::Validation
    );
    let forged = TimedOutFlowReceipt::new(receipt.flow_id(), [0_u8; 16]);
    store.ack_timed_out(&forged).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    store.release_timed_out(&receipt).await?;
    let recovered = store
        .poll_timed_out(&poll)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "released receipt was not recovered")
        })?;
    assert_eq!(recovered.flow_id(), flow_id);
    assert_ne!(recovered.token(), receipt.token());
    store.ack_timed_out(&recovered).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());
    assert!(store.delete(flow_id.as_str(), 0).await?);
    Ok(())
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_rejects_a_malformed_url_before_network_io() {
    let result = SqlFlowStore::connect_mysql("mysql://%").await;
    let error = match result {
        Ok(_) => panic!("malformed MySQL URL unexpectedly connected"),
        Err(error) => error,
    };
    assert_invalid_url(error, "MySQL");
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_rejects_a_malformed_url_before_network_io() {
    let result = SqlFlowStore::connect_postgres("postgres://%").await;
    let error = match result {
        Ok(_) => panic!("malformed PostgreSQL URL unexpectedly connected"),
        Err(error) => error,
    };
    assert_invalid_url(error, "PostgreSQL");
}

#[cfg(feature = "mssql")]
#[tokio::test]
async fn mssql_rejects_a_malformed_url_before_network_io() {
    let result = SqlFlowStore::connect_mssql("not a SQL Server connection string").await;
    let error = match result {
        Ok(_) => panic!("malformed SQL Server URL unexpectedly connected"),
        Err(error) => error,
    };
    assert_invalid_url(error, "SQL Server");
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_e2e_preserves_null_deadlines_and_concurrent_cas() -> CatgaResult<()> {
    let Some(url) = external_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mysql(url.as_ref()).await?;
    store.migrate().await?;
    suspended_null_and_cas_contract(&store, "mysql").await
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_e2e_rejects_stale_flow_transitions_and_fences_heartbeats() -> CatgaResult<()> {
    let Some(url) = external_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let store = SqlFlowStore::connect_mysql(url.as_ref()).await?;
    store.migrate().await?;
    flow_store_rejection_and_heartbeat_contract(&store, "mysql").await
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_e2e_recovers_released_timeout_receipts_and_fences_acknowledgements()
-> CatgaResult<()> {
    let Some(url) = external_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mysql(url.as_ref()).await?;
    store.migrate().await?;
    timeout_receipt_recovery_contract(&store, "mysql").await
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_e2e_preserves_null_deadlines_and_concurrent_cas() -> CatgaResult<()> {
    let Some(url) = external_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    suspended_null_and_cas_contract(&store, "postgres").await
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_e2e_rejects_stale_flow_transitions_and_fences_heartbeats() -> CatgaResult<()> {
    let Some(url) = external_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    flow_store_rejection_and_heartbeat_contract(&store, "postgres").await
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_e2e_recovers_released_timeout_receipts_and_fences_acknowledgements()
-> CatgaResult<()> {
    let Some(url) = external_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_postgres(url.as_ref()).await?;
    store.migrate().await?;
    timeout_receipt_recovery_contract(&store, "postgres").await
}

#[cfg(feature = "mssql")]
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_e2e_preserves_null_deadlines_and_concurrent_cas() -> CatgaResult<()> {
    let Some(url) = external_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    suspended_null_and_cas_contract(&store, "mssql").await
}
