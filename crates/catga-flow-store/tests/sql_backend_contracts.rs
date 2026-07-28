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
    FlowContinuation, FlowState, SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore,
    WaitCondition, WaitPolicy,
};
use catga_flow_store::{SqlFlowStore, SqlSuspendedFlowStore};

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
