//! SQL Server real-service integration coverage.
#![cfg(feature = "mssql")]

use std::time::{Duration, SystemTime};

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStatus, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition,
    WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

type MssqlAdminPool = bb8::Pool<bb8_tiberius::ConnectionManager>;

#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
struct MssqlCoverageState {
    attempts: u32,
}

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
async fn mssql_all_adapter_migrations_serialize_on_an_empty_database() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, isolated_url, database) = create_mssql_test_database(url.as_ref()).await?;
    let result = async {
        let flow_first = SqlFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let flow_second = SqlFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let suspended_first = SqlSuspendedFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let suspended_second = SqlSuspendedFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let progress_first = SqlDslStepProgressStore::connect_mssql(isolated_url.as_str()).await?;
        let progress_second = SqlDslStepProgressStore::connect_mssql(isolated_url.as_str()).await?;
        let snapshot_first =
            SqlStateMachineStore::<MssqlCoverageState>::connect_mssql(isolated_url.as_str())
                .await?;
        let snapshot_second =
            SqlStateMachineStore::<MssqlCoverageState>::connect_mssql(isolated_url.as_str())
                .await?;
        let scheduler_first = SqlFlowScheduler::connect_mssql(isolated_url.as_str()).await?;
        let scheduler_second = SqlFlowScheduler::connect_mssql(isolated_url.as_str()).await?;

        let (
            flow_first,
            flow_second,
            suspended_first,
            suspended_second,
            progress_first,
            progress_second,
            snapshot_first,
            snapshot_second,
            scheduler_first,
            scheduler_second,
        ) = tokio::join!(
            flow_first.migrate(),
            flow_second.migrate(),
            suspended_first.migrate(),
            suspended_second.migrate(),
            progress_first.migrate(),
            progress_second.migrate(),
            snapshot_first.migrate(),
            snapshot_second.migrate(),
            scheduler_first.migrate(),
            scheduler_second.migrate(),
        );
        flow_first?;
        flow_second?;
        suspended_first?;
        suspended_second?;
        progress_first?;
        progress_second?;
        snapshot_first?;
        snapshot_second?;
        scheduler_first?;
        scheduler_second
    }
    .await;
    let cleanup = drop_mssql_test_database(&admin, database.as_str()).await;
    result?;
    cleanup
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
async fn mssql_state_machine_store_rejects_absent_and_invalid_transitions() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlStateMachineStore::<MssqlCoverageState>::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("mssql-snapshot-edges-{}", uuid::Uuid::new_v4());
    let initial = StateMachineSnapshot::new(id.as_str(), MssqlCoverageState { attempts: 0 });
    let next = initial.next_version(MssqlCoverageState { attempts: 1 })?;

    assert!(store.get(id.as_str()).await?.is_none());
    assert!(!store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), initial.clone()).await?);
    assert!(store.create(initial.clone()).await?);
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), next).await?);
    Ok(())
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
    sql_contracts::dsl_progress_contract(&store, "mssql-e2e").await?;
    sql_contracts::dsl_flow_restart_contract(&store, "mssql-e2e").await
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
    assert!(!store.update(initial.version(), initial.clone()).await?);
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
    assert!(
        !store
            .heartbeat(id.as_str(), "node-a", initial.version())
            .await?
    );
    assert!(!store.update(initial.version(), absent_next).await?);

    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert!(!store.update(initial.version(), initial.clone()).await?);
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
async fn mssql_timeout_receipts_are_leased_released_acknowledged_and_token_fenced()
-> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let id = format!("mssql-timeout-{}", uuid::Uuid::new_v4());
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
    assert!(
        store
            .create(FlowContinuation::waiting(
                FlowState::new(id.as_str(), "payment", [], "node-a").suspended(),
                "finish",
                WaitCondition::new(
                    format!("{id}/wait"),
                    WaitPolicy::All,
                    1,
                    now - Duration::from_secs(2),
                    Duration::from_secs(1),
                ),
            ))
            .await?
    );

    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    let first =
        store.poll_timed_out(&poll).await?.pop().ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "missing first SQL Server receipt")
        })?;
    assert_eq!(first.flow_id(), id);
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    let malformed = TimedOutFlowReceipt::new(first.flow_id(), [0_u8; 15]);
    assert_eq!(
        store
            .release_timed_out(&malformed)
            .await
            .expect_err("short SQL Server receipt tokens must be rejected")
            .code(),
        ErrorCode::Validation
    );
    let mut forged_token = first.token().to_vec();
    forged_token[0] ^= u8::MAX;
    let forged = TimedOutFlowReceipt::new(first.flow_id(), forged_token);
    store.release_timed_out(&forged).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    store.release_timed_out(&first).await?;
    let second = store.poll_timed_out(&poll).await?.pop().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "missing released SQL Server receipt")
    })?;
    assert_ne!(second.token(), first.token());
    store.ack_timed_out(&first).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    let after_lease = TimedOutFlowPoll::new(now + Duration::from_secs(60), 1, 1)?;
    let third = store
        .poll_timed_out(&after_lease)
        .await?
        .pop()
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "missing expired SQL Server receipt")
        })?;
    assert_ne!(third.token(), second.token());
    store.ack_timed_out(&second).await?;
    assert!(store.poll_timed_out(&after_lease).await?.is_empty());

    store.release_timed_out(&third).await?;
    let fourth = store
        .poll_timed_out(&after_lease)
        .await?
        .pop()
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "missing re-released SQL Server receipt",
            )
        })?;
    store.ack_timed_out(&fourth).await?;
    assert!(store.poll_timed_out(&after_lease).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_suspended_store_rejects_duplicate_wait_correlations_and_queries_unfiltered()
-> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let store = SqlSuspendedFlowStore::connect_mssql(url.as_ref()).await?;
    store.migrate().await?;
    let prefix = format!("mssql-duplicate-correlation-{}", uuid::Uuid::new_v4());
    let correlation = format!("{prefix}/wait");
    for suffix in ["first", "second"] {
        assert!(
            store
                .create(FlowContinuation::waiting(
                    FlowState::new(
                        format!("{prefix}-{suffix}"),
                        "duplicate-correlation",
                        [],
                        "node-a",
                    )
                    .suspended(),
                    "finish",
                    WaitCondition::new(
                        correlation.as_str(),
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
            .get_by_wait_correlation(correlation.as_str())
            .await
            .expect_err("ambiguous SQL Server wait correlations must be rejected")
            .code(),
        ErrorCode::Conflict
    );
    assert!(store.query(&FlowQuery::new(1, 1)?).await?.len() <= 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_public_adapters_surface_schema_errors_before_migration() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, isolated_url, database) = create_mssql_test_database(url.as_ref()).await?;
    let result = async {
        let flow = SqlFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let suspended = SqlSuspendedFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let progress = SqlDslStepProgressStore::connect_mssql(isolated_url.as_str()).await?;
        let snapshots =
            SqlStateMachineStore::<MssqlCoverageState>::connect_mssql(isolated_url.as_str())
                .await?;
        let scheduler = SqlFlowScheduler::connect_mssql(isolated_url.as_str()).await?;
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);

        assert!(flow.get("not-migrated").await.is_err());
        assert!(suspended.get("not-migrated").await.is_err());
        assert!(progress.get("not-migrated", 0).await.is_err());
        assert!(snapshots.get("not-migrated").await.is_err());
        assert!(
            scheduler
                .claim_due("worker", now, Duration::from_secs(1), 1)
                .await
                .is_err()
        );
        assert!(
            suspended
                .poll_timed_out(&TimedOutFlowPoll::new(now, 1, 1)?)
                .await
                .is_err()
        );
        Ok(())
    }
    .await;
    let cleanup = drop_mssql_test_database(&admin, database.as_str()).await;
    result?;
    cleanup
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

/// Exercises concurrent physical-revision updates through every SQL Server adapter.
///
/// The public stores keep their logical version unchanged for several operations (claims,
/// wait-result writes, and physical record deletes).  This test uses a fresh database so the
/// concurrent operations cannot accidentally observe another E2E case's due records.  It proves
/// that an optimistic-CAS retry preserves every distinct update and that a single stale flow or
/// due schedule cannot be leased twice.
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_concurrent_physical_revision_paths_preserve_all_records() -> CatgaResult<()> {
    let Some(url) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, isolated_url, database) = create_mssql_test_database(url.as_ref()).await?;
    let result = async {
        let flow = SqlFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let suspended = SqlSuspendedFlowStore::connect_mssql(isolated_url.as_str()).await?;
        let progress = SqlDslStepProgressStore::connect_mssql(isolated_url.as_str()).await?;
        let snapshots =
            SqlStateMachineStore::<MssqlCoverageState>::connect_mssql(isolated_url.as_str())
                .await?;
        let scheduler = SqlFlowScheduler::connect_mssql(isolated_url.as_str()).await?;
        flow.migrate().await?;
        suspended.migrate().await?;
        progress.migrate().await?;
        snapshots.migrate().await?;
        scheduler.migrate().await?;

        let prefix = format!("mssql-concurrent-{}", uuid::Uuid::new_v4());
        let flow_type = format!("{prefix}/type");
        let stale_id = format!("{prefix}/stale-flow");
        assert!(
            flow.create(
                FlowState::new(stale_id.as_str(), flow_type.as_str(), [], "creator")
                    .heartbeated_at(SystemTime::UNIX_EPOCH),
            )
            .await?
        );
        let (first_claim, second_claim) = tokio::join!(
            flow.try_claim(flow_type.as_str(), "worker-a", Duration::from_secs(1)),
            flow.try_claim(flow_type.as_str(), "worker-b", Duration::from_secs(1)),
        );
        let first_claim = first_claim?;
        let second_claim = second_claim?;
        assert_eq!(
            usize::from(first_claim.is_some()) + usize::from(second_claim.is_some()),
            1,
            "exactly one concurrent worker may claim a stale flow"
        );
        let claimed = flow
            .get(stale_id.as_str())
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed flow disappeared"))?;
        assert!(matches!(claimed.owner(), Some("worker-a" | "worker-b")));

        let continuation_id = format!("{prefix}/continuation");
        let continuation = FlowContinuation::waiting(
            FlowState::new(
                continuation_id.as_str(),
                format!("{prefix}/continuation-type"),
                [],
                "node-a",
            )
            .suspended(),
            "resume",
            WaitCondition::new(
                format!("{prefix}/correlation"),
                WaitPolicy::All,
                2,
                SystemTime::now(),
                Duration::from_secs(30),
            ),
        );
        assert!(suspended.create(continuation).await?);
        let (first_result, second_result) = tokio::join!(
            suspended.record_wait_success(
                continuation_id.as_str(),
                0,
                "child-a",
                b"first".to_vec(),
            ),
            suspended.record_wait_success(
                continuation_id.as_str(),
                0,
                "child-b",
                b"second".to_vec(),
            ),
        );
        assert!(first_result?);
        assert!(second_result?);
        let persisted = suspended
            .get(continuation_id.as_str())
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "continuation disappeared"))?;
        assert_eq!(
            persisted
                .wait()
                .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "wait disappeared"))?
                .completed_count(),
            2,
            "concurrent child results must both survive the revision retry"
        );

        let progress_id = format!("{prefix}/progress");
        let initial_progress = DslStepProgress::new(progress_id.as_str(), 7, b"initial".as_slice());
        assert!(progress.create(initial_progress.clone()).await?);
        let first_progress = initial_progress.clone().next_version(b"first".as_slice())?;
        let second_progress = initial_progress.next_version(b"second".as_slice())?;
        let (first_update, second_update) = tokio::join!(
            progress.update(0, first_progress.clone()),
            progress.update(0, second_progress.clone()),
        );
        assert_eq!(usize::from(first_update?) + usize::from(second_update?), 1);
        let (first_delete, second_delete) = tokio::join!(
            progress.delete(progress_id.as_str(), 7),
            progress.delete(progress_id.as_str(), 7),
        );
        assert_eq!(usize::from(first_delete?) + usize::from(second_delete?), 1);
        assert!(progress.get(progress_id.as_str(), 7).await?.is_none());

        let snapshot_id = format!("{prefix}/snapshot");
        let initial_snapshot =
            StateMachineSnapshot::new(snapshot_id.as_str(), MssqlCoverageState { attempts: 0 });
        let (first_create, second_create) = tokio::join!(
            snapshots.create(initial_snapshot.clone()),
            snapshots.create(initial_snapshot.clone()),
        );
        assert_eq!(usize::from(first_create?) + usize::from(second_create?), 1);
        let first_snapshot = initial_snapshot.next_version(MssqlCoverageState { attempts: 1 })?;
        let second_snapshot = initial_snapshot.next_version(MssqlCoverageState { attempts: 2 })?;
        let (first_update, second_update) = tokio::join!(
            snapshots.update(0, first_snapshot),
            snapshots.update(0, second_snapshot),
        );
        assert_eq!(usize::from(first_update?) + usize::from(second_update?), 1);
        let persisted_snapshot = snapshots
            .get(snapshot_id.as_str())
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "snapshot disappeared"))?;
        assert_eq!(persisted_snapshot.version(), 1);
        assert!(matches!(persisted_snapshot.state().attempts, 1 | 2,));

        let due = SystemTime::now() - Duration::from_secs(1);
        let schedule_flow = format!("{prefix}/schedule-flow");
        let first_schedule = scheduler
            .schedule_resume(schedule_flow.as_str(), "first", due)
            .await?;
        let second_schedule = scheduler
            .schedule_resume(schedule_flow.as_str(), "second", due)
            .await?;
        let now = SystemTime::now();
        let (first_due, second_due) = tokio::join!(
            scheduler.claim_due("worker-a", now, Duration::from_secs(30), 1),
            scheduler.claim_due("worker-b", now, Duration::from_secs(30), 1),
        );
        let first_due = first_due?;
        let second_due = second_due?;
        let concurrently_claimed = first_due.len() + second_due.len();
        assert!(
            (1..=2).contains(&concurrently_claimed),
            "at least one worker must claim due work without duplicating a lease"
        );
        let remaining_due = scheduler
            .claim_due("worker-c", now, Duration::from_secs(30), 2)
            .await?;
        assert_eq!(concurrently_claimed + remaining_due.len(), 2);
        let mut schedule_ids = first_due
            .iter()
            .chain(&second_due)
            .chain(&remaining_due)
            .map(|scheduled| scheduled.schedule_id().to_owned())
            .collect::<Vec<_>>();
        schedule_ids.sort_unstable();
        let mut expected_schedule_ids: Vec<String> =
            vec![first_schedule.into(), second_schedule.into()];
        expected_schedule_ids.sort_unstable();
        assert_eq!(schedule_ids, expected_schedule_ids);
        for scheduled in &first_due {
            assert!(
                scheduler
                    .ack_due("worker-a", scheduled.schedule_id())
                    .await?
            );
        }
        for scheduled in &second_due {
            assert!(
                scheduler
                    .ack_due("worker-b", scheduled.schedule_id())
                    .await?
            );
        }
        for scheduled in &remaining_due {
            assert!(
                scheduler
                    .ack_due("worker-c", scheduled.schedule_id())
                    .await?
            );
        }
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
