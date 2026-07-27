#![doc = "SQLite integration coverage for the feature-gated FlowStore."]
#![cfg(feature = "sqlite")]

use std::time::{Duration, SystemTime};

use catga_codec_memorypack::MemoryPackable;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStatus, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition,
    WaitPolicy,
};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

#[path = "../../../tests/flow/timeout_store_contract.rs"]
mod timeout_store_contract;

#[tokio::test]
async fn sqlite_suspended_query_filters_before_its_scan_limit() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("filtered-suspended.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    assert!(
        store
            .create(FlowContinuation::new(
                FlowState::new("old-unrelated", "unrelated", [], "node-a"),
                "finish",
            ))
            .await?
    );

    let range_start = SystemTime::now();
    let matching = FlowContinuation::new(
        FlowState::new("matching-suspended", "payment", [], "node-a").suspended(),
        "finish",
    );
    assert!(store.create(matching).await?);
    let range_end = SystemTime::now() + Duration::from_secs(1);

    let query = FlowQuery::new(1, 1)?
        .with_status(FlowStatus::Suspended)
        .with_flow_type("payment")
        .created_between(range_start, range_end)?;
    let summaries = store.query(&query).await?;

    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id(), "matching-suspended");
    Ok(())
}

#[tokio::test]
async fn sqlite_flow_scheduler_is_idempotent_bounded_and_lease_fenced() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("schedules.db");
    let url = format!("sqlite://{}", database.display());
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;
    let due = SystemTime::now() + Duration::from_secs(10);

    let schedule_id = scheduler
        .schedule_resume("scheduler-flow", "charge", due)
        .await?;
    assert_eq!(
        scheduler
            .schedule_resume("scheduler-flow", "charge", due + Duration::from_secs(1))
            .await?,
        schedule_id
    );
    let first = scheduler
        .claim_due("worker-a", due, Duration::from_secs(30), 2)
        .await?;
    assert_eq!(first.len(), 1);
    assert!(
        scheduler
            .claim_due("worker-b", due, Duration::from_secs(30), 2)
            .await?
            .is_empty()
    );
    let claimed = first
        .iter()
        .find(|resume| resume.schedule_id() == schedule_id.as_ref())
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "initial schedule was not claimed"))?;
    assert!(!scheduler.cancel_resume(claimed.schedule_id()).await?);
    assert!(
        scheduler
            .renew_due(
                "worker-a",
                claimed.schedule_id(),
                due + Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await?
    );
    assert!(
        scheduler
            .release_due("worker-a", claimed.schedule_id())
            .await?
    );
    let reclaimed = scheduler
        .claim_due(
            "worker-b",
            due + Duration::from_secs(2),
            Duration::from_secs(30),
            3,
        )
        .await?;
    let resumed = reclaimed
        .iter()
        .find(|resume| resume.schedule_id() == schedule_id.as_ref())
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "released schedule was not reclaimed")
        })?;
    assert!(scheduler.ack_due("worker-b", resumed.schedule_id()).await?);
    assert!(!scheduler.ack_due("worker-b", resumed.schedule_id()).await?);

    for state_id in ["reserve", "notify", "receipt"] {
        scheduler
            .schedule_resume("scheduler-flow", state_id, due)
            .await?;
    }
    assert_eq!(
        scheduler
            .claim_due("worker-c", due, Duration::from_secs(30), 2)
            .await?
            .len(),
        2
    );
    Ok(())
}

#[derive(Clone, Debug, Eq, MemoryPackable, PartialEq)]
struct PersistedStateMachine {
    paid: bool,
    quantity: u32,
}

#[tokio::test]
async fn sqlite_state_machine_store_preserves_snapshots_and_version_cas() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("state-machines.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlStateMachineStore::<PersistedStateMachine>::connect_sqlite(&url).await?;
    store.migrate().await?;

    let initial = StateMachineSnapshot::new(
        "order-7",
        PersistedStateMachine {
            quantity: 3,
            paid: false,
        },
    );
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert_eq!(store.get("order-7").await?, Some(initial.clone()));

    let next = initial.next_version(PersistedStateMachine {
        quantity: 3,
        paid: true,
    })?;
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), next.clone()).await?);
    assert_eq!(store.get("order-7").await?, Some(next));

    let racing = StateMachineSnapshot::new(
        "order-race",
        PersistedStateMachine {
            quantity: 1,
            paid: false,
        },
    );
    assert!(store.create(racing.clone()).await?);
    let first_next = racing.next_version(PersistedStateMachine {
        quantity: 2,
        paid: true,
    })?;
    let second_next = racing.next_version(PersistedStateMachine {
        quantity: 3,
        paid: true,
    })?;
    let (first, second) = tokio::join!(
        store.update(racing.version(), first_next),
        store.update(racing.version(), second_next),
    );
    assert_eq!(usize::from(first?) + usize::from(second?), 1);
    Ok(())
}

#[tokio::test]
async fn sqlite_state_machine_store_rejects_oversized_snapshots() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("state-machine-payload-limit.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlStateMachineStore::<Vec<u8>>::connect_sqlite(&url).await?;
    store.migrate().await?;

    let oversized = StateMachineSnapshot::new("large-state", vec![0; 1024 * 1024 + 1]);
    let error = store
        .create(oversized)
        .await
        .expect_err("snapshot must be bounded");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn sqlite_dsl_progress_store_preserves_versioned_step_progress() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("dsl-progress.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let initial = DslStepProgress::new("sql-dsl-progress", 4, b"initial".as_slice());
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert_eq!(
        store.get("sql-dsl-progress", 4).await?,
        Some(initial.clone())
    );

    let next = initial.clone().next_version(b"next".as_slice())?;
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), initial).await?);
    assert_eq!(store.get("sql-dsl-progress", 4).await?, Some(next));
    assert!(store.delete("sql-dsl-progress", 4).await?);
    assert!(!store.delete("sql-dsl-progress", 4).await?);
    assert!(store.get("sql-dsl-progress", 4).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn sqlite_flow_store_creates_and_loads_a_flow_state() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("flows.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let state = FlowState::new("sql-flow-1", "payment", b"input".as_slice(), "node-a");
    assert!(store.create(state.clone()).await?);
    assert_eq!(store.get(state.id()).await?, Some(state));
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_preserves_wait_results_and_summary_bounds() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("suspended.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let waiting = FlowContinuation::waiting(
        FlowState::new("sql-wait-1", "payment", [], "node-a").suspended(),
        "finish",
        WaitCondition::new(
            "sql-wait-1/correlation",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    let created_at = waiting.created_at();
    assert!(store.create(waiting.clone()).await?);
    assert_eq!(store.get("sql-wait-1").await?, Some(waiting.clone()));
    tokio::time::sleep(Duration::from_millis(1)).await;
    assert!(
        store
            .record_wait_success("sql-wait-1", 0, "child-a", b"ok".to_vec())
            .await?
    );
    let persisted = required(store.get("sql-wait-1").await?, "persisted continuation")?;
    let wait = required(persisted.wait(), "persisted wait condition")?;
    let result = required(wait.results().first(), "persisted wait result")?;
    assert_eq!(result.payload(), Some(&b"ok"[..]));

    let summaries = store.query(&FlowQuery::new(1, 1)?).await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(
        required(summaries.first(), "flow summary")?.id(),
        "sql-wait-1"
    );
    assert!(
        required(summaries.first(), "flow summary")?.updated_at() > created_at,
        "successful wait-result persistence must refresh the discovery timestamp"
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_looks_up_one_active_wait_by_correlation() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("suspended-correlation.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let waiting = FlowContinuation::waiting(
        FlowState::new("sql-correlation-1", "payment", [], "node-a").suspended(),
        "finish",
        WaitCondition::new(
            "sql-correlation/one",
            WaitPolicy::All,
            1,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(waiting).await?);

    let found = required(
        store.get_by_wait_correlation("sql-correlation/one").await?,
        "indexed wait correlation",
    )?;
    assert_eq!(found.state().id(), "sql-correlation-1");
    assert!(
        store
            .get_by_wait_correlation("sql-correlation/missing")
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_rejects_an_ambiguous_wait_correlation() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("ambiguous-suspended-correlation.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    for flow_id in ["sql-ambiguous-correlation-1", "sql-ambiguous-correlation-2"] {
        assert!(
            store
                .create(FlowContinuation::waiting(
                    FlowState::new(flow_id, "payment", [], "node-a").suspended(),
                    "finish",
                    WaitCondition::new(
                        "sql-correlation/shared",
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
            .get_by_wait_correlation("sql-correlation/shared")
            .await
            .expect_err("ambiguous correlation must not select a continuation")
            .code(),
        ErrorCode::Conflict
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_summary_preserves_submillisecond_creation_time() -> CatgaResult<()>
{
    let directory = temporary_directory()?;
    let database = directory.path().join("summary-precision.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let continuation = FlowContinuation::new(
        FlowState::new("sql-summary-precision", "payment", [], "node-a"),
        "finish",
    );
    let created_at = continuation.created_at();
    assert!(store.create(continuation).await?);

    let summaries = store.query(&FlowQuery::new(1, 1)?).await?;
    assert_eq!(
        required(summaries.first(), "precise flow summary")?.created_at(),
        created_at
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_migrates_an_existing_table_with_discovery_order_index()
-> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("existing-continuations.db");
    let url = format!("sqlite://{}", database.display());
    std::fs::File::create(&database).map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create existing continuation database")
            .with_details(error.to_string())
    })?;
    let pool = sqlx::SqlitePool::connect(&url)
        .await
        .map_err(|error| test_database_error("connect existing continuation database", error))?;
    sqlx::query(
        "CREATE TABLE catga_flow_continuations (\
         flow_key BLOB PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
         flow_type TEXT NOT NULL, status INTEGER NOT NULL, version INTEGER NOT NULL, \
         created_at_ms INTEGER NOT NULL, deadline_ms INTEGER NULL, revision INTEGER NOT NULL, \
         due_token BLOB NULL, lease_until_ms INTEGER NULL, payload BLOB NOT NULL)",
    )
    .execute(&pool)
    .await
    .map_err(|error| test_database_error("create existing continuation table", error))?;
    drop(pool);

    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let index_sql: String =
        sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type = 'index' \
         AND name = 'catga_flow_continuations_order_idx'",
        )
        .fetch_one(&sqlx::SqlitePool::connect(&url).await.map_err(|error| {
            test_database_error("connect discovery-index inspection pool", error)
        })?)
        .await
        .map_err(|error| test_database_error("read continuation discovery index", error))?;
    assert!(index_sql.contains("created_at_ms, created_at_subsec_ns, flow_key"));
    Ok(())
}

#[tokio::test]
async fn sqlite_suspended_store_uses_full_snapshot_claims_and_versioned_updates() -> CatgaResult<()>
{
    let directory = temporary_directory()?;
    let database = directory.path().join("suspended-cas.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let running = FlowContinuation::new(
        FlowState::new("sql-suspended-cas", "payment", [], "node-a"),
        "finish",
    );
    assert!(store.create(running.clone()).await?);
    assert!(store.heartbeat("sql-suspended-cas", "node-a", 0).await?);
    let claimed = running.clone().with_state(
        running
            .state()
            .clone()
            .claimed_by("node-b")
            .next_version()?,
    );
    assert!(!store.claim(&running, claimed).await?);

    let current = required(
        store.get("sql-suspended-cas").await?,
        "current suspended continuation",
    )?;
    let next = current
        .clone()
        .ready()
        .with_state(current.state().clone().suspended().next_version()?);
    assert!(store.update(current.state().version(), next).await?);
    assert!(store.delete("sql-suspended-cas", 1).await?);
    assert!(store.get("sql-suspended-cas").await?.is_none());
    Ok(())
}

#[tokio::test]
async fn sqlite_flow_store_uses_version_cas_stale_claims_and_owner_heartbeats() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("flow-cas.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let initial = FlowState::new("sql-flow-cas", "payment", [], "node-a");
    assert!(store.create(initial.clone()).await?);
    let next = initial.clone().next_version()?;
    assert!(store.update(initial.version(), next.clone()).await?);
    assert!(!store.update(initial.version(), initial).await?);

    let stale = FlowState::new("sql-flow-stale", "payment", [], "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await?);
    let claimed = required(
        store
            .try_claim("payment", "node-b", Duration::from_secs(86_400))
            .await?,
        "claimed stale flow",
    )?;
    assert_eq!(claimed.id(), "sql-flow-stale");
    assert_eq!(claimed.owner(), Some("node-b"));
    assert_eq!(claimed.version(), 1);
    assert!(
        store
            .heartbeat(claimed.id(), "node-b", claimed.version())
            .await?
    );
    let persisted = required(store.get(claimed.id()).await?, "claimed flow state")?;
    assert_eq!(persisted.version(), 1);
    Ok(())
}

#[tokio::test]
async fn sqlite_timeout_receipts_are_leased_released_and_token_fenced() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("timeout-receipts.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);
    let continuation = FlowContinuation::waiting(
        FlowState::new("sql-timeout-1", "payment", [], "node-a").suspended(),
        "finish",
        WaitCondition::new(
            "sql-timeout-1/wait",
            WaitPolicy::All,
            1,
            now - Duration::from_secs(2),
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(continuation).await?);
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 0);

    let poll = TimedOutFlowPoll::new(now, 1, 1)?;
    let first = required(
        store.poll_timed_out(&poll).await?.pop(),
        "first timeout receipt",
    )?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 1);
    assert_eq!(first.flow_id(), "sql-timeout-1");
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    let mut forged_token = first.token().to_vec();
    *required(forged_token.first_mut(), "timeout receipt token byte")? ^= u8::MAX;
    let forged = TimedOutFlowReceipt::new(first.flow_id(), forged_token);
    store.release_timed_out(&forged).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    store.release_timed_out(&first).await?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 2);
    let second = required(
        store.poll_timed_out(&poll).await?.pop(),
        "released timeout receipt",
    )?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 3);
    assert_ne!(second.token(), first.token());

    store.ack_timed_out(&first).await?;
    assert!(store.poll_timed_out(&poll).await?.is_empty());

    let after_lease = TimedOutFlowPoll::new(now + Duration::from_secs(60), 1, 1)?;
    let third = required(
        store.poll_timed_out(&after_lease).await?.pop(),
        "expired timeout receipt lease",
    )?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 4);
    assert_ne!(third.token(), second.token());

    store.ack_timed_out(&second).await?;
    assert!(store.poll_timed_out(&after_lease).await?.is_empty());

    store.release_timed_out(&third).await?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 5);
    let fourth = required(
        store.poll_timed_out(&after_lease).await?.pop(),
        "second released timeout receipt",
    )?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 6);
    assert_ne!(fourth.token(), third.token());
    store.ack_timed_out(&fourth).await?;
    assert_eq!(stored_revision(&url, "sql-timeout-1").await?, 7);
    assert!(store.poll_timed_out(&after_lease).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn sqlite_timeout_poll_is_bounded_and_reconciles_stale_waits() -> CatgaResult<()> {
    let directory = temporary_directory()?;
    let database = directory.path().join("timeout-contract.db");
    let url = format!("sqlite://{}", database.display());
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;

    timeout_store_contract::run_timeout_store_contract(&store, "sqlite-timeout", false).await
}

fn temporary_directory() -> CatgaResult<tempfile::TempDir> {
    tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create SQLite test directory")
            .with_details(error.to_string())
    })
}

fn required<T>(value: Option<T>, description: &'static str) -> CatgaResult<T> {
    value.ok_or_else(|| CatgaError::new(ErrorCode::Internal, description))
}

async fn stored_revision(url: &str, flow_id: &str) -> CatgaResult<i64> {
    let pool = sqlx::SqlitePool::connect(url)
        .await
        .map_err(|error| test_database_error("connect revision inspection pool", error))?;
    sqlx::query_scalar("SELECT revision FROM catga_flow_continuations WHERE flow_id = ?")
        .bind(flow_id)
        .fetch_one(&pool)
        .await
        .map_err(|error| test_database_error("read continuation revision", error))
}

fn test_database_error(description: &'static str, error: sqlx::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, description).with_details(error.to_string())
}
