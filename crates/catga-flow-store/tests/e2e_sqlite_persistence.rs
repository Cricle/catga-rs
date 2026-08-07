//! E2E tests for catga-flow-store with real SQLite backend.
//!
//! These tests verify flow persistence, state recovery, and SQL integration
//! using a real SQLite database file that survives process restarts.

use std::time::Duration;

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStatus, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};

fn database_error(operation: &str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(
        ErrorCode::Unavailable,
        format!("SQL FlowStore {operation} failed: {error}"),
    )
}

/// Tests that a flow survives across a simulated "restart" by reconnecting to the same database.
#[tokio::test]
async fn e2e_flow_persists_across_sqlite_restarts() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("restart-persistence.db");
    let url = format!("sqlite://{}", database.display());

    // First "session" - create a flow
    {
        let store = SqlFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let state = FlowState::new(
            "persistent-flow",
            "payment",
            b"initial-data".to_vec().into_boxed_slice(),
            "node-a",
        );
        assert!(store.create(state).await?);
    }

    // Simulate "restart" - reconnect to the same database
    {
        let store = SqlFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let recovered = store.get("persistent-flow").await?;
        assert!(recovered.is_some(), "flow must survive database reconnection");
        let state = recovered.unwrap();
        assert_eq!(state.id(), "persistent-flow");
        assert_eq!(state.flow_type(), "payment");
        assert_eq!(state.version(), 0);
        assert_eq!(state.owner(), Some("node-a"));
    }

    Ok(())
}

/// Tests that state machine snapshots persist across restarts.
#[tokio::test]
async fn e2e_state_machine_snapshots_persist_across_restarts() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("state-persistence.db");
    let url = format!("sqlite://{}", database.display());

    #[derive(Debug, Clone, PartialEq, MemoryPackable)]
    struct OrderState {
        items: u32,
        total: f64,
        paid: bool,
    }

    // First session - create and update snapshots
    {
        let store = SqlStateMachineStore::<OrderState>::connect_sqlite(&url).await?;
        store.migrate().await?;

        let initial = StateMachineSnapshot::new(
            "order-1",
            OrderState {
                items: 2,
                total: 99.99,
                paid: false,
            },
        );
        assert!(store.create(initial.clone()).await?);

        let next = initial.next_version(OrderState {
            items: 2,
            total: 99.99,
            paid: true,
        })?;
        assert!(store.update(initial.version(), next.clone()).await?);
    }

    // Restart - verify snapshots survived
    {
        let store = SqlStateMachineStore::<OrderState>::connect_sqlite(&url).await?;
        store.migrate().await?;

        let recovered = store.get("order-1").await?;
        assert!(recovered.is_some(), "state machine must survive restart");
        let snapshot = recovered.unwrap();
        assert_eq!(snapshot.state().items, 2);
        assert_eq!(snapshot.state().total, 99.99);
        assert!(snapshot.state().paid, "paid flag must persist");
        assert_eq!(snapshot.version(), 1);
    }

    Ok(())
}

/// Tests that DSL step progress persists and recovers.
#[tokio::test]
async fn e2e_dsl_progress_persists_across_restarts() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("dsl-progress-persistence.db");
    let url = format!("sqlite://{}", database.display());

    // First session - record progress
    {
        let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let step1 = DslStepProgress::new("pipeline-1", 0, b"step-1-data".to_vec());
        assert!(store.create(step1.clone()).await?);

        let step2 = step1.clone().next_version(b"step-2-data".to_vec())?;
        assert!(store.update(step1.version(), step2.clone()).await?);
    }

    // Restart - verify progress survived
    {
        let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let recovered = store.get("pipeline-1", 0).await?;
        assert!(recovered.is_some(), "DSL progress must survive restart");
        assert_eq!(recovered.unwrap().version(), 1);
    }

    Ok(())
}

/// Tests flow scheduler with pause and resume simulation.
#[tokio::test]
async fn e2e_scheduler_survives_restart_with_pending_schedules() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("scheduler-restart.db");
    let url = format!("sqlite://{}", database.display());

    // First session - schedule some flows
    {
        let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
        scheduler.migrate().await?;

        let due = std::time::SystemTime::now() + Duration::from_secs(10);
        let schedule_id = scheduler
            .schedule_resume("scheduled-flow-1", "process", due)
            .await?;
        assert!(!schedule_id.is_empty());
    }

    // Restart - claim due work
    {
        let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
        scheduler.migrate().await?;

        // Nothing is due yet
        let claimed = scheduler
            .claim_due(
                "worker-b",
                std::time::SystemTime::now(),
                Duration::from_secs(30),
                10,
            )
            .await?;
        assert!(claimed.is_empty(), "nothing should be due yet");

        // Advance time past due
        let past_due = std::time::SystemTime::now() + Duration::from_secs(20);
        let claimed = scheduler
            .claim_due("worker-b", past_due, Duration::from_secs(30), 10)
            .await?;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].flow_id(), "scheduled-flow-1");
        assert_eq!(claimed[0].state_id(), "process");
    }

    Ok(())
}

/// Tests suspended flows with wait conditions persist across restarts.
#[tokio::test]
async fn e2e_suspended_flows_with_waits_persist_across_restarts() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("suspended-wait-persistence.db");
    let url = format!("sqlite://{}", database.display());

    let flow_id = "suspended-wait-flow";
    let now = std::time::SystemTime::now();

    // First session - create a suspended flow with wait
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let waiting = FlowContinuation::waiting(
            FlowState::new(
                flow_id,
                "payment",
                b"awaiting-callback".to_vec(),
                "node-a",
            )
            .suspended(),
            "complete",
            WaitCondition::new(
                "payment-callback-123",
                WaitPolicy::All,
                1,
                now,
                Duration::from_secs(60),
            ),
        );
        assert!(store.create(waiting.clone()).await?);

        // Record success for the wait condition
        assert!(store
            .record_wait_success(flow_id, 0, "callback-1", b"payment-confirmed".to_vec())
            .await?);
    }

    // Restart - verify suspended flow survived
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let recovered = store.get(flow_id).await?;
        assert!(recovered.is_some(), "suspended flow must survive restart");

        let continuation = recovered.unwrap();
        assert_eq!(continuation.state().id(), flow_id);
        assert_eq!(continuation.state().status(), FlowStatus::Suspended);

        // Verify wait results survived
        let wait = continuation.wait().expect("wait must be preserved");
        assert_eq!(wait.results().len(), 1);
        assert_eq!(wait.results()[0].payload(), Some(&b"payment-confirmed"[..]));
    }

    Ok(())
}

/// Tests query capabilities after restart with multiple flow types.
#[tokio::test]
async fn e2e_flow_queries_work_after_restart() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("query-after-restart.db");
    let url = format!("sqlite://{}", database.display());

    // First session - create flows of different types and statuses
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let flows = [
            ("flow-a", "type-a", FlowStatus::Running),
            ("flow-b", "type-a", FlowStatus::Suspended),
            ("flow-c", "type-b", FlowStatus::Suspended),
            ("flow-d", "type-b", FlowStatus::Done),
        ];

        for (id, flow_type, status) in flows {
            let mut state = FlowState::new(id, flow_type, Vec::new(), "node-a");
            state = match status {
                FlowStatus::Running => state,
                FlowStatus::Suspended => state.suspended(),
                FlowStatus::Done => state.done(0),
                _ => state,
            };
            store
                .create(FlowContinuation::new(state, "finish"))
                .await?;
        }
    }

    // Restart - verify queries work correctly
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        // Query all suspended flows
        let suspended = store
            .query(&FlowQuery::new(10, 10).unwrap().with_status(FlowStatus::Suspended))
            .await?;
        assert_eq!(suspended.len(), 2);
        assert!(suspended.iter().all(|s| s.status() == FlowStatus::Suspended));

        // Query by flow type
        let type_a = store
            .query(&FlowQuery::new(10, 10).unwrap().with_flow_type("type-a"))
            .await?;
        assert_eq!(type_a.len(), 2);
        assert!(type_a.iter().all(|s| s.flow_type() == "type-a"));

        // Query combined filters
        let type_b_suspended = store
            .query(
                &FlowQuery::new(10, 10)
                    .unwrap()
                    .with_flow_type("type-b")
                    .with_status(FlowStatus::Suspended),
            )
            .await?;
        assert_eq!(type_b_suspended.len(), 1);
        assert_eq!(type_b_suspended[0].id(), "flow-c");
    }

    Ok(())
}

/// Tests timeout receipt workflow across restarts.
#[tokio::test]
async fn e2e_timeout_receipts_recover_across_restarts() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("timeout-receipt-restart.db");
    let url = format!("sqlite://{}", database.display());

    let flow_id = "timeout-flow";
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(5000);

    // First session - create a waiting continuation and poll for timeout
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let waiting = FlowContinuation::waiting(
            FlowState::new(flow_id, "payment", Vec::new(), "node-a").suspended(),
            "resume",
            WaitCondition::new(
                "timeout-correlator",
                WaitPolicy::All,
                1,
                now - Duration::from_secs(5),
                Duration::from_secs(1),
            ),
        );
        assert!(store.create(waiting).await?);

        // Poll and get timeout receipt
        let poll = TimedOutFlowPoll::new(now, 1, 1)?;
        let receipts = store.poll_timed_out(&poll).await?;
        assert_eq!(receipts.len(), 1);
        let receipt = receipts.into_iter().next().unwrap();
        assert_eq!(receipt.flow_id(), flow_id);

        // Release the receipt back
        store.release_timed_out(&receipt).await?;
    }

    // Restart - verify the timeout can be polled again
    {
        let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;

        let poll = TimedOutFlowPoll::new(now, 1, 1)?;
        let receipts = store.poll_timed_out(&poll).await?;
        assert_eq!(receipts.len(), 1);

        // Acknowledge the receipt
        let receipt = receipts.into_iter().next().unwrap();
        store.ack_timed_out(&receipt).await?;

        // Verify it's no longer pollable
        let after_ack = store.poll_timed_out(&poll).await?;
        assert!(after_ack.is_empty());
    }

    Ok(())
}

/// Tests concurrent updates to the same flow across multiple connections.
#[tokio::test]
async fn e2e_concurrent_flow_updates_idempotent() -> CatgaResult<()> {
    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("concurrent-updates.db");
    let url = format!("sqlite://{}", database.display());

    // Create initial flow
    {
        let store = SqlFlowStore::connect_sqlite(&url).await?;
        store.migrate().await?;
        let state = FlowState::new("concurrent-flow", "payment", Vec::new(), "node-a");
        store.create(state).await?;
    }

    // Simulate concurrent updates from two "workers"
    let (result1, result2) = tokio::join!(
        async {
            let store = SqlFlowStore::connect_sqlite(&url).await?;
            let current = store.get("concurrent-flow").await?.unwrap();
            let version = current.version();
            let next = current.next_version()?;
            store.update(version, next).await
        },
        async {
            let store = SqlFlowStore::connect_sqlite(&url).await?;
            let current = store.get("concurrent-flow").await?.unwrap();
            let version = current.version();
            let next = current.next_version()?;
            store.update(version, next).await
        }
    );

    // Exactly one should succeed
    let success_count = usize::from(result1?) + usize::from(result2?);
    assert_eq!(success_count, 1, "exactly one concurrent update must succeed");

    // Verify final state
    {
        let store = SqlFlowStore::connect_sqlite(&url).await?;
        let final_state = store.get("concurrent-flow").await?.unwrap();
        assert_eq!(final_state.version(), 1);
    }

    Ok(())
}

/// Tests that we can connect with application-owned pool.
#[tokio::test]
async fn e2e_application_owned_pool_preserves_data() -> CatgaResult<()> {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    let directory =
        tempfile::tempdir().map_err(|e| database_error("create temp directory", e))?;
    let database = directory.path().join("app-pool-persistence.db");
    let url = format!("sqlite://{}", database.display());

    // First session with custom pool settings
    {
        let options = SqliteConnectOptions::from_str(&url)
            .map_err(|e| database_error("parse SQLite options", e))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|e| database_error("connect application pool", e))?;

        let store = SqlFlowStore::from_sqlite_pool(pool);
        store.migrate().await?;

        let state = FlowState::new("app-pool-flow", "payment", Vec::new(), "node-a");
        store.create(state).await?;
    }

    // Restart with different pool settings
    {
        let options = SqliteConnectOptions::from_str(&url)
            .map_err(|e| database_error("parse SQLite options", e))?
            .read_only(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .map_err(|e| database_error("connect application pool", e))?;

        let store = SqlFlowStore::from_sqlite_pool(pool);
        store.migrate().await?;

        let recovered = store.get("app-pool-flow").await?;
        assert!(recovered.is_some());
    }

    Ok(())
}
