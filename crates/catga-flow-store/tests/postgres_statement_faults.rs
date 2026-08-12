//! PostgreSQL statement-failure and migration-conflict coverage for every write path.
//!
//! Each store maps database failures on its statements to retryable availability errors through
//! dedicated boundaries. These tests install `BEFORE ... RAISE EXCEPTION` triggers so every
//! insert, update, and delete statement fails deterministically, and they pre-create conflicting
//! schema objects plus a legacy continuation table so the migration boundaries are exercised too.
#![cfg(feature = "postgres")]

use std::time::{Duration, SystemTime};

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize, MemoryPackWriter,
};
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowScheduler,
    FlowState, FlowStore, StateMachineSnapshot, StateMachineStore, SuspendedFlowStore,
    TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlStateMachineStore,
    SqlSuspendedFlowStore,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use url::Url;

#[path = "sql_contracts.rs"]
mod sql_contracts;

/// A snapshot state kept dependency-free for the statement-fault checks.
#[derive(Clone, Debug, Eq, catga_core::MemoryPackable, PartialEq)]
struct PgFaultState {
    paid: bool,
    quantity: u32,
}

/// Creates an isolated database under the service URL and returns the admin pool plus URL.
async fn harness() -> CatgaResult<Option<(PgPool, String, String)>> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(None);
    };
    let database = format!("catga_stmt_faults_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let statement = format!("CREATE DATABASE {database}");
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(&admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = Url::parse(base.as_ref())
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(format!("/{database}").as_str());
    Ok(Some((admin, String::from(url.as_str()), database)))
}

/// Drops the isolated database best-effort once a test completes.
async fn cleanup(admin: &PgPool, database: &str) {
    let statement = format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)");
    let _ = sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(admin)
        .await;
}

/// Installs a trigger that raises an exception for every `event` (`INSERT`, `UPDATE`, or
/// `DELETE`) on `table`, emulating a database-level statement failure.
async fn abort_statement(pool: &PgPool, table: &str, event: &str) -> CatgaResult<()> {
    let function = format!("catga_test_abort_{table}");
    let statement = format!(
        "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger AS $$ \
         BEGIN RAISE EXCEPTION 'injected statement failure'; END; $$ LANGUAGE plpgsql"
    );
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let statement = format!(
        "CREATE TRIGGER {function}_{event} BEFORE {event} ON {table} \
         FOR EACH ROW EXECUTE FUNCTION {function}()"
    );
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    Ok(())
}

/// Runs one raw DDL statement on the administrative pool.
async fn admin_ddl(pool: &PgPool, sql: &str) -> CatgaResult<()> {
    sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

/// Asserts one operation failed with the store's retryable availability boundary.
fn expect_unavailable<T>(result: CatgaResult<T>, operation: &str) {
    match result {
        Ok(_) => panic!("{operation} unexpectedly succeeded over an aborted statement"),
        Err(error) => assert_eq!(
            error.code(),
            ErrorCode::Unavailable,
            "{operation} must surface the aborted statement as an availability failure"
        ),
    }
}

/// Flow-state inserts and updates must surface aborted statements as unavailable.
#[tokio::test]
async fn postgres_flow_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let Some((admin, url, database)) = harness().await? else {
        return Ok(());
    };
    let result = async {
        let store = SqlFlowStore::connect_postgres(&url).await?;
        store.migrate().await?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;

        abort_statement(&pool, "catga_flow_states", "INSERT").await?;
        let state = FlowState::new("fault-flow", "payment", [], "node-a");
        expect_unavailable(store.create(state.clone()).await, "flow create");
        expect_unavailable(
            store.create_batch(vec![state.clone()]).await,
            "flow batch create",
        );
        let drop_trigger =
            "DROP TRIGGER catga_test_abort_catga_flow_states_INSERT ON catga_flow_states";
        admin_ddl(&pool, drop_trigger).await?;
        assert!(store.create(state.clone()).await?);

        abort_statement(&pool, "catga_flow_states", "UPDATE").await?;
        expect_unavailable(
            store.update(0, state.clone().next_version()?).await,
            "flow update",
        );
        expect_unavailable(
            store.heartbeat("fault-flow", "node-a", 0).await,
            "flow heartbeat",
        );
        expect_unavailable(
            store.try_claim("payment", "node-b", Duration::ZERO).await,
            "flow stale claim",
        );
        Ok(())
    }
    .await;
    cleanup(&admin, &database).await;
    result
}

/// Continuation inserts, updates, and deletes must surface aborted statements as unavailable.
#[tokio::test]
async fn postgres_suspended_store_surfaces_aborted_statements() -> CatgaResult<()> {
    let Some((admin, url, database)) = harness().await? else {
        return Ok(());
    };
    let result = async {
        let store = SqlSuspendedFlowStore::connect_postgres(&url).await?;
        store.migrate().await?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5_000);

        abort_statement(&pool, "catga_flow_continuations", "INSERT").await?;
        let continuation = FlowContinuation::new(
            FlowState::new("fault-flow", "payment", [], "node-a"),
            "resume",
        );
        expect_unavailable(store.create(continuation).await, "continuation create");
        let drop_trigger = "DROP TRIGGER catga_test_abort_catga_flow_continuations_INSERT \
                            ON catga_flow_continuations";
        admin_ddl(&pool, drop_trigger).await?;

        let waiting = FlowContinuation::waiting(
            FlowState::new("fault-flow", "payment", [], "node-a").suspended(),
            "resume",
            WaitCondition::new(
                "fault-correlator",
                WaitPolicy::All,
                1,
                now - Duration::from_secs(5),
                Duration::from_secs(1),
            ),
        );
        assert!(store.create(waiting.clone()).await?);
        abort_statement(&pool, "catga_flow_continuations", "UPDATE").await?;
        let next_state = waiting.state().clone().next_version()?;
        let next = waiting.clone().with_state(next_state);
        expect_unavailable(store.update(0, next.clone()).await, "continuation update");
        expect_unavailable(store.claim(&waiting, next).await, "continuation claim");
        expect_unavailable(
            store
                .record_wait_success("fault-flow", 0, "child-a", Vec::new())
                .await,
            "continuation wait success",
        );
        expect_unavailable(
            store
                .record_wait_failure(
                    "fault-flow",
                    0,
                    "child-a",
                    CatgaError::new(ErrorCode::Internal, "forced child failure"),
                )
                .await,
            "continuation wait failure",
        );
        let poll = TimedOutFlowPoll::new(now, 1, 1)?;
        expect_unavailable(
            store.poll_timed_out(&poll).await,
            "continuation timeout poll",
        );
        let malformed = TimedOutFlowReceipt::new("fault-flow", vec![7u8; 3]);
        match store.ack_timed_out(&malformed).await {
            Ok(()) => panic!("a malformed receipt token must not settle"),
            Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
        }

        abort_statement(&pool, "catga_flow_continuations", "DELETE").await?;
        expect_unavailable(store.delete("fault-flow", 0).await, "continuation delete");
        Ok(())
    }
    .await;
    cleanup(&admin, &database).await;
    result
}

/// Schedule inserts, lease updates, and acknowledgements must surface aborted statements.
#[tokio::test]
async fn postgres_scheduler_surfaces_aborted_statements() -> CatgaResult<()> {
    let Some((admin, url, database)) = harness().await? else {
        return Ok(());
    };
    let result = async {
        let scheduler = SqlFlowScheduler::connect_postgres(&url).await?;
        scheduler.migrate().await?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(120);

        abort_statement(&pool, "catga_flow_schedules", "INSERT").await?;
        expect_unavailable(
            scheduler
                .schedule_resume("fault-flow", "resume", due_at)
                .await,
            "schedule resume insert",
        );
        let drop_trigger =
            "DROP TRIGGER catga_test_abort_catga_flow_schedules_INSERT ON catga_flow_schedules";
        admin_ddl(&pool, drop_trigger).await?;
        let schedule_a = scheduler
            .schedule_resume("fault-flow", "resume-a", due_at)
            .await?;
        let schedule_b = scheduler
            .schedule_resume("fault-flow", "resume-b", due_at)
            .await?;
        let claimed = scheduler
            .claim_due("node-a", now, Duration::from_secs(30), 4)
            .await?;
        assert_eq!(claimed.len(), 2, "both due schedules must be leased");

        abort_statement(&pool, "catga_flow_schedules", "UPDATE").await?;
        expect_unavailable(
            scheduler.release_due("node-a", &schedule_a).await,
            "schedule release",
        );
        expect_unavailable(
            scheduler
                .renew_due("node-a", &schedule_b, now, Duration::from_secs(30))
                .await,
            "schedule renew",
        );
        let drop_trigger =
            "DROP TRIGGER catga_test_abort_catga_flow_schedules_UPDATE ON catga_flow_schedules";
        admin_ddl(&pool, drop_trigger).await?;
        let schedule_c = scheduler
            .schedule_resume("fault-flow", "resume-c", due_at)
            .await?;
        let claimed = scheduler
            .claim_due("node-a", now, Duration::from_secs(30), 4)
            .await?;
        assert_eq!(claimed.len(), 1, "only the fresh schedule must be leased");

        abort_statement(&pool, "catga_flow_schedules", "DELETE").await?;
        let claimed_id = claimed[0].schedule_id().to_string();
        let unclaimed_id = if claimed_id == schedule_c.as_ref() {
            schedule_a.clone()
        } else {
            schedule_c.clone()
        };
        expect_unavailable(
            scheduler.ack_due("node-a", &claimed_id).await,
            "schedule acknowledgement",
        );
        expect_unavailable(
            scheduler.cancel_resume(&unclaimed_id).await,
            "schedule cancel",
        );
        Ok(())
    }
    .await;
    cleanup(&admin, &database).await;
    result
}

/// DSL progress and snapshot writes must surface aborted statements as unavailable.
#[tokio::test]
async fn postgres_progress_and_snapshots_surface_aborted_statements() -> CatgaResult<()> {
    let Some((admin, url, database)) = harness().await? else {
        return Ok(());
    };
    let result = async {
        let store = SqlDslStepProgressStore::connect_postgres(&url).await?;
        store.migrate().await?;
        let machine = SqlStateMachineStore::<PgFaultState>::connect_postgres(&url).await?;
        machine.migrate().await?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;

        abort_statement(&pool, "catga_dsl_step_progress", "INSERT").await?;
        let progress = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
        expect_unavailable(store.create(progress).await, "progress create");
        let drop_trigger =
            "DROP TRIGGER catga_test_abort_catga_dsl_step_progress_INSERT ON catga_dsl_step_progress";
        admin_ddl(&pool, drop_trigger).await?;
        let progress = DslStepProgress::new("fault-flow", 0, b"payload".as_slice());
        assert!(store.create(progress.clone()).await?);
        abort_statement(&pool, "catga_dsl_step_progress", "UPDATE").await?;
        expect_unavailable(
            store
                .update(0, progress.next_version(b"next".as_slice())?)
                .await,
            "progress update",
        );
        let drop_trigger =
            "DROP TRIGGER catga_test_abort_catga_dsl_step_progress_UPDATE ON catga_dsl_step_progress";
        admin_ddl(&pool, drop_trigger).await?;
        abort_statement(&pool, "catga_dsl_step_progress", "DELETE").await?;
        expect_unavailable(store.delete("fault-flow", 0).await, "progress delete");

        abort_statement(&pool, "catga_state_machine_snapshots", "INSERT").await?;
        let snapshot = StateMachineSnapshot::new(
            "fault-machine",
            PgFaultState {
                paid: false,
                quantity: 0,
            },
        );
        expect_unavailable(machine.create(snapshot).await, "snapshot create");
        let drop_trigger = "DROP TRIGGER catga_test_abort_catga_state_machine_snapshots_INSERT \
                            ON catga_state_machine_snapshots";
        admin_ddl(&pool, drop_trigger).await?;
        let snapshot = StateMachineSnapshot::new(
            "fault-machine",
            PgFaultState {
                paid: false,
                quantity: 0,
            },
        );
        assert!(machine.create(snapshot.clone()).await?);
        abort_statement(&pool, "catga_state_machine_snapshots", "UPDATE").await?;
        expect_unavailable(
            machine
                .update(
                    0,
                    snapshot.next_version(PgFaultState {
                        paid: true,
                        quantity: 1,
                    })?,
                )
                .await,
            "snapshot update",
        );
        Ok(())
    }
    .await;
    cleanup(&admin, &database).await;
    result
}

/// Continuation migration conflicts and legacy upgrades must stay deterministic.
#[tokio::test]
async fn postgres_continuation_migration_surfaces_conflicts_and_upgrades() -> CatgaResult<()> {
    let Some((admin, url, database)) = harness().await? else {
        return Ok(());
    };
    let result = async {
        // A view squatting the table name must abort the schema upgrade statements.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        admin_ddl(
            &pool,
            "CREATE VIEW catga_flow_continuations AS SELECT 1 AS collision",
        )
        .await?;
        let store = SqlSuspendedFlowStore::connect_postgres(&url).await?;
        expect_unavailable(store.migrate().await, "continuation table conflict");
        admin_ddl(&pool, "DROP VIEW catga_flow_continuations").await?;

        // A legacy table without the newer columns must be upgraded and backfilled in place.
        admin_ddl(
            &pool,
            "CREATE TABLE catga_flow_continuations (\
                 flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
                 flow_type TEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, \
                 created_at_ms BIGINT NOT NULL, deadline_ms BIGINT NULL, \
                 revision BIGINT NOT NULL, due_token BYTEA NULL, lease_until_ms BIGINT NULL, \
                 payload BYTEA NOT NULL)",
        )
        .await?;
        sqlx::query(
            "INSERT INTO catga_flow_continuations \
             (flow_key, flow_id, flow_type, status, version, created_at_ms, revision, payload) \
             VALUES ($1, 'legacy-flow', 'payment', 1, 0, 5000, 0, $2)",
        )
        .bind([1u8].as_slice())
        .bind([0u8].as_slice())
        .execute(&pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        store.migrate().await?;
        let backfilled: i64 = sqlx::query_scalar(
            "SELECT updated_at_ms FROM catga_flow_continuations WHERE flow_id = 'legacy-flow'",
        )
        .fetch_one(&pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        assert_eq!(
            backfilled, 5000,
            "legacy rows must carry their creation time as the initial update time"
        );
        let keyed: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM catga_flow_continuations WHERE flow_type_key IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        assert_eq!(keyed, 1, "legacy rows must receive a backfilled type key");

        // A view squatting the recreated query index name must abort the index rebuild.
        admin_ddl(&pool, "DROP INDEX catga_flow_continuations_query_idx").await?;
        admin_ddl(
            &pool,
            "CREATE VIEW catga_flow_continuations_query_idx AS SELECT 1 AS collision",
        )
        .await?;
        expect_unavailable(store.migrate().await, "continuation index conflict");
        Ok(())
    }
    .await;
    cleanup(&admin, &database).await;
    result
}
