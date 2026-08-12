//! PostgreSQL real-service edge-case coverage driven by the shared [`edge_support`] contracts.
//!
//! Every test provisions an isolated database so the raw column rewrites that prove
//! corruption fencing cannot disturb the other contracts sharing the service.
#![cfg(feature = "postgres")]

use std::time::Duration;

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::flow::{
    DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowScheduler, FlowState, FlowStore,
    StateMachineStore, SuspendedFlowStore, TimedOutFlowStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlFlowStoreOptions,
    SqlStateMachineStore, SqlSuspendedFlowStore,
};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, query, query_scalar};

mod edge_support;
#[path = "sql_contracts.rs"]
mod sql_contracts;

use edge_support::{BigState, EdgeDialect, EdgeState};

struct PostgresEdges {
    pool: PgPool,
}

impl PostgresEdges {
    async fn connect(url: &str) -> CatgaResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        Ok(Self { pool })
    }

    async fn execute(&self, sql: &'static str, binds: &[EdgeBind<'_>]) -> CatgaResult<()> {
        let mut statement = query(sql);
        for bind in binds {
            statement = match bind {
                EdgeBind::Text(value) => statement.bind(*value),
                EdgeBind::Bytes(value) => statement.bind(*value),
                EdgeBind::Integer(value) => statement.bind(*value),
            };
        }
        statement
            .execute(&self.pool)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        Ok(())
    }

    async fn payload(&self, sql: &'static str, flow_id: &str) -> CatgaResult<Vec<u8>> {
        query_scalar::<_, Vec<u8>>(sql)
            .bind(flow_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
    }
}

enum EdgeBind<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
    Integer(i64),
}

#[async_trait]
impl EdgeDialect for PostgresEdges {
    async fn set_flow_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET flow_id = $1 WHERE flow_id = $2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn flow_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM catga_flow_states WHERE flow_id = $1",
            flow_id,
        )
        .await
    }

    async fn set_flow_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET payload = $1 WHERE flow_id = $2",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_flow_heartbeat_ms(&self, flow_id: &str, heartbeat_ms: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET heartbeat_ms = $1 WHERE flow_id = $2",
            &[EdgeBind::Integer(heartbeat_ms), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_continuation_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET flow_id = $1 WHERE flow_id = $2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn continuation_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM catga_flow_continuations WHERE flow_id = $1",
            flow_id,
        )
        .await
    }

    async fn set_continuation_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET payload = $1 WHERE flow_id = $2",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_continuation_wait_correlation(
        &self,
        flow_id: &str,
        correlation: &str,
        correlation_key: &[u8; 32],
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET wait_correlation = $1, wait_correlation_key = $2 \
             WHERE flow_id = $3",
            &[
                EdgeBind::Text(correlation),
                EdgeBind::Bytes(correlation_key.as_slice()),
                EdgeBind::Text(flow_id),
            ],
        )
        .await
    }

    async fn set_continuation_status(&self, flow_id: &str, status: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET status = $1 WHERE flow_id = $2",
            &[EdgeBind::Integer(status), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_continuation_updated_subsec_ns(
        &self,
        flow_id: &str,
        subsec_ns: i64,
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET updated_at_subsec_ns = $1 WHERE flow_id = $2",
            &[EdgeBind::Integer(subsec_ns), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_progress_identity(
        &self,
        flow_id: &str,
        step_index: u32,
        replacement: &str,
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_dsl_step_progress SET flow_id = $1 WHERE flow_id = $2 AND step_index = $3",
            &[
                EdgeBind::Text(replacement),
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn progress_payload(&self, flow_id: &str, step_index: u32) -> CatgaResult<Vec<u8>> {
        query_scalar::<_, Vec<u8>>(
            "SELECT payload FROM catga_dsl_step_progress WHERE flow_id = $1 AND step_index = $2",
        )
        .bind(flow_id)
        .bind(i64::from(step_index))
        .fetch_one(&self.pool)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
    }

    async fn set_progress_payload(
        &self,
        flow_id: &str,
        step_index: u32,
        payload: &[u8],
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_dsl_step_progress SET payload = $1 WHERE flow_id = $2 AND step_index = $3",
            &[
                EdgeBind::Bytes(payload),
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn set_snapshot_identity(&self, instance_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET instance_id = $1 WHERE instance_id = $2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_version(&self, instance_id: &str, version: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET version = $1 WHERE instance_id = $2",
            &[EdgeBind::Integer(version), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_payload(&self, instance_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET payload = $1 WHERE instance_id = $2",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_schedule_identity(&self, schedule_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_schedules SET flow_id = $1 WHERE schedule_id = $2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(schedule_id)],
        )
        .await
    }

    async fn bump_flow_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET revision = revision + 1 WHERE flow_id = $1",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_continuation_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET revision = revision + 1 WHERE flow_id = $1",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_progress_revision(&self, flow_id: &str, step_index: u32) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_dsl_step_progress SET revision = revision + 1 \
             WHERE flow_id = $1 AND step_index = $2",
            &[
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn bump_snapshot_revision(&self, instance_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET revision = revision + 1 WHERE instance_id = $1",
            &[EdgeBind::Text(instance_id)],
        )
        .await
    }
}

async fn create_postgres_database(base_url: &str) -> CatgaResult<(PgPool, String, String)> {
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .connect(base_url)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let database = format!("catga_edge_{}", uuid::Uuid::new_v4().simple());
    query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{database}\""
    )))
    .execute(&admin)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = url::Url::parse(base_url)
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(&database);
    Ok((admin, url.into(), database))
}

async fn drop_postgres_database(admin: &PgPool, database: &str) -> CatgaResult<()> {
    query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE \"{database}\" WITH (FORCE)"
    )))
    .execute(admin)
    .await
    .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_flow_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = PostgresEdges::connect(url.as_str()).await?;
        edge_support::flow_store_edges(&store, &edges, "postgres-flow").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_suspended_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = PostgresEdges::connect(url.as_str()).await?;
        edge_support::suspended_store_edges(&store, &edges, "postgres-suspended").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_dsl_progress_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = PostgresEdges::connect(url.as_str()).await?;
        edge_support::dsl_progress_edges(&store, &edges, "postgres-progress").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_state_machine_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = PostgresEdges::connect(url.as_str()).await?;
        edge_support::state_machine_edges(&store, &edges, "postgres-snapshot").await?;
        let big = SqlStateMachineStore::<BigState>::connect_postgres(url.as_str()).await?;
        edge_support::state_machine_oversize_encode(&big, "postgres-snapshot").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_scheduler_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let scheduler = SqlFlowScheduler::connect_postgres(url.as_str()).await?;
        scheduler.migrate().await?;
        let edges = PostgresEdges::connect(url.as_str()).await?;
        edge_support::scheduler_edges(&scheduler, &edges, "postgres-scheduler").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_timeout_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        edge_support::timeout_edges(&store).await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// Proves the continuation migration upgrades a pre-keyed legacy table in place.
///
/// The legacy layout predates the indexed `flow_type_key` column, so the migration must add the
/// column, backfill every existing row, and only then enforce `NOT NULL`.
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_continuation_migration_backfills_legacy_type_keys() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let legacy = PgPoolOptions::new()
            .max_connections(4)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        query(
            "CREATE TABLE catga_flow_continuations (\
             flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
             flow_type TEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, \
             created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0, \
             deadline_ms BIGINT NULL, revision BIGINT NOT NULL, \
             due_token BYTEA NULL, lease_until_ms BIGINT NULL, payload BYTEA NOT NULL)",
        )
        .execute(&legacy)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        query(
            "INSERT INTO catga_flow_continuations \
             (flow_key, flow_id, flow_type, status, version, created_at_ms, revision, payload) \
             VALUES ($1, $2, $3, 2, 0, 0, 0, $4)",
        )
        .bind(vec![7_u8; 32])
        .bind("legacy-continuation")
        .bind("legacy-type")
        .bind(Vec::<u8>::new())
        .execute(&legacy)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;

        let store = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let filled = query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM catga_flow_continuations WHERE flow_type_key IS NULL",
        )
        .fetch_one(&legacy)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        assert_eq!(
            filled, 0,
            "the migration must backfill every legacy type key"
        );

        let continuation = FlowContinuation::new(
            FlowState::new("upgraded-continuation", "upgraded-type", [], "node-a"),
            "run",
        );
        assert!(store.create(continuation.clone()).await?);
        assert_eq!(
            store.get("upgraded-continuation").await?,
            Some(continuation),
            "the upgraded schema must serve new continuations"
        );
        Ok(())
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_flow_heartbeat_exhausts_its_bounded_retries_under_contention() -> CatgaResult<()>
{
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(PostgresEdges::connect(url.as_str()).await?);
        edge_support::flow_heartbeat_cas_exhaustion(&store, &edges, "postgres-flow").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_suspended_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(PostgresEdges::connect(url.as_str()).await?);
        edge_support::suspended_cas_exhaustion(&store, &edges, "postgres-suspended").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_dsl_progress_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(PostgresEdges::connect(url.as_str()).await?);
        edge_support::dsl_progress_cas_exhaustion(&store, &edges, "postgres-progress").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_state_machine_updates_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_postgres(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(PostgresEdges::connect(url.as_str()).await?);
        edge_support::state_machine_cas_exhaustion(&store, &edges, "postgres-snapshot").await
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// Every public adapter must surface a database error rather than a panic when its
/// migration has not run yet.
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_public_adapters_surface_schema_errors_before_migration() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let flow = SqlFlowStore::connect_postgres(url.as_str()).await?;
        let suspended = SqlSuspendedFlowStore::connect_postgres(url.as_str()).await?;
        let progress = SqlDslStepProgressStore::connect_postgres(url.as_str()).await?;
        let snapshots = SqlStateMachineStore::<EdgeState>::connect_postgres(url.as_str()).await?;
        let scheduler = SqlFlowScheduler::connect_postgres(url.as_str()).await?;
        let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(2_000);

        let state = catga_core::flow::FlowState::new("not-migrated", "schema-errors", [], "node-a");
        assert!(flow.create(state.clone()).await.is_err());
        assert!(flow.create_batch(vec![state.clone()]).await.is_err());
        assert!(flow.get("not-migrated").await.is_err());
        assert!(flow.update(0, state.next_version()?).await.is_err());
        assert!(
            flow.try_claim("schema-errors", "node-b", Duration::from_secs(1))
                .await
                .is_err()
        );
        assert!(flow.heartbeat("not-migrated", "node-a", 0).await.is_err());

        let continuation = catga_core::flow::FlowContinuation::new(
            catga_core::flow::FlowState::new("not-migrated", "schema-errors", [], "node-a"),
            "run",
        );
        assert!(suspended.create(continuation.clone()).await.is_err());
        assert!(suspended.get("not-migrated").await.is_err());
        assert!(
            suspended
                .query(&catga_core::flow::FlowQuery::new(1, 1)?)
                .await
                .is_err()
        );
        assert!(
            suspended
                .update(
                    0,
                    continuation.with_state(
                        catga_core::flow::FlowState::new(
                            "not-migrated",
                            "schema-errors",
                            [],
                            "node-a"
                        )
                        .next_version()?,
                    )
                )
                .await
                .is_err()
        );
        assert!(suspended.delete("not-migrated", 0).await.is_err());
        assert!(
            suspended
                .heartbeat("not-migrated", "node-a", 0)
                .await
                .is_err()
        );
        assert!(
            suspended
                .poll_timed_out(&catga_core::flow::TimedOutFlowPoll::new(now, 1, 1)?)
                .await
                .is_err()
        );

        let step = catga_core::flow::DslStepProgress::new("not-migrated", 0, b"payload".as_slice());
        assert!(progress.create(step.clone()).await.is_err());
        assert!(progress.get("not-migrated", 0).await.is_err());
        assert!(
            progress
                .update(0, step.next_version(b"next".as_slice())?)
                .await
                .is_err()
        );
        assert!(progress.delete("not-migrated", 0).await.is_err());

        let snapshot = catga_core::flow::StateMachineSnapshot::new(
            "not-migrated",
            EdgeState {
                paid: false,
                quantity: 0,
            },
        );
        assert!(snapshots.create(snapshot.clone()).await.is_err());
        assert!(snapshots.get("not-migrated").await.is_err());
        assert!(
            snapshots
                .update(
                    0,
                    snapshot.next_version(EdgeState {
                        paid: true,
                        quantity: 0,
                    })?
                )
                .await
                .is_err()
        );

        assert!(
            scheduler
                .schedule_resume("not-migrated", "resume", now)
                .await
                .is_err()
        );
        assert!(
            scheduler
                .claim_due("worker", now, Duration::from_secs(1), 1)
                .await
                .is_err()
        );
        assert!(scheduler.cancel_resume("not-migrated").await.is_err());
        assert!(scheduler.ack_due("worker", "not-migrated").await.is_err());
        assert!(
            scheduler
                .release_due("worker", "not-migrated")
                .await
                .is_err()
        );
        assert!(
            scheduler
                .renew_due("worker", "not-migrated", now, Duration::from_secs(1))
                .await
                .is_err()
        );
        Ok(())
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// A pre-existing relation with the scheduler table's name must surface the DDL failure
/// instead of silently migrating the wrong shape.
#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_scheduler_migration_surfaces_relation_conflicts() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_postgres_database(base.as_ref()).await?;
    let result = async {
        let decoy = PgPoolOptions::new()
            .max_connections(4)
            .connect(url.as_str())
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
        query("CREATE VIEW catga_flow_schedules AS SELECT 1 AS decoy")
            .execute(&decoy)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
        let scheduler = SqlFlowScheduler::connect_postgres(url.as_str()).await?;
        assert!(
            scheduler.migrate().await.is_err(),
            "migrating over an incompatible relation must fail"
        );
        Ok(())
    }
    .await;
    let cleanup = drop_postgres_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_POSTGRES_URL"]
async fn postgres_constructor_options_and_application_owned_pools() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let options = SqlFlowStoreOptions::new()
        .max_connections(4)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .max_lifetime(Duration::from_secs(60))
        .idle_timeout(Duration::from_secs(30));
    let configured = SqlFlowStore::connect_postgres_with_options(base.as_ref(), options).await?;
    configured.migrate().await?;

    let flow_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlFlowStore::from_postgres_pool(flow_pool)
        .migrate()
        .await?;
    let suspended_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlSuspendedFlowStore::from_postgres_pool(suspended_pool)
        .migrate()
        .await?;
    let scheduler_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlFlowScheduler::from_postgres_pool(scheduler_pool)
        .migrate()
        .await?;
    let progress_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlDslStepProgressStore::from_postgres_pool(progress_pool)
        .migrate()
        .await?;
    let snapshot_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlStateMachineStore::<EdgeState>::from_postgres_pool(
        snapshot_pool,
        MemoryPackSnapshotCodec::default(),
    )
    .migrate()
    .await?;
    Ok(())
}
