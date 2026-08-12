//! MySQL real-service edge-case coverage driven by the shared [`edge_support`] contracts.
//!
//! Every test provisions an isolated database so the raw column rewrites that prove
//! corruption fencing cannot disturb the other contracts sharing the service.
#![cfg(feature = "mysql")]

use std::time::Duration;

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::flow::{
    DslStepProgressStore, DueFlowScheduler, FlowScheduler, FlowStore, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlFlowStoreOptions,
    SqlStateMachineStore, SqlSuspendedFlowStore,
};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, query, query_scalar};

mod edge_support;
#[path = "sql_contracts.rs"]
mod sql_contracts;

use edge_support::{BigState, EdgeDialect, EdgeState};

struct MysqlEdges {
    pool: MySqlPool,
}

impl MysqlEdges {
    async fn connect(url: &str) -> CatgaResult<Self> {
        let pool = MySqlPoolOptions::new()
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
impl EdgeDialect for MysqlEdges {
    async fn set_flow_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET flow_id = ? WHERE flow_id = ?",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn flow_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM catga_flow_states WHERE flow_id = ?",
            flow_id,
        )
        .await
    }

    async fn set_flow_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET payload = ? WHERE flow_id = ?",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_flow_heartbeat_ms(&self, flow_id: &str, heartbeat_ms: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET heartbeat_ms = ? WHERE flow_id = ?",
            &[EdgeBind::Integer(heartbeat_ms), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_continuation_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET flow_id = ? WHERE flow_id = ?",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn continuation_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM catga_flow_continuations WHERE flow_id = ?",
            flow_id,
        )
        .await
    }

    async fn set_continuation_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET payload = ? WHERE flow_id = ?",
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
            "UPDATE catga_flow_continuations SET wait_correlation = ?, wait_correlation_key = ? \
             WHERE flow_id = ?",
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
            "UPDATE catga_flow_continuations SET status = ? WHERE flow_id = ?",
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
            "UPDATE catga_flow_continuations SET updated_at_subsec_ns = ? WHERE flow_id = ?",
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
            "UPDATE catga_dsl_step_progress SET flow_id = ? WHERE flow_id = ? AND step_index = ?",
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
            "SELECT payload FROM catga_dsl_step_progress WHERE flow_id = ? AND step_index = ?",
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
            "UPDATE catga_dsl_step_progress SET payload = ? WHERE flow_id = ? AND step_index = ?",
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
            "UPDATE catga_state_machine_snapshots SET instance_id = ? WHERE instance_id = ?",
            &[EdgeBind::Text(replacement), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_version(&self, instance_id: &str, version: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET version = ? WHERE instance_id = ?",
            &[EdgeBind::Integer(version), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_payload(&self, instance_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET payload = ? WHERE instance_id = ?",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_schedule_identity(&self, schedule_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_schedules SET flow_id = ? WHERE schedule_id = ?",
            &[EdgeBind::Text(replacement), EdgeBind::Text(schedule_id)],
        )
        .await
    }

    async fn bump_flow_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_states SET revision = revision + 1 WHERE flow_id = ?",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_continuation_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET revision = revision + 1 WHERE flow_id = ?",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_progress_revision(&self, flow_id: &str, step_index: u32) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_dsl_step_progress SET revision = revision + 1 \
             WHERE flow_id = ? AND step_index = ?",
            &[
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn bump_snapshot_revision(&self, instance_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_state_machine_snapshots SET revision = revision + 1 WHERE instance_id = ?",
            &[EdgeBind::Text(instance_id)],
        )
        .await
    }
}

async fn create_mysql_database(base_url: &str) -> CatgaResult<(MySqlPool, String, String)> {
    let admin = MySqlPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(5))
        .connect(base_url)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let database = format!("catga_edge_{}", uuid::Uuid::new_v4().simple());
    query(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{database}`")))
        .execute(&admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    let mut url = url::Url::parse(base_url)
        .map_err(|error| CatgaError::new(ErrorCode::Validation, error.to_string()))?;
    url.set_path(&database);
    Ok((admin, url.into(), database))
}

async fn drop_mysql_database(admin: &MySqlPool, database: &str) -> CatgaResult<()> {
    query(sqlx::AssertSqlSafe(format!("DROP DATABASE `{database}`")))
        .execute(admin)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_flow_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MysqlEdges::connect(url.as_str()).await?;
        edge_support::flow_store_edges(&store, &edges, "mysql-flow").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_suspended_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MysqlEdges::connect(url.as_str()).await?;
        edge_support::suspended_store_edges(&store, &edges, "mysql-suspended").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_dsl_progress_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MysqlEdges::connect(url.as_str()).await?;
        edge_support::dsl_progress_edges(&store, &edges, "mysql-progress").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_state_machine_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MysqlEdges::connect(url.as_str()).await?;
        edge_support::state_machine_edges(&store, &edges, "mysql-snapshot").await?;
        let big = SqlStateMachineStore::<BigState>::connect_mysql(url.as_str()).await?;
        edge_support::state_machine_oversize_encode(&big, "mysql-snapshot").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_scheduler_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let scheduler = SqlFlowScheduler::connect_mysql(url.as_str()).await?;
        scheduler.migrate().await?;
        let edges = MysqlEdges::connect(url.as_str()).await?;
        edge_support::scheduler_edges(&scheduler, &edges, "mysql-scheduler").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_timeout_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        edge_support::timeout_edges(&store).await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_flow_heartbeat_exhausts_its_bounded_retries_under_contention() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MysqlEdges::connect(url.as_str()).await?);
        edge_support::flow_heartbeat_cas_exhaustion(&store, &edges, "mysql-flow").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_suspended_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MysqlEdges::connect(url.as_str()).await?);
        edge_support::suspended_cas_exhaustion(&store, &edges, "mysql-suspended").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_dsl_progress_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MysqlEdges::connect(url.as_str()).await?);
        edge_support::dsl_progress_cas_exhaustion(&store, &edges, "mysql-progress").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_state_machine_updates_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_mysql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MysqlEdges::connect(url.as_str()).await?);
        edge_support::state_machine_cas_exhaustion(&store, &edges, "mysql-snapshot").await
    }
    .await;
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// Every public adapter must surface a database error rather than a panic when its
/// migration has not run yet.
#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_public_adapters_surface_schema_errors_before_migration() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mysql_database(base.as_ref()).await?;
    let result = async {
        let flow = SqlFlowStore::connect_mysql(url.as_str()).await?;
        let suspended = SqlSuspendedFlowStore::connect_mysql(url.as_str()).await?;
        let progress = SqlDslStepProgressStore::connect_mysql(url.as_str()).await?;
        let snapshots = SqlStateMachineStore::<EdgeState>::connect_mysql(url.as_str()).await?;
        let scheduler = SqlFlowScheduler::connect_mysql(url.as_str()).await?;
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
    let cleanup = drop_mysql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MYSQL_URL"]
async fn mysql_constructor_options_and_application_owned_pools() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let options = SqlFlowStoreOptions::new()
        .max_connections(4)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .max_lifetime(Duration::from_secs(60))
        .idle_timeout(Duration::from_secs(30));
    let configured = SqlFlowStore::connect_mysql_with_options(base.as_ref(), options).await?;
    configured.migrate().await?;

    let flow_pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlFlowStore::from_mysql_pool(flow_pool).migrate().await?;
    let suspended_pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlSuspendedFlowStore::from_mysql_pool(suspended_pool)
        .migrate()
        .await?;
    let scheduler_pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlFlowScheduler::from_mysql_pool(scheduler_pool)
        .migrate()
        .await?;
    let progress_pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlDslStepProgressStore::from_mysql_pool(progress_pool)
        .migrate()
        .await?;
    let snapshot_pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(base.as_ref())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))?;
    SqlStateMachineStore::<EdgeState>::from_mysql_pool(
        snapshot_pool,
        MemoryPackSnapshotCodec::default(),
    )
    .migrate()
    .await?;
    Ok(())
}
