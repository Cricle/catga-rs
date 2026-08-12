//! SQLite edge-case coverage driven by the shared [`edge_support`] contracts.
#![cfg(feature = "sqlite")]

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlFlowStoreOptions,
    SqlStateMachineStore, SqlSuspendedFlowStore,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{SqlitePool, query, query_scalar};

mod edge_support;

use edge_support::{BigState, EdgeDialect, EdgeState};

/// Serializes the bounded-retry contention tests in this binary: each hammers one single-writer
/// SQLite database with revision bumpers, and running them concurrently multiplies lock waits
/// past the store's busy timeout on slow (instrumented) machines.
static SQLITE_CAS_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct SqliteEdges {
    pool: SqlitePool,
}

impl SqliteEdges {
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
impl EdgeDialect for SqliteEdges {
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
        _correlation_key: &[u8; 32],
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE catga_flow_continuations SET wait_correlation = ? WHERE flow_id = ?",
            &[EdgeBind::Text(correlation), EdgeBind::Text(flow_id)],
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

async fn harness(name: &str) -> CatgaResult<(tempfile::TempDir, Box<str>, SqliteEdges)> {
    let directory = tempfile::tempdir().map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "create SQLite edge-test directory")
            .with_details(error.to_string())
    })?;
    let url: Box<str> = format!("sqlite://{}", directory.path().join(name).display())
        .as_str()
        .into();
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(
            SqliteConnectOptions::from_str(&url)
                .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(5)),
        )
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    Ok((directory, url, SqliteEdges { pool }))
}

#[tokio::test]
async fn sqlite_flow_store_edges() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("flow-edges.db").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    edge_support::flow_store_edges(&store, &edges, "sqlite-flow").await
}

#[tokio::test]
async fn sqlite_suspended_store_edges() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("suspended-edges.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    edge_support::suspended_store_edges(&store, &edges, "sqlite-suspended").await
}

#[tokio::test]
async fn sqlite_dsl_progress_edges() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("progress-edges.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    edge_support::dsl_progress_edges(&store, &edges, "sqlite-progress").await
}

#[tokio::test]
async fn sqlite_state_machine_edges() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("snapshot-edges.db").await?;
    let store = SqlStateMachineStore::<EdgeState>::connect_sqlite(&url).await?;
    store.migrate().await?;
    edge_support::state_machine_edges(&store, &edges, "sqlite-snapshot").await?;
    let big = SqlStateMachineStore::<BigState>::connect_sqlite(&url).await?;
    edge_support::state_machine_oversize_encode(&big, "sqlite-snapshot").await
}

#[tokio::test]
async fn sqlite_scheduler_edges() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("scheduler-edges.db").await?;
    let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    scheduler.migrate().await?;
    edge_support::scheduler_edges(&scheduler, &edges, "sqlite-scheduler").await
}

#[tokio::test]
async fn sqlite_timeout_edges() -> CatgaResult<()> {
    let (_directory, url, _edges) = harness("timeout-edges.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    edge_support::timeout_edges(&store).await
}

#[tokio::test]
async fn sqlite_flow_heartbeat_exhausts_its_bounded_retries_under_contention() -> CatgaResult<()> {
    let (_directory, url, edges) = harness("flow-cas.db").await?;
    let store = SqlFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let _serial = SQLITE_CAS_SERIAL.lock().await;
    edge_support::flow_heartbeat_cas_exhaustion(&store, &std::sync::Arc::new(edges), "sqlite-flow")
        .await
}

#[tokio::test]
async fn sqlite_suspended_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let (_directory, url, edges) = harness("suspended-cas.db").await?;
    let store = SqlSuspendedFlowStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let _serial = SQLITE_CAS_SERIAL.lock().await;
    edge_support::suspended_cas_exhaustion(&store, &std::sync::Arc::new(edges), "sqlite-suspended")
        .await
}

#[tokio::test]
async fn sqlite_dsl_progress_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let (_directory, url, edges) = harness("progress-cas.db").await?;
    let store = SqlDslStepProgressStore::connect_sqlite(&url).await?;
    store.migrate().await?;
    let _serial = SQLITE_CAS_SERIAL.lock().await;
    edge_support::dsl_progress_cas_exhaustion(
        &store,
        &std::sync::Arc::new(edges),
        "sqlite-progress",
    )
    .await
}

#[tokio::test]
async fn sqlite_state_machine_updates_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let (_directory, url, edges) = harness("snapshot-cas.db").await?;
    let store = SqlStateMachineStore::<EdgeState>::connect_sqlite(&url).await?;
    store.migrate().await?;
    let _serial = SQLITE_CAS_SERIAL.lock().await;
    edge_support::state_machine_cas_exhaustion(
        &store,
        &std::sync::Arc::new(edges),
        "sqlite-snapshot",
    )
    .await
}

#[tokio::test]
async fn sqlite_constructor_options_and_application_owned_pools() -> CatgaResult<()> {
    let (_directory, url, _edges) = harness("constructors.db").await?;
    let options = SqlFlowStoreOptions::new()
        .max_connections(3)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(2))
        .max_lifetime(Duration::from_secs(60))
        .idle_timeout(Duration::from_secs(30));
    let configured = SqlFlowStore::connect_sqlite_with_options(&url, options).await?;
    configured.migrate().await?;

    let rejected = SqlFlowStoreOptions::new().max_connections(0);
    let error = match SqlFlowStore::connect_sqlite_with_options(&url, rejected).await {
        Ok(_) => panic!("a zero connection limit must be rejected before opening a pool"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Validation);

    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(
            SqliteConnectOptions::from_str(&url)
                .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
                .create_if_missing(true),
        )
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let store = SqlFlowStore::from_sqlite_pool(pool.clone());
    store.migrate().await?;
    let snapshots = SqlStateMachineStore::<EdgeState>::from_sqlite_pool(
        pool,
        MemoryPackSnapshotCodec::default(),
    );
    snapshots.migrate().await?;
    Ok(())
}
