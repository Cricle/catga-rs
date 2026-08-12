//! SQL Server real-service edge-case coverage driven by the shared [`edge_support`] contracts.
//!
//! Every test provisions an isolated database so the raw column rewrites that prove
//! corruption fencing cannot disturb the other contracts sharing the service.
#![cfg(feature = "mssql")]

use std::time::Duration;

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::flow::{
    DslStepProgressStore, FlowScheduler, FlowState, FlowStore, StateMachineStore,
    SuspendedFlowStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow_store::{
    MssqlPool, SqlDslStepProgressStore, SqlFlowScheduler, SqlFlowStore, SqlFlowStoreOptions,
    SqlStateMachineStore, SqlSuspendedFlowStore,
};
use tiberius::Query;

mod edge_support;
#[path = "sql_contracts.rs"]
mod sql_contracts;

use edge_support::{BigState, EdgeDialect, EdgeState};

struct MssqlEdges {
    pool: MssqlPool,
}

fn unavailable(operation: &str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(
        ErrorCode::Unavailable,
        format!("SQL Server edge-test {operation}: {error}"),
    )
}

fn internal(operation: &str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(
        ErrorCode::Internal,
        format!("SQL Server edge-test {operation}: {error}"),
    )
}

impl MssqlEdges {
    async fn connect(url: &str) -> CatgaResult<Self> {
        let manager = bb8_tiberius::ConnectionManager::build(url)
            .map_err(|error| unavailable("build edge connection", error))?;
        let pool = bb8::Pool::builder()
            .max_size(4)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .await
            .map_err(|error| unavailable("connect edge pool", error))?;
        Ok(Self { pool })
    }

    async fn execute(&self, sql: &'static str, binds: &[EdgeBind<'_>]) -> CatgaResult<()> {
        let mut statement = Query::new(sql);
        for bind in binds {
            match bind {
                EdgeBind::Text(value) => statement.bind(*value),
                EdgeBind::Bytes(value) => statement.bind(*value),
                EdgeBind::Integer(value) => statement.bind(*value),
            }
        }
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|error| unavailable("acquire edge connection", error))?;
        statement
            .execute(&mut connection)
            .await
            .map_err(|error| internal("execute rewrite", error))?;
        Ok(())
    }

    async fn payload(&self, sql: &'static str, flow_id: &str) -> CatgaResult<Vec<u8>> {
        let mut statement = Query::new(sql);
        statement.bind(flow_id);
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|error| unavailable("acquire edge connection", error))?;
        let row = statement
            .query(&mut connection)
            .await
            .map_err(|error| internal("read payload", error))?
            .into_row()
            .await
            .map_err(|error| internal("read payload row", error))?
            .ok_or_else(|| internal("read payload row", "no row"))?;
        let payload = row
            .try_get::<&[u8], _>("payload")
            .map_err(|error| internal("decode payload", error))?
            .ok_or_else(|| internal("decode payload", "NULL payload"))?;
        Ok(payload.to_vec())
    }
}

enum EdgeBind<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
    Integer(i64),
}

#[async_trait]
impl EdgeDialect for MssqlEdges {
    async fn set_flow_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_states SET flow_id = @P1 WHERE flow_id = @P2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn flow_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM dbo.catga_flow_states WHERE flow_id = @P1",
            flow_id,
        )
        .await
    }

    async fn set_flow_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_states SET payload = @P1 WHERE flow_id = @P2",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_flow_heartbeat_ms(&self, flow_id: &str, heartbeat_ms: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_states SET heartbeat_ms = @P1 WHERE flow_id = @P2",
            &[EdgeBind::Integer(heartbeat_ms), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn set_continuation_identity(&self, flow_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_continuations SET flow_id = @P1 WHERE flow_id = @P2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn continuation_payload(&self, flow_id: &str) -> CatgaResult<Vec<u8>> {
        self.payload(
            "SELECT payload FROM dbo.catga_flow_continuations WHERE flow_id = @P1",
            flow_id,
        )
        .await
    }

    async fn set_continuation_payload(&self, flow_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_continuations SET payload = @P1 WHERE flow_id = @P2",
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
            "UPDATE dbo.catga_flow_continuations \
             SET wait_correlation = @P1, wait_correlation_key = @P2 WHERE flow_id = @P3",
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
            "UPDATE dbo.catga_flow_continuations SET status = @P1 WHERE flow_id = @P2",
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
            "UPDATE dbo.catga_flow_continuations SET updated_at_subsec_ns = @P1 WHERE flow_id = @P2",
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
            "UPDATE dbo.catga_dsl_step_progress SET flow_id = @P1 \
             WHERE flow_id = @P2 AND step_index = @P3",
            &[
                EdgeBind::Text(replacement),
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn progress_payload(&self, flow_id: &str, step_index: u32) -> CatgaResult<Vec<u8>> {
        let mut statement = Query::new(
            "SELECT payload FROM dbo.catga_dsl_step_progress \
             WHERE flow_id = @P1 AND step_index = @P2",
        );
        statement.bind(flow_id);
        statement.bind(i64::from(step_index));
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|error| unavailable("acquire edge connection", error))?;
        let row = statement
            .query(&mut connection)
            .await
            .map_err(|error| internal("read progress payload", error))?
            .into_row()
            .await
            .map_err(|error| internal("read progress payload row", error))?
            .ok_or_else(|| internal("read progress payload row", "no row"))?;
        let payload = row
            .try_get::<&[u8], _>("payload")
            .map_err(|error| internal("decode progress payload", error))?
            .ok_or_else(|| internal("decode progress payload", "NULL payload"))?;
        Ok(payload.to_vec())
    }

    async fn set_progress_payload(
        &self,
        flow_id: &str,
        step_index: u32,
        payload: &[u8],
    ) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_dsl_step_progress SET payload = @P1 \
             WHERE flow_id = @P2 AND step_index = @P3",
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
            "UPDATE dbo.catga_state_machine_snapshots SET instance_id = @P1 WHERE instance_id = @P2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_version(&self, instance_id: &str, version: i64) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_state_machine_snapshots SET version = @P1 WHERE instance_id = @P2",
            &[EdgeBind::Integer(version), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_snapshot_payload(&self, instance_id: &str, payload: &[u8]) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_state_machine_snapshots SET payload = @P1 WHERE instance_id = @P2",
            &[EdgeBind::Bytes(payload), EdgeBind::Text(instance_id)],
        )
        .await
    }

    async fn set_schedule_identity(&self, schedule_id: &str, replacement: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_schedules SET flow_id = @P1 WHERE schedule_id = @P2",
            &[EdgeBind::Text(replacement), EdgeBind::Text(schedule_id)],
        )
        .await
    }

    async fn bump_flow_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_states SET revision = revision + 1 WHERE flow_id = @P1",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_continuation_revision(&self, flow_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_flow_continuations SET revision = revision + 1 WHERE flow_id = @P1",
            &[EdgeBind::Text(flow_id)],
        )
        .await
    }

    async fn bump_progress_revision(&self, flow_id: &str, step_index: u32) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_dsl_step_progress SET revision = revision + 1 \
             WHERE flow_id = @P1 AND step_index = @P2",
            &[
                EdgeBind::Text(flow_id),
                EdgeBind::Integer(i64::from(step_index)),
            ],
        )
        .await
    }

    async fn bump_snapshot_revision(&self, instance_id: &str) -> CatgaResult<()> {
        self.execute(
            "UPDATE dbo.catga_state_machine_snapshots SET revision = revision + 1 \
             WHERE instance_id = @P1",
            &[EdgeBind::Text(instance_id)],
        )
        .await
    }
}

async fn create_mssql_database(base_url: &str) -> CatgaResult<(MssqlPool, String, String)> {
    let manager = bb8_tiberius::ConnectionManager::build(base_url)
        .map_err(|error| unavailable("build admin connection", error))?;
    let admin = bb8::Pool::builder()
        .max_size(4)
        .build(manager)
        .await
        .map_err(|error| unavailable("connect admin pool", error))?;
    let database = format!("catga_edge_{}", uuid::Uuid::new_v4().simple());
    {
        let mut connection = admin
            .get()
            .await
            .map_err(|error| unavailable("acquire admin connection", error))?;
        connection
            .simple_query(format!("CREATE DATABASE [{database}]").as_str())
            .await
            .map_err(|error| unavailable("create edge database", error))?
            .into_first_result()
            .await
            .map_err(|error| unavailable("create edge database", error))?;
    }
    // ADO connection parsing keeps the final value of a repeated key, so appending the database
    // redirects this test without dropping any operator-supplied setting.
    Ok((admin, format!("{base_url};Database={database}"), database))
}

async fn drop_mssql_database(admin: &MssqlPool, database: &str) -> CatgaResult<()> {
    let mut connection = admin
        .get()
        .await
        .map_err(|error| unavailable("acquire cleanup connection", error))?;
    connection
        .simple_query(
            format!(
                "ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; \
                 DROP DATABASE [{database}]"
            )
            .as_str(),
        )
        .await
        .map_err(|error| unavailable("drop edge database", error))?
        .into_first_result()
        .await
        .map_err(|error| unavailable("drop edge database", error))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MssqlEdges::connect(url.as_str()).await?;
        edge_support::flow_store_edges(&store, &edges, "mssql-flow").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_suspended_store_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MssqlEdges::connect(url.as_str()).await?;
        edge_support::suspended_store_edges(&store, &edges, "mssql-suspended").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_dsl_progress_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MssqlEdges::connect(url.as_str()).await?;
        edge_support::dsl_progress_edges(&store, &edges, "mssql-progress").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_state_machine_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = MssqlEdges::connect(url.as_str()).await?;
        edge_support::state_machine_edges(&store, &edges, "mssql-snapshot").await?;
        let big = SqlStateMachineStore::<BigState>::connect_mssql(url.as_str()).await?;
        edge_support::state_machine_oversize_encode(&big, "mssql-snapshot").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_scheduler_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let scheduler = SqlFlowScheduler::connect_mssql(url.as_str()).await?;
        scheduler.migrate().await?;
        let edges = MssqlEdges::connect(url.as_str()).await?;
        edge_support::scheduler_edges(&scheduler, &edges, "mssql-scheduler").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_timeout_edges() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        edge_support::timeout_edges(&store).await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// Proves the flow-state migration upgrades a pre-keyed legacy table in place.
///
/// The legacy layout predates the indexed `flow_type_key` column, so the migration must add the
/// column, backfill every existing row, and only then enforce `NOT NULL`.
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_migration_backfills_legacy_type_keys() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let legacy = MssqlEdges::connect(url.as_str()).await?;
        legacy
            .execute(
                "CREATE TABLE dbo.catga_flow_states (\
                   flow_key BINARY(32) NOT NULL PRIMARY KEY, flow_id NVARCHAR(4000) NOT NULL, \
                   flow_type NVARCHAR(4000) NOT NULL, status BIGINT NOT NULL, \
                   version BIGINT NOT NULL, heartbeat_ms BIGINT NOT NULL, \
                   revision BIGINT NOT NULL, payload VARBINARY(MAX) NOT NULL); \
                 INSERT INTO dbo.catga_flow_states \
                   (flow_key, flow_id, flow_type, status, version, heartbeat_ms, revision, payload) \
                 VALUES (0x0707070707070707070707070707070707070707070707070707070707070707, \
                   N'legacy-flow', N'legacy-type', 0, 0, 0, 0, 0x00);",
                &[],
            )
            .await?;

        let store = SqlFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        {
            let mut connection = legacy
                .pool
                .get()
                .await
                .map_err(|error| unavailable("acquire verify connection", error))?;
            let row = connection
                .simple_query(
                    "SELECT CAST(COUNT(*) AS BIGINT) AS missing \
                     FROM dbo.catga_flow_states WHERE flow_type_key IS NULL;",
                )
                .await
                .map_err(|error| internal("verify legacy backfill", error))?
                .into_row()
                .await
                .map_err(|error| internal("verify legacy backfill row", error))?
                .ok_or_else(|| internal("verify legacy backfill row", "no row"))?;
            let missing: i64 = row
                .get("missing")
                .ok_or_else(|| internal("decode legacy backfill", "NULL count"))?;
            assert_eq!(missing, 0, "the migration must backfill every legacy type key");
        }

        let flow = FlowState::new("upgraded-flow", "upgraded-type", [], "node-a");
        assert!(store.create(flow.clone()).await?);
        assert_eq!(
            store.get("upgraded-flow").await?,
            Some(flow),
            "the upgraded schema must serve new flows"
        );
        Ok(())
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_heartbeat_exhausts_its_bounded_retries_under_contention() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MssqlEdges::connect(url.as_str()).await?);
        edge_support::flow_heartbeat_cas_exhaustion(&store, &edges, "mssql-flow").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_suspended_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlSuspendedFlowStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MssqlEdges::connect(url.as_str()).await?);
        edge_support::suspended_cas_exhaustion(&store, &edges, "mssql-suspended").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_dsl_progress_mutations_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlDslStepProgressStore::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MssqlEdges::connect(url.as_str()).await?);
        edge_support::dsl_progress_cas_exhaustion(&store, &edges, "mssql-progress").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_state_machine_updates_exhaust_their_bounded_retries_under_contention()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let store = SqlStateMachineStore::<EdgeState>::connect_mssql(url.as_str()).await?;
        store.migrate().await?;
        let edges = std::sync::Arc::new(MssqlEdges::connect(url.as_str()).await?);
        edge_support::state_machine_cas_exhaustion(&store, &edges, "mssql-snapshot").await
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// A pool that cannot reach a server must surface the acquisition failure from every adapter.
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_adapters_surface_pool_acquisition_failures() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let _ = base;
    let dead_url =
        "server=tcp:127.0.0.1,1;User Id=sa;Password=catga-dead-pool;TrustServerCertificate=true";
    let dead_pool = || {
        let manager = bb8_tiberius::ConnectionManager::build(dead_url)
            .map_err(|error| unavailable("build dead pool", error))?;
        Ok::<MssqlPool, CatgaError>(
            bb8::Pool::builder()
                .max_size(4)
                .connection_timeout(Duration::from_millis(500))
                .build_unchecked(manager),
        )
    };
    assert!(
        SqlFlowStore::from_mssql_pool(dead_pool()?)
            .migrate()
            .await
            .is_err()
    );
    assert!(
        SqlFlowStore::from_mssql_pool(dead_pool()?)
            .get("unreachable")
            .await
            .is_err()
    );
    assert!(
        SqlSuspendedFlowStore::from_mssql_pool(dead_pool()?)
            .migrate()
            .await
            .is_err()
    );
    assert!(
        SqlSuspendedFlowStore::from_mssql_pool(dead_pool()?)
            .get("unreachable")
            .await
            .is_err()
    );
    assert!(
        SqlFlowScheduler::from_mssql_pool(dead_pool()?)
            .migrate()
            .await
            .is_err()
    );
    assert!(
        SqlFlowScheduler::from_mssql_pool(dead_pool()?)
            .cancel_resume("unreachable")
            .await
            .is_err()
    );
    assert!(
        SqlDslStepProgressStore::from_mssql_pool(dead_pool()?)
            .migrate()
            .await
            .is_err()
    );
    assert!(
        SqlDslStepProgressStore::from_mssql_pool(dead_pool()?)
            .get("unreachable", 0)
            .await
            .is_err()
    );
    assert!(
        SqlStateMachineStore::<EdgeState>::from_mssql_pool(
            dead_pool()?,
            MemoryPackSnapshotCodec::default(),
        )
        .migrate()
        .await
        .is_err()
    );
    assert!(
        SqlStateMachineStore::<EdgeState>::from_mssql_pool(
            dead_pool()?,
            MemoryPackSnapshotCodec::default(),
        )
        .get("unreachable")
        .await
        .is_err()
    );
    Ok(())
}

/// A contending migrator holding the FlowStore schema lock must turn a concurrent migration
/// into a bounded error rather than an unbounded wait.
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_migration_fails_fast_when_the_schema_lock_is_held() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let blocker = MssqlEdges::connect(url.as_str()).await?;
        let mut connection = blocker
            .pool
            .get()
            .await
            .map_err(|error| unavailable("acquire lock holder", error))?;
        connection
            .simple_query(
                "BEGIN TRANSACTION; \
                 EXEC sys.sp_getapplock @Resource = N'catga_flow_states_schema', \
                   @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 5000;",
            )
            .await
            .map_err(|error| internal("hold schema lock", error))?
            .into_first_result()
            .await
            .map_err(|error| internal("hold schema lock", error))?;

        let store = SqlFlowStore::connect_mssql(url.as_str()).await?;
        let migration = store.migrate().await;
        connection
            .simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
            .await
            .map_err(|error| internal("release schema lock", error))?
            .into_first_result()
            .await
            .map_err(|error| internal("release schema lock", error))?;
        assert!(
            migration.is_err(),
            "a held schema lock must fail the waiting migration"
        );
        Ok(())
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

/// A legacy row whose flow type is SQL NULL must roll the migration back with an explicit
/// error instead of committing a half-upgraded schema.
#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_flow_migration_rolls_back_when_a_legacy_type_key_cannot_be_derived()
-> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let (admin, url, database) = create_mssql_database(base.as_ref()).await?;
    let result = async {
        let legacy = MssqlEdges::connect(url.as_str()).await?;
        legacy
            .execute(
                "CREATE TABLE dbo.catga_flow_states (\
                   flow_key BINARY(32) NOT NULL PRIMARY KEY, flow_id NVARCHAR(4000) NOT NULL, \
                   flow_type NVARCHAR(4000) NULL, status BIGINT NOT NULL, \
                   version BIGINT NOT NULL, heartbeat_ms BIGINT NOT NULL, \
                   revision BIGINT NOT NULL, payload VARBINARY(MAX) NOT NULL); \
                 INSERT INTO dbo.catga_flow_states \
                   (flow_key, flow_id, flow_type, status, version, heartbeat_ms, revision, payload) \
                 VALUES (0x0909090909090909090909090909090909090909090909090909090909090909, \
                   N'legacy-null-flow', NULL, 0, 0, 0, 0, 0x00);",
                &[],
            )
            .await?;
        let store = SqlFlowStore::connect_mssql(url.as_str()).await?;
        assert!(
            store.migrate().await.is_err(),
            "a NULL legacy flow type must fail the migration"
        );
        Ok(())
    }
    .await;
    let cleanup = drop_mssql_database(&admin, database.as_str()).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "requires CATGA_MSSQL_URL"]
async fn mssql_constructor_options_and_application_owned_pools() -> CatgaResult<()> {
    let Some(base) = sql_contracts::service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let options = SqlFlowStoreOptions::new()
        .max_connections(4)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .max_lifetime(Duration::from_secs(60))
        .idle_timeout(Duration::from_secs(30));
    let configured = SqlFlowStore::connect_mssql_with_options(base.as_ref(), options).await?;
    configured.migrate().await?;

    async fn owned_pool(url: &str) -> CatgaResult<MssqlPool> {
        let manager = bb8_tiberius::ConnectionManager::build(url)
            .map_err(|error| unavailable("build application pool", error))?;
        bb8::Pool::builder()
            .max_size(2)
            .build(manager)
            .await
            .map_err(|error| unavailable("connect application pool", error))
    }

    SqlFlowStore::from_mssql_pool(owned_pool(base.as_ref()).await?)
        .migrate()
        .await?;
    SqlSuspendedFlowStore::from_mssql_pool(owned_pool(base.as_ref()).await?)
        .migrate()
        .await?;
    SqlFlowScheduler::from_mssql_pool(owned_pool(base.as_ref()).await?)
        .migrate()
        .await?;
    SqlDslStepProgressStore::from_mssql_pool(owned_pool(base.as_ref()).await?)
        .migrate()
        .await?;
    SqlStateMachineStore::<EdgeState>::from_mssql_pool(
        owned_pool(base.as_ref()).await?,
        MemoryPackSnapshotCodec::default(),
    )
    .migrate()
    .await?;
    Ok(())
}
