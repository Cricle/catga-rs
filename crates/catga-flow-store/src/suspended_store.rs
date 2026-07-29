//! Durable continuation storage backed by the selected SQL backend.

#[cfg(feature = "sqlite")]
use std::str::FromStr;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use std::time::Duration;

#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use async_trait::async_trait;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_core::{CatgaError, CatgaResult};
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_flow::{FlowContinuation, FlowQuery, FlowSummary, SuspendedFlowStore};
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_flow::{TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore};

use crate::backend::Backend;

/// A feature-selected SQL store for restart-safe Flow continuations.
///
/// Migrate the selected backend once before use. The store maintains a physical revision in addition to
/// the Flow business version so a stale continuation cannot overwrite a heartbeat or wait result.
pub struct SqlSuspendedFlowStore {
    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "mysql",
            feature = "postgres",
            feature = "mssql"
        )),
        allow(dead_code)
    )]
    backend: Backend,
}

impl SqlSuspendedFlowStore {
    /// Opens a SQL Server continuation store with a bounded bb8/Tiberius pool.
    #[cfg(feature = "mssql")]
    pub async fn connect_mssql(url: &str) -> CatgaResult<Self> {
        let manager = bb8_tiberius::ConnectionManager::build(url)
            .map_err(|error| crate::error::database_error("parse SQL Server URL", error))?;
        let pool = bb8::Pool::builder()
            .max_size(8)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .await
            .map_err(|error| crate::error::database_error("connect SQL Server", error))?;
        Ok(Self::from_mssql_pool(pool))
    }

    /// Adopts an application-owned SQL Server pool without allocating another pool.
    #[cfg(feature = "mssql")]
    pub fn from_mssql_pool(pool: crate::MssqlPool) -> Self {
        Self {
            backend: Backend::Mssql(pool),
        }
    }

    /// Opens a MySQL 8 continuation store with a bounded SQLx pool.
    #[cfg(feature = "mysql")]
    pub async fn connect_mysql(url: &str) -> CatgaResult<Self> {
        use sqlx::mysql::MySqlPoolOptions;
        let pool = MySqlPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(|error| crate::error::database_error("connect MySQL", error))?;
        Ok(Self::from_mysql_pool(pool))
    }

    /// Adopts an application-owned MySQL pool without allocating another pool.
    #[cfg(feature = "mysql")]
    pub fn from_mysql_pool(pool: sqlx::MySqlPool) -> Self {
        Self {
            backend: Backend::MySql(pool),
        }
    }

    /// Opens a PostgreSQL continuation store with a bounded SQLx pool.
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres(url: &str) -> CatgaResult<Self> {
        use sqlx::postgres::PgPoolOptions;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(|error| crate::error::database_error("connect PostgreSQL", error))?;
        Ok(Self::from_postgres_pool(pool))
    }

    /// Adopts an application-owned PostgreSQL pool without allocating another pool.
    #[cfg(feature = "postgres")]
    pub fn from_postgres_pool(pool: sqlx::PgPool) -> Self {
        Self {
            backend: Backend::Postgres(pool),
        }
    }

    /// Opens a SQLite continuation store with a bounded WAL pool.
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite(url: &str) -> CatgaResult<Self> {
        use sqlx::sqlite::{
            SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
        };

        let options = SqliteConnectOptions::from_str(url)
            .map_err(|error| crate::error::database_error("parse SQLite URL", error))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(|error| crate::error::database_error("connect SQLite", error))?;
        Ok(Self {
            backend: Backend::Sqlite(pool),
        })
    }

    /// Applies this backend's idempotent continuation schema migration.
    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    pub async fn migrate(&self) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_suspended::migrate(pool).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_suspended::migrate(pool).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_suspended::migrate(pool).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_suspended::migrate(pool).await,
        }
    }
}

#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
#[async_trait]
impl SuspendedFlowStore for SqlSuspendedFlowStore {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_suspended::create(pool, continuation).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_suspended::create(pool, continuation).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_suspended::create(pool, continuation).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_suspended::create(pool, continuation).await,
        }
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_suspended::get(pool, flow_id).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_suspended::get(pool, flow_id).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_suspended::get(pool, flow_id).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_suspended::get(pool, flow_id).await,
        }
    }

    async fn get_by_wait_correlation(
        &self,
        correlation_id: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::get_by_wait_correlation(pool, correlation_id).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::get_by_wait_correlation(pool, correlation_id).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::get_by_wait_correlation(pool, correlation_id).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::get_by_wait_correlation(pool, correlation_id).await
            }
        }
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_suspended::query(pool, query).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_suspended::query(pool, query).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_suspended::query(pool, query).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_suspended::query(pool, query).await,
        }
    }

    async fn delete(&self, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::delete(pool, flow_id, expected_version).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::delete(pool, flow_id, expected_version).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::delete(pool, flow_id, expected_version).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::delete(pool, flow_id, expected_version).await
            }
        }
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::update(pool, expected_version, next).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::update(pool, expected_version, next).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::update(pool, expected_version, next).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::update(pool, expected_version, next).await
            }
        }
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_suspended::claim(pool, expected, next).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_suspended::claim(pool, expected, next).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_suspended::claim(pool, expected, next).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_suspended::claim(pool, expected, next).await,
        }
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::record_wait_success(
                    pool, flow_id, version, child_id, payload,
                )
                .await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::record_wait_success(
                    pool, flow_id, version, child_id, payload,
                )
                .await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::record_wait_success(
                    pool, flow_id, version, child_id, payload,
                )
                .await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::record_wait_success(
                    pool, flow_id, version, child_id, payload,
                )
                .await
            }
        }
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::record_wait_failure(
                    pool, flow_id, version, child_id, error,
                )
                .await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::record_wait_failure(pool, flow_id, version, child_id, error)
                    .await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::record_wait_failure(
                    pool, flow_id, version, child_id, error,
                )
                .await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::record_wait_failure(pool, flow_id, version, child_id, error)
                    .await
            }
        }
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_suspended::heartbeat(pool, flow_id, owner, version).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_suspended::heartbeat(pool, flow_id, owner, version).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_suspended::heartbeat(pool, flow_id, owner, version).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_suspended::heartbeat(pool, flow_id, owner, version).await
            }
        }
    }
}

#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
#[async_trait]
impl TimedOutFlowStore for SqlSuspendedFlowStore {
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_timeout::poll(pool, poll).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_timeout::poll(pool, poll).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_timeout::poll(pool, poll).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_timeout::poll(pool, poll).await,
        }
    }

    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_timeout::acknowledge(pool, receipt).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_timeout::acknowledge(pool, receipt).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_timeout::acknowledge(pool, receipt).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_timeout::acknowledge(pool, receipt).await,
        }
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_timeout::release(pool, receipt).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_timeout::release(pool, receipt).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_timeout::release(pool, receipt).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_timeout::release(pool, receipt).await,
        }
    }
}
