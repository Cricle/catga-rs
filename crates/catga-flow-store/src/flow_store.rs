//! The plain durable [`catga_flow::FlowStore`] implementation.

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
use catga_core::CatgaResult;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_flow::{FlowState, FlowStore};

use crate::backend::Backend;

#[cfg(feature = "sqlite")]
const SQLITE_DEFAULT_WRITE_CONNECTIONS: u32 = 1;

/// A feature-selected SQL implementation of the FlowStore contract.
///
/// Construct the store with the constructor for its enabled backend, then migrate it
/// once before serving flow traffic. Each instance owns one bounded pool and has no background
/// tasks.
pub struct SqlFlowStore {
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

impl SqlFlowStore {
    /// Opens a SQL Server store with a bounded bb8/Tiberius pool.
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

    /// Adopts an application-owned SQL Server pool without creating another pool.
    #[cfg(feature = "mssql")]
    pub fn from_mssql_pool(pool: crate::MssqlPool) -> Self {
        Self {
            backend: Backend::Mssql(pool),
        }
    }

    /// Opens a MySQL 8 store with a bounded SQLx pool.
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

    /// Adopts an application-owned MySQL pool without creating another connection pool.
    #[cfg(feature = "mysql")]
    pub fn from_mysql_pool(pool: sqlx::MySqlPool) -> Self {
        Self {
            backend: Backend::MySql(pool),
        }
    }

    /// Opens a PostgreSQL store with a bounded SQLx pool.
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

    /// Adopts an application-owned PostgreSQL pool without creating another connection pool.
    #[cfg(feature = "postgres")]
    pub fn from_postgres_pool(pool: sqlx::PgPool) -> Self {
        Self {
            backend: Backend::Postgres(pool),
        }
    }

    /// Opens a SQLite store with one write connection, WAL journaling, and a five-second busy timeout.
    ///
    /// SQLite permits one writer at a time. Serializing the default write path at the pool avoids
    /// lock-contention tail latency under concurrent flow transitions. Applications with a known
    /// read-heavy workload can configure their own pool and use [`Self::from_sqlite_pool`].
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite(url: &str) -> CatgaResult<Self> {
        use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

        let options = SqliteConnectOptions::from_str(url)
            .map_err(|error| crate::error::database_error("parse SQLite URL", error))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(SQLITE_DEFAULT_WRITE_CONNECTIONS)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(|error| crate::error::database_error("connect SQLite", error))?;
        Ok(Self {
            backend: Backend::Sqlite(pool),
        })
    }

    /// Adopts an application-owned SQLite pool.
    ///
    /// This is the explicit escape hatch for applications that have measured a read-heavy
    /// workload and need a different pool capacity than [`Self::connect_sqlite`] uses for its
    /// single-writer default.
    #[cfg(feature = "sqlite")]
    pub fn from_sqlite_pool(pool: sqlx::SqlitePool) -> Self {
        Self {
            backend: Backend::Sqlite(pool),
        }
    }

    /// Applies this backend's idempotent FlowStore schema migration.
    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    pub async fn migrate(&self) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::migrate(pool).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::migrate(pool).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::migrate(pool).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::migrate(pool).await,
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
impl FlowStore for SqlFlowStore {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::create(pool, state).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::create(pool, state).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::create(pool, state).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::create(pool, state).await,
        }
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::update(pool, expected_version, next).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::update(pool, expected_version, next).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::update(pool, expected_version, next).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::update(pool, expected_version, next).await,
        }
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::get(pool, id).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::get(pool, id).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::get(pool, id).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::get(pool, id).await,
        }
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite::try_claim(pool, flow_type, owner, stale_after).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql::try_claim(pool, flow_type, owner, stale_after).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres::try_claim(pool, flow_type, owner, stale_after).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql::try_claim(pool, flow_type, owner, stale_after).await
            }
        }
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::heartbeat(pool, id, owner, version).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::heartbeat(pool, id, owner, version).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::heartbeat(pool, id, owner, version).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::heartbeat(pool, id, owner, version).await,
        }
    }
}
