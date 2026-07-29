//! The public feature-selected SQL store for durable DSL step checkpoints.

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
use catga_flow::{DslStepProgress, DslStepProgressStore};

use crate::backend::Backend;

/// A feature-selected SQL implementation of durable DSL step progress.
///
/// Construct this store with the constructor for an enabled backend, then migrate that backend
/// before accepting recoverable DSL flow traffic. Rows use a SHA-256 flow identity key plus the
/// step index, retain the original ID for collision detection, and perform mutations through
/// version and physical-revision compare-and-set guards. The store creates no background tasks.
pub struct SqlDslStepProgressStore {
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

impl SqlDslStepProgressStore {
    /// Opens a SQL Server progress store with a bounded bb8/Tiberius pool.
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

    /// Opens a MySQL 8 progress store with a bounded SQLx pool.
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

    /// Opens a PostgreSQL progress store with a bounded SQLx pool.
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

    /// Opens a SQLite progress store with a bounded WAL pool and five-second busy timeout.
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

    /// Applies this backend's idempotent DSL step-progress schema migration.
    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    pub async fn migrate(&self) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_dsl_progress::migrate(pool).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_dsl_progress::migrate(pool).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_dsl_progress::migrate(pool).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_dsl_progress::migrate(pool).await,
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
impl DslStepProgressStore for SqlDslStepProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_dsl_progress::create(pool, progress).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_dsl_progress::create(pool, progress).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_dsl_progress::create(pool, progress).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_dsl_progress::create(pool, progress).await,
        }
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_dsl_progress::update(pool, expected_version, next).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_dsl_progress::update(pool, expected_version, next).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_dsl_progress::update(pool, expected_version, next).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_dsl_progress::update(pool, expected_version, next).await
            }
        }
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_dsl_progress::get(pool, flow_id, step_index).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_dsl_progress::get(pool, flow_id, step_index).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_dsl_progress::get(pool, flow_id, step_index).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_dsl_progress::get(pool, flow_id, step_index).await,
        }
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_dsl_progress::delete(pool, flow_id, step_index).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_dsl_progress::delete(pool, flow_id, step_index).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_dsl_progress::delete(pool, flow_id, step_index).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_dsl_progress::delete(pool, flow_id, step_index).await
            }
        }
    }
}
