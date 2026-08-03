//! The public feature-selected SQL store for durable state-machine snapshots.

use std::marker::PhantomData;
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
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_core::CatgaResult;
use catga_core::SnapshotCodec;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_core::flow::{StateMachineSnapshot, StateMachineStore};

use crate::backend::Backend;

/// A feature-selected SQL implementation of durable state-machine snapshots.
///
/// The default type parameter uses bounded MemoryPack encoding; use one of the `*_with_codec`
/// constructors for an application-specific [`SnapshotCodec`]. Construct the store with an enabled
/// backend and call its `migrate` method before accepting state-machine traffic. Rows use a fixed
/// SHA-256 identity key plus the original identity for collision detection, retain both logical
/// versions and physical revisions for bounded compare-and-set, and cap each encoded state at one
/// mebibyte. This type creates neither background tasks nor unbounded queues.
pub struct SqlStateMachineStore<S, C = MemoryPackSnapshotCodec<S>> {
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
    #[cfg_attr(
        not(any(
            feature = "sqlite",
            feature = "mysql",
            feature = "postgres",
            feature = "mssql"
        )),
        allow(dead_code)
    )]
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> SqlStateMachineStore<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Opens a SQL Server store using bounded MemoryPack state encoding.
    #[cfg(feature = "mssql")]
    pub async fn connect_mssql(url: &str) -> CatgaResult<Self> {
        Self::connect_mssql_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a MySQL 8 store using bounded MemoryPack state encoding.
    #[cfg(feature = "mysql")]
    pub async fn connect_mysql(url: &str) -> CatgaResult<Self> {
        Self::connect_mysql_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a PostgreSQL store using bounded MemoryPack state encoding.
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres(url: &str) -> CatgaResult<Self> {
        Self::connect_postgres_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a SQLite store using bounded MemoryPack state encoding.
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite(url: &str) -> CatgaResult<Self> {
        Self::connect_sqlite_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> SqlStateMachineStore<S, C>
where
    C: SnapshotCodec<S>,
{
    /// Opens a SQL Server store with a caller-provided state codec and bounded bb8/Tiberius pool.
    #[cfg(feature = "mssql")]
    pub async fn connect_mssql_with_codec(url: &str, codec: C) -> CatgaResult<Self> {
        let manager = bb8_tiberius::ConnectionManager::build(url)
            .map_err(|error| crate::error::database_error("parse SQL Server URL", error))?;
        let pool = bb8::Pool::builder()
            .max_size(8)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .await
            .map_err(|error| crate::error::database_error("connect SQL Server", error))?;
        Ok(Self::from_mssql_pool(pool, codec))
    }

    /// Adopts an application-owned SQL Server pool and caller-provided state codec.
    #[cfg(feature = "mssql")]
    pub fn from_mssql_pool(pool: crate::MssqlPool, codec: C) -> Self {
        Self {
            backend: Backend::Mssql(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a MySQL 8 store with a caller-provided state codec and bounded SQLx pool.
    #[cfg(feature = "mysql")]
    pub async fn connect_mysql_with_codec(url: &str, codec: C) -> CatgaResult<Self> {
        use sqlx::mysql::MySqlPoolOptions;

        let pool = MySqlPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(|error| crate::error::database_error("connect MySQL", error))?;
        Ok(Self::from_mysql_pool(pool, codec))
    }

    /// Adopts an application-owned MySQL pool and caller-provided state codec.
    #[cfg(feature = "mysql")]
    pub fn from_mysql_pool(pool: sqlx::MySqlPool, codec: C) -> Self {
        Self {
            backend: Backend::MySql(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a PostgreSQL store with a caller-provided state codec and bounded SQLx pool.
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres_with_codec(url: &str, codec: C) -> CatgaResult<Self> {
        use sqlx::postgres::PgPoolOptions;

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .map_err(|error| crate::error::database_error("connect PostgreSQL", error))?;
        Ok(Self::from_postgres_pool(pool, codec))
    }

    /// Adopts an application-owned PostgreSQL pool and caller-provided state codec.
    #[cfg(feature = "postgres")]
    pub fn from_postgres_pool(pool: sqlx::PgPool, codec: C) -> Self {
        Self {
            backend: Backend::Postgres(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a SQLite store with a caller-provided state codec, WAL, and five-second busy timeout.
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite_with_codec(url: &str, codec: C) -> CatgaResult<Self> {
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
        Ok(Self::from_sqlite_pool(pool, codec))
    }

    /// Adopts an application-owned SQLite pool and caller-provided state codec.
    #[cfg(feature = "sqlite")]
    pub fn from_sqlite_pool(pool: sqlx::SqlitePool, codec: C) -> Self {
        Self {
            backend: Backend::Sqlite(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Applies the selected backend's idempotent state-machine schema migration.
    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    pub async fn migrate(&self) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_state_machine::migrate(pool).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_state_machine::migrate(pool).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_state_machine::migrate(pool).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_state_machine::migrate(pool).await,
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
impl<S, C> StateMachineStore<S> for SqlStateMachineStore<S, C>
where
    S: Clone + Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn create(&self, snapshot: StateMachineSnapshot<S>) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_state_machine::create(pool, snapshot, &self.codec).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_state_machine::create(pool, snapshot, &self.codec).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_state_machine::create(pool, snapshot, &self.codec).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_state_machine::create(pool, snapshot, &self.codec).await
            }
        }
    }

    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<S>>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_state_machine::get(pool, instance_id, &self.codec).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_state_machine::get(pool, instance_id, &self.codec).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_state_machine::get(pool, instance_id, &self.codec).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_state_machine::get(pool, instance_id, &self.codec).await
            }
        }
    }

    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<S>,
    ) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_state_machine::update(pool, expected_version, next, &self.codec).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_state_machine::update(pool, expected_version, next, &self.codec).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_state_machine::update(pool, expected_version, next, &self.codec)
                    .await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_state_machine::update(pool, expected_version, next, &self.codec).await
            }
        }
    }
}
