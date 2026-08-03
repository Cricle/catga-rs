//! The plain durable [`catga_core::flow::FlowStore`] implementation.

use std::time::Duration;

#[cfg(feature = "sqlite")]
use std::str::FromStr;

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
use catga_core::flow::{FlowState, FlowStore};
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::backend::Backend;

#[cfg(feature = "sqlite")]
const SQLITE_DEFAULT_WRITE_CONNECTIONS: u32 = 1;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
const SERVER_DEFAULT_CONNECTIONS: u32 = 8;

#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// Optional pool settings for one [`SqlFlowStore`] connection constructor.
///
/// Every field defaults to the underlying pool library's own behavior (SQLx for SQLite, MySQL,
/// and PostgreSQL; bb8 for SQL Server). Set a field only to override that library default; unset
/// fields are left to the library rather than re-managed here. The one exception is the acquire
/// timeout, which Catga pins to a fail-fast five seconds unless overridden.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlFlowStoreOptions {
    connection_limit: Option<u32>,
    min_connections: Option<u32>,
    acquire_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
}

impl SqlFlowStoreOptions {
    /// Creates options that retain the constructor's backend-specific defaults.
    pub const fn new() -> Self {
        Self {
            connection_limit: None,
            min_connections: None,
            acquire_timeout: None,
            max_lifetime: None,
            idle_timeout: None,
        }
    }

    /// Selects the maximum number of connections owned by this store's pool.
    ///
    /// The value must be greater than zero; constructors reject zero before network I/O.
    #[must_use]
    pub const fn max_connections(mut self, connection_limit: u32) -> Self {
        self.connection_limit = Some(connection_limit);
        self
    }

    /// Selects how many connections are established eagerly and kept warm.
    ///
    /// A warm connection avoids paying TCP and authentication latency on the first request after
    /// startup or a quiet period. The value is clamped to the maximum connection count.
    #[must_use]
    pub const fn min_connections(mut self, min_connections: u32) -> Self {
        self.min_connections = Some(min_connections);
        self
    }

    /// Selects how long a request waits for a free connection before failing.
    #[must_use]
    pub const fn acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = Some(acquire_timeout);
        self
    }

    /// Selects the longest a connection lives before it is recycled.
    ///
    /// Periodic recycling keeps long-running stores resilient to server restarts and intermediate
    /// idle-timeout drops.
    #[must_use]
    pub const fn max_lifetime(mut self, max_lifetime: Duration) -> Self {
        self.max_lifetime = Some(max_lifetime);
        self
    }

    /// Selects how long a connection may sit idle before it is released.
    #[must_use]
    pub const fn idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = Some(idle_timeout);
        self
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    fn resolve(self, default_limit: u32) -> CatgaResult<u32> {
        let connection_limit = self.connection_limit.unwrap_or(default_limit);
        if connection_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "SQL FlowStore pool capacity must be greater than zero",
            ));
        }
        Ok(connection_limit)
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    fn resolved_min_connections(&self, connection_limit: u32) -> Option<u32> {
        self.min_connections.map(|min| min.min(connection_limit))
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    fn resolved_acquire_timeout(&self) -> Duration {
        self.acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT)
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    const fn resolved_max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime
    }

    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    const fn resolved_idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }
}

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
        Self::connect_mssql_with_options(url, SqlFlowStoreOptions::default()).await
    }

    /// Opens a SQL Server store with an explicit pool capacity.
    #[cfg(feature = "mssql")]
    pub async fn connect_mssql_with_options(
        url: &str,
        options: SqlFlowStoreOptions,
    ) -> CatgaResult<Self> {
        let manager = bb8_tiberius::ConnectionManager::build(url)
            .map_err(|error| crate::error::database_error("parse SQL Server URL", error))?;
        let connection_limit = options.resolve(SERVER_DEFAULT_CONNECTIONS)?;
        let mut builder = bb8::Pool::builder()
            .max_size(connection_limit)
            .connection_timeout(options.resolved_acquire_timeout());
        if let Some(min_idle) = options.resolved_min_connections(connection_limit) {
            builder = builder.min_idle(Some(min_idle));
        }
        if let Some(max_lifetime) = options.resolved_max_lifetime() {
            builder = builder.max_lifetime(Some(max_lifetime));
        }
        if let Some(idle_timeout) = options.resolved_idle_timeout() {
            builder = builder.idle_timeout(Some(idle_timeout));
        }
        let pool = builder
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
        Self::connect_mysql_with_options(url, SqlFlowStoreOptions::default()).await
    }

    /// Opens a MySQL 8 store with an explicit pool capacity.
    #[cfg(feature = "mysql")]
    pub async fn connect_mysql_with_options(
        url: &str,
        options: SqlFlowStoreOptions,
    ) -> CatgaResult<Self> {
        use sqlx::mysql::MySqlPoolOptions;

        let connection_limit = options.resolve(SERVER_DEFAULT_CONNECTIONS)?;
        let mut pool = MySqlPoolOptions::new()
            .max_connections(connection_limit)
            .acquire_timeout(options.resolved_acquire_timeout());
        if let Some(min_connections) = options.resolved_min_connections(connection_limit) {
            pool = pool.min_connections(min_connections);
        }
        if let Some(max_lifetime) = options.resolved_max_lifetime() {
            pool = pool.max_lifetime(Some(max_lifetime));
        }
        if let Some(idle_timeout) = options.resolved_idle_timeout() {
            pool = pool.idle_timeout(Some(idle_timeout));
        }
        let pool = pool
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
        Self::connect_postgres_with_options(url, SqlFlowStoreOptions::default()).await
    }

    /// Opens a PostgreSQL store with an explicit pool capacity.
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres_with_options(
        url: &str,
        options: SqlFlowStoreOptions,
    ) -> CatgaResult<Self> {
        use sqlx::postgres::PgPoolOptions;

        let connection_limit = options.resolve(SERVER_DEFAULT_CONNECTIONS)?;
        let mut pool = PgPoolOptions::new()
            .max_connections(connection_limit)
            .acquire_timeout(options.resolved_acquire_timeout());
        if let Some(min_connections) = options.resolved_min_connections(connection_limit) {
            pool = pool.min_connections(min_connections);
        }
        if let Some(max_lifetime) = options.resolved_max_lifetime() {
            pool = pool.max_lifetime(Some(max_lifetime));
        }
        if let Some(idle_timeout) = options.resolved_idle_timeout() {
            pool = pool.idle_timeout(Some(idle_timeout));
        }
        let pool = pool
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
    /// read-heavy workload can configure the capacity with [`Self::connect_sqlite_with_options`]
    /// or provide a pool through [`Self::from_sqlite_pool`].
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite(url: &str) -> CatgaResult<Self> {
        Self::connect_sqlite_with_options(url, SqlFlowStoreOptions::default()).await
    }

    /// Opens a SQLite store with an explicit pool capacity.
    #[cfg(feature = "sqlite")]
    pub async fn connect_sqlite_with_options(
        url: &str,
        options: SqlFlowStoreOptions,
    ) -> CatgaResult<Self> {
        use sqlx::sqlite::{
            SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
        };

        let connect_options = SqliteConnectOptions::from_str(url)
            .map_err(|error| crate::error::database_error("parse SQLite URL", error))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let connection_limit = options.resolve(SQLITE_DEFAULT_WRITE_CONNECTIONS)?;
        let mut pool = SqlitePoolOptions::new()
            .max_connections(connection_limit)
            .acquire_timeout(options.resolved_acquire_timeout());
        if let Some(min_connections) = options.resolved_min_connections(connection_limit) {
            pool = pool.min_connections(min_connections);
        }
        if let Some(max_lifetime) = options.resolved_max_lifetime() {
            pool = pool.max_lifetime(Some(max_lifetime));
        }
        if let Some(idle_timeout) = options.resolved_idle_timeout() {
            pool = pool.idle_timeout(Some(idle_timeout));
        }
        let pool = pool
            .connect_with(connect_options)
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

    async fn create_batch(&self, states: Vec<FlowState>) -> CatgaResult<Vec<bool>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite::create_batch(pool, states).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql::create_batch(pool, states).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres::create_batch(pool, states).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql::create_batch(pool, states).await,
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
