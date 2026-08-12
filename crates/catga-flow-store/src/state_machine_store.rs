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
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use catga_core::CatgaResult;
use catga_core::SnapshotCodec;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
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
///
/// ```
/// use catga_core::MemoryPackable;
/// use catga_core::codec::memorypack::{
///     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
///     MemoryPackWriter,
/// };
/// use catga_core::flow::{StateMachineSnapshot, StateMachineStore};
/// use catga_flow_store::SqlStateMachineStore;
///
/// #[derive(Clone, Debug, PartialEq, MemoryPackable)]
/// struct OrderState {
///     items: u32,
///     paid: bool,
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let directory = tempfile::tempdir()?;
/// let url = format!("sqlite://{}", directory.path().join("machines.db").display());
/// let store = SqlStateMachineStore::<OrderState>::connect_sqlite(&url).await?;
/// store.migrate().await?;
///
/// let snapshot = StateMachineSnapshot::new("order-1", OrderState { items: 2, paid: false });
/// assert!(store.create(snapshot).await?);
/// let stored = store
///     .get("order-1")
///     .await?
///     .expect("the snapshot was just created");
/// assert_eq!(stored.state().items, 2);
/// # Ok(())
/// # }
/// ```
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
    ///
    /// ```no_run
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::<OrderState>::connect_mssql("server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true").await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mssql")]
    pub async fn connect_mssql(url: &str) -> CatgaResult<Self> {
        Self::connect_mssql_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a MySQL 8 store using bounded MemoryPack state encoding.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::<OrderState>::connect_mysql("mysql://catga:catga@localhost/catga").await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mysql")]
    pub async fn connect_mysql(url: &str) -> CatgaResult<Self> {
        Self::connect_mysql_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a PostgreSQL store using bounded MemoryPack state encoding.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::<OrderState>::connect_postgres("postgres://catga:catga@localhost/catga").await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "postgres")]
    pub async fn connect_postgres(url: &str) -> CatgaResult<Self> {
        Self::connect_postgres_with_codec(url, MemoryPackSnapshotCodec::default()).await
    }

    /// Opens a SQLite store using bounded MemoryPack state encoding.
    ///
    /// ```
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let directory = tempfile::tempdir()?;
    /// let url = format!("sqlite://{}", directory.path().join("machines.db").display());
    /// let store = SqlStateMachineStore::<OrderState>::connect_sqlite(&url).await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::connect_mssql_with_codec(
    ///     "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true",
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// )
    /// .await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let manager = bb8_tiberius::ConnectionManager::build(
    ///     "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true",
    /// )?;
    /// let pool = bb8::Pool::builder().max_size(8).build(manager).await?;
    /// let store = SqlStateMachineStore::from_mssql_pool(
    ///     pool,
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// );
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mssql")]
    pub fn from_mssql_pool(pool: crate::MssqlPool, codec: C) -> Self {
        Self {
            backend: Backend::Mssql(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a MySQL 8 store with a caller-provided state codec and bounded SQLx pool.
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::connect_mysql_with_codec(
    ///     "mysql://catga:catga@localhost/catga",
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// )
    /// .await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = sqlx::mysql::MySqlPoolOptions::new()
    ///     .max_connections(8)
    ///     .connect("mysql://catga:catga@localhost/catga")
    ///     .await?;
    /// let store = SqlStateMachineStore::from_mysql_pool(
    ///     pool,
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// );
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mysql")]
    pub fn from_mysql_pool(pool: sqlx::MySqlPool, codec: C) -> Self {
        Self {
            backend: Backend::MySql(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a PostgreSQL store with a caller-provided state codec and bounded SQLx pool.
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let store = SqlStateMachineStore::connect_postgres_with_codec(
    ///     "postgres://catga:catga@localhost/catga",
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// )
    /// .await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```no_run
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = sqlx::postgres::PgPoolOptions::new()
    ///     .max_connections(8)
    ///     .connect("postgres://catga:catga@localhost/catga")
    ///     .await?;
    /// let store = SqlStateMachineStore::from_postgres_pool(
    ///     pool,
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// );
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "postgres")]
    pub fn from_postgres_pool(pool: sqlx::PgPool, codec: C) -> Self {
        Self {
            backend: Backend::Postgres(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Opens a SQLite store with a caller-provided state codec, WAL, and five-second busy timeout.
    ///
    /// ```
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let directory = tempfile::tempdir()?;
    /// let url = format!("sqlite://{}", directory.path().join("machines.db").display());
    /// let store = SqlStateMachineStore::connect_sqlite_with_codec(
    ///     &url,
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// )
    /// .await?;
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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
    ///
    /// ```
    /// use std::str::FromStr;
    /// use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
    /// use catga_flow_store::SqlStateMachineStore;
    /// use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    /// # use catga_core::MemoryPackable;
    /// # use catga_core::codec::memorypack::{
    /// #     MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    /// #     MemoryPackWriter,
    /// # };
    /// #
    /// # #[derive(Clone, MemoryPackable)]
    /// # struct OrderState;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let directory = tempfile::tempdir()?;
    /// let url = format!("sqlite://{}", directory.path().join("machines.db").display());
    /// let pool = SqlitePoolOptions::new()
    ///     .max_connections(4)
    ///     .connect_with(SqliteConnectOptions::from_str(&url)?.create_if_missing(true))
    ///     .await?;
    /// let store = SqlStateMachineStore::from_sqlite_pool(
    ///     pool,
    ///     MemoryPackSnapshotCodec::<OrderState>::default(),
    /// );
    /// store.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sqlite")]
    pub fn from_sqlite_pool(pool: sqlx::SqlitePool, codec: C) -> Self {
        Self {
            backend: Backend::Sqlite(pool),
            codec,
            state: PhantomData,
        }
    }

    /// Applies the selected backend's idempotent state-machine schema migration.
    ///
    /// Creates the snapshot table once; rerunning is a no-op. Create or restore snapshots only
    /// after the migration has completed successfully — the store performs no implicit migration
    /// on first use.
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
