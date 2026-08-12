//! Feature-selected durable SQL Flow-resume scheduler.

#[cfg(feature = "sqlite")]
use std::str::FromStr;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
use std::time::{Duration, SystemTime};

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
use catga_core::flow::{DueFlowScheduler, FlowScheduler, ScheduledResume};

use crate::backend::Backend;

/// A durable SQL scheduler for explicitly claimed Flow resumptions.
///
/// Calling `migrate` once creates only durable state. This type never creates a worker, timer,
/// or background task; applications call `DueFlowScheduler::claim_due` themselves.
///
/// # Leasing semantics
///
/// Each resume is stored once per `(flow_id, state_id)` pair behind a fixed-width target key, so
/// rescheduling the same suspended step replaces the due time instead of accumulating duplicate
/// rows. `claim_due` takes a bounded batch of due rows inside one transaction with skip-locked
/// selection (or the dialect's equivalent guarded update), stamps the caller's lease, and returns
/// the receipts. The owner then acknowledges completion with `ack_due`, returns the row to the
/// pool with `release_due`, or extends its lease with `renew_due`; a lease that expires makes the
/// row claimable by another owner.
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use catga_core::flow::{DueFlowScheduler, FlowScheduler};
/// use catga_flow_store::SqlFlowScheduler;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let directory = tempfile::tempdir()?;
/// let url = format!("sqlite://{}", directory.path().join("schedules.db").display());
/// let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
/// scheduler.migrate().await?;
///
/// let already_due = SystemTime::now() - Duration::from_secs(1);
/// let schedule_id = scheduler.schedule_resume("flow-7", "state-7", already_due).await?;
/// let claimed = scheduler
///     .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 8)
///     .await?;
/// assert_eq!(claimed.len(), 1);
/// assert!(scheduler.ack_due("worker-a", &schedule_id).await?);
/// # Ok(())
/// # }
/// ```
pub struct SqlFlowScheduler {
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

impl SqlFlowScheduler {
    /// Opens a SQL Server scheduler with a bounded bb8/Tiberius pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let scheduler = SqlFlowScheduler::connect_mssql("server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true").await?;
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Adopts an application-owned SQL Server pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let manager = bb8_tiberius::ConnectionManager::build(
    ///     "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true",
    /// )?;
    /// let pool = bb8::Pool::builder().max_size(8).build(manager).await?;
    /// let scheduler = SqlFlowScheduler::from_mssql_pool(pool);
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mssql")]
    pub fn from_mssql_pool(pool: crate::MssqlPool) -> Self {
        Self {
            backend: Backend::Mssql(pool),
        }
    }

    /// Opens a MySQL 8 scheduler with a bounded SQLx pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let scheduler = SqlFlowScheduler::connect_mysql("mysql://catga:catga@localhost/catga").await?;
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Adopts an application-owned MySQL pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = sqlx::mysql::MySqlPoolOptions::new()
    ///     .max_connections(8)
    ///     .connect("mysql://catga:catga@localhost/catga")
    ///     .await?;
    /// let scheduler = SqlFlowScheduler::from_mysql_pool(pool);
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "mysql")]
    pub fn from_mysql_pool(pool: sqlx::MySqlPool) -> Self {
        Self {
            backend: Backend::MySql(pool),
        }
    }

    /// Opens a PostgreSQL scheduler with a bounded SQLx pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let scheduler = SqlFlowScheduler::connect_postgres("postgres://catga:catga@localhost/catga").await?;
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Adopts an application-owned PostgreSQL pool.
    ///
    /// ```no_run
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = sqlx::postgres::PgPoolOptions::new()
    ///     .max_connections(8)
    ///     .connect("postgres://catga:catga@localhost/catga")
    ///     .await?;
    /// let scheduler = SqlFlowScheduler::from_postgres_pool(pool);
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "postgres")]
    pub fn from_postgres_pool(pool: sqlx::PgPool) -> Self {
        Self {
            backend: Backend::Postgres(pool),
        }
    }

    /// Opens a SQLite scheduler with a bounded WAL pool.
    ///
    /// ```
    /// use catga_flow_store::SqlFlowScheduler;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let directory = tempfile::tempdir()?;
    /// let url = format!("sqlite://{}", directory.path().join("schedules.db").display());
    /// let scheduler = SqlFlowScheduler::connect_sqlite(&url).await?;
    /// scheduler.migrate().await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Applies this backend's idempotent scheduler schema migration.
    ///
    /// Creates the schedule table and its due-time index once; rerunning is a no-op. Poll
    /// `claim_due` only after the migration has completed successfully — the scheduler performs
    /// no implicit migration on first use.
    #[cfg(any(
        feature = "sqlite",
        feature = "mysql",
        feature = "postgres",
        feature = "mssql"
    ))]
    pub async fn migrate(&self) -> CatgaResult<()> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => crate::sqlite_scheduler::migrate(pool).await,
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_scheduler::migrate(pool).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => crate::postgres_scheduler::migrate(pool).await,
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_scheduler::migrate(pool).await,
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
impl FlowScheduler for SqlFlowScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::schedule_resume(pool, flow_id, state_id, due_at).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_scheduler::schedule_resume(pool, flow_id, state_id, due_at).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::schedule_resume(pool, flow_id, state_id, due_at).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_scheduler::schedule_resume(pool, flow_id, state_id, due_at).await
            }
        }
    }

    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::cancel_resume(pool, schedule_id).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_scheduler::cancel_resume(pool, schedule_id).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::cancel_resume(pool, schedule_id).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_scheduler::cancel_resume(pool, schedule_id).await,
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
impl DueFlowScheduler for SqlFlowScheduler {
    async fn claim_due(
        &self,
        owner: &str,
        now: SystemTime,
        lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::claim_due(pool, owner, now, lease_for, limit).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_scheduler::claim_due(pool, owner, now, lease_for, limit).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::claim_due(pool, owner, now, lease_for, limit).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_scheduler::claim_due(pool, owner, now, lease_for, limit).await
            }
        }
    }

    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::ack_due(pool, owner, schedule_id).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => crate::mysql_scheduler::ack_due(pool, owner, schedule_id).await,
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::ack_due(pool, owner, schedule_id).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => crate::mssql_scheduler::ack_due(pool, owner, schedule_id).await,
        }
    }

    async fn release_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::release_due(pool, owner, schedule_id).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_scheduler::release_due(pool, owner, schedule_id).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::release_due(pool, owner, schedule_id).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_scheduler::release_due(pool, owner, schedule_id).await
            }
        }
    }

    async fn renew_due(
        &self,
        owner: &str,
        schedule_id: &str,
        now: SystemTime,
        lease_for: Duration,
    ) -> CatgaResult<bool> {
        match &self.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite(pool) => {
                crate::sqlite_scheduler::renew_due(pool, owner, schedule_id, now, lease_for).await
            }
            #[cfg(feature = "mysql")]
            Backend::MySql(pool) => {
                crate::mysql_scheduler::renew_due(pool, owner, schedule_id, now, lease_for).await
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pool) => {
                crate::postgres_scheduler::renew_due(pool, owner, schedule_id, now, lease_for).await
            }
            #[cfg(feature = "mssql")]
            Backend::Mssql(pool) => {
                crate::mssql_scheduler::renew_due(pool, owner, schedule_id, now, lease_for).await
            }
        }
    }
}
