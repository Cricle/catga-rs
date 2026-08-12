#![forbid(unsafe_code)]
//! Feature-gated durable Flow stores backed by SQL databases or Redis.
//!
//! Enable only the adapters an application deploys:
//!
//! - `sqlite`, `mysql`, `postgres`, and `mssql` provide [`SqlFlowStore`],
//!   [`SqlSuspendedFlowStore`], [`SqlFlowScheduler`], [`SqlDslStepProgressStore`], and
//!   [`SqlStateMachineStore`].
//!   Multiple SQL features may be enabled in one binary; the constructor selects the concrete
//!   pool without dynamic SQL or a driver-wide connection abstraction.
//!   Applications call each store's `migrate` method and explicitly poll
//!   [`catga_core::flow::DueFlowScheduler::claim_due`] on `SqlFlowScheduler`; the adapter owns no
//!   worker or timer.
//! - `redis` re-exports `RedisFlows` and `RedisSuspendedFlows` for the plain state and
//!   continuation contracts, plus Redis-backed timeout and scheduling support.
//! - `tls-rustls` enables Rustls support for whichever network SQL drivers are selected.
//!
//! SQL backends share versioned MemoryPack frames, fixed SHA-256 identity keys, bounded
//! optimistic-concurrency retries, bounded discovery scans, and receipt fencing. Dialect-specific
//! modules retain native parameter binding, skip-locked claims, and indexed time ordering without
//! duplicating the public store contract. No adapter creates a worker or background task.
//!
//! # Dialect architecture
//!
//! Every public store type is a thin, feature-selected facade over one private `Backend`
//! connection-pool enum chosen by its `connect_*` or `from_*_pool` constructor. Each trait method
//! (`create`, `update`, `try_claim`, ...) delegates to a per-dialect module that owns the concrete
//! statements:
//!
//! - `sqlite_*`, `mysql_*`, and `postgres_*` modules instantiate the shared `define_server_*!`
//!   macro rules (`server_suspended`, `server_scheduler`, `server_state_machine`,
//!   `server_dsl_progress`, and `server_timeout`). One macro expansion carries the dialect's
//!   schema DDL, its pool type, and two flags: `$postgres` rewrites the canonical `?` bind
//!   placeholders into PostgreSQL's `$1..$n` form at query construction, and `$sqlite` selects
//!   SQLite's `UPDATE ... RETURNING` lease claims and its narrower, hash-key-free continuation
//!   schema. All three sqlx dialects therefore share exactly one audited statement body per
//!   operation.
//! - `mssql_*` modules remain handwritten per store, because SQL Server's Tiberius driver has
//!   no sqlx-compatible query surface for the macro seam to bind.
//!
//! All dialects persist the same layout: a fixed-width SHA-256 identity key for indexing, the
//! original identity for collision detection, a versioned MemoryPack payload frame, a logical
//! business version for compare-and-set transitions, and a physical row revision so heartbeats and
//! continuation writes cannot clobber each other.
//!
//! # SQLite startup
//!
//! Construct and migrate stores at an application-owned startup boundary.
//! The migration is idempotent, but flow processing should start only after it
//! has completed successfully.
//!
//! ```
//! use catga_flow_store::SqlFlowStore;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let directory = tempfile::tempdir()?;
//! let url = format!("sqlite://{}", directory.path().join("flows.db").display());
//! let store = SqlFlowStore::connect_sqlite(&url).await?;
//! store.migrate().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Application-owned pools
//!
//! Production applications can reuse an existing driver pool instead of letting each store create
//! its own: `SqlFlowStore::from_mysql_pool`, `SqlFlowStore::from_postgres_pool`,
//! `SqlFlowStore::from_mssql_pool`, and `SqlFlowStore::from_sqlite_pool`. This keeps the
//! database connection budget under application control. When exposing driver pool types is not
//! desirable, use [`SqlFlowStoreOptions`] with a `connect_*_with_options` constructor.
//!
//! ```
//! use std::str::FromStr;
//! use catga_flow_store::SqlFlowStore;
//! use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let directory = tempfile::tempdir()?;
//! let url = format!("sqlite://{}", directory.path().join("flows.db").display());
//! let pool = SqlitePoolOptions::new()
//!     .max_connections(12)
//!     .connect_with(SqliteConnectOptions::from_str(&url)?.create_if_missing(true))
//!     .await?;
//! let store = SqlFlowStore::from_sqlite_pool(pool);
//! store.migrate().await?;
//! # Ok(())
//! # }
//! ```

mod backend;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod dsl_progress_codec;
mod dsl_progress_store;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod error;
mod flow_store;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod key;
#[cfg(feature = "mssql")]
mod mssql;
#[cfg(feature = "mssql")]
mod mssql_dsl_progress;
#[cfg(feature = "mssql")]
mod mssql_scheduler;
#[cfg(feature = "mssql")]
mod mssql_state_machine;
#[cfg(feature = "mssql")]
mod mssql_suspended;
#[cfg(feature = "mssql")]
mod mssql_timeout;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "mysql")]
mod mysql_dsl_progress;
#[cfg(feature = "mysql")]
mod mysql_scheduler;
#[cfg(feature = "mysql")]
mod mysql_state_machine;
#[cfg(feature = "mysql")]
mod mysql_suspended;
#[cfg(feature = "mysql")]
mod mysql_timeout;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
mod postgres_dsl_progress;
#[cfg(feature = "postgres")]
mod postgres_scheduler;
#[cfg(feature = "postgres")]
mod postgres_schema;
#[cfg(feature = "postgres")]
mod postgres_state_machine;
#[cfg(feature = "postgres")]
mod postgres_suspended;
#[cfg(feature = "postgres")]
mod postgres_timeout;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod scheduler_common;
mod scheduler_store;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod server_dsl_progress;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod server_scheduler;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod server_state_machine;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod server_suspended;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod server_timeout;
#[cfg(any(feature = "sqlite", feature = "mysql", feature = "postgres"))]
mod sql_backend;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod sql_common;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
mod sqlite_dsl_progress;
#[cfg(feature = "sqlite")]
mod sqlite_scheduler;
#[cfg(feature = "sqlite")]
mod sqlite_state_machine;
#[cfg(feature = "sqlite")]
mod sqlite_suspended;
#[cfg(feature = "sqlite")]
mod sqlite_timeout;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod state_codec;
#[cfg(any(
    feature = "sqlite",
    feature = "mysql",
    feature = "postgres",
    feature = "mssql"
))]
mod state_machine_codec;
mod state_machine_store;
mod suspended_store;

pub use dsl_progress_store::SqlDslStepProgressStore;
pub use flow_store::{SqlFlowStore, SqlFlowStoreOptions};
pub use scheduler_store::SqlFlowScheduler;
pub use state_machine_store::SqlStateMachineStore;
pub use suspended_store::SqlSuspendedFlowStore;

/// Bounded SQL Server connection pool accepted by the SQL Server constructors.
#[cfg(feature = "mssql")]
pub type MssqlPool = bb8::Pool<bb8_tiberius::ConnectionManager>;

/// Re-exports Redis plain-state and suspended-flow stores when the `redis` feature is enabled.
#[cfg(feature = "redis")]
pub use catga_redis::{RedisFlows, RedisSuspendedFlows};
