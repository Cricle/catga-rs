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
//!   [`catga_flow::DueFlowScheduler::claim_due`] on `SqlFlowScheduler`; the adapter owns no
//!   worker or timer.
//! - `redis` re-exports `RedisFlows` and `RedisSuspendedFlows` for the plain state and
//!   continuation contracts, plus Redis-backed timeout and scheduling support.
//! - `tls-rustls` enables Rustls support for whichever network SQL drivers are selected.
//!
//! SQL backends share versioned MemoryPack frames, fixed SHA-256 identity keys, bounded
//! optimistic-concurrency retries, bounded discovery scans, and receipt fencing. Dialect-specific
//! modules retain native parameter binding, skip-locked claims, and indexed time ordering without
//! duplicating the public store contract. No adapter creates a worker or background task.

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
#[cfg(any(feature = "mysql", feature = "postgres"))]
mod server_dsl_progress;
#[cfg(any(feature = "mysql", feature = "postgres"))]
mod server_scheduler;
#[cfg(any(feature = "mysql", feature = "postgres"))]
mod server_state_machine;
#[cfg(any(feature = "mysql", feature = "postgres"))]
mod server_suspended;
#[cfg(any(feature = "mysql", feature = "postgres"))]
mod server_timeout;
#[cfg(any(feature = "mysql", feature = "postgres"))]
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
pub use flow_store::SqlFlowStore;
pub use scheduler_store::SqlFlowScheduler;
pub use state_machine_store::SqlStateMachineStore;
pub use suspended_store::SqlSuspendedFlowStore;

/// Bounded SQL Server connection pool accepted by the SQL Server constructors.
#[cfg(feature = "mssql")]
pub type MssqlPool = bb8::Pool<bb8_tiberius::ConnectionManager>;

/// Re-exports Redis plain-state and suspended-flow stores when the `redis` feature is enabled.
#[cfg(feature = "redis")]
pub use catga_redis::{RedisFlows, RedisSuspendedFlows};
