//! PostgreSQL schema-migration serialization shared by every durable store.

use catga_core::CatgaResult;
use sqlx::{PgPool, Postgres, Transaction};

use crate::{error::database_error, sql_backend::statement};

/// Stable namespace for transaction-scoped PostgreSQL FlowStore migration serialization.
///
/// `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` still race during first-time
/// PostgreSQL catalog creation. All FlowStore schema migrations therefore share one advisory
/// transaction lock and execute their DDL atomically.
const MIGRATION_ADVISORY_LOCK: i64 = 4_928_346_905_119_623_496;

/// Starts a transaction after acquiring the process-independent migration lock.
pub(crate) async fn begin<'pool>(
    pool: &'pool PgPool,
    operation: &'static str,
) -> CatgaResult<Transaction<'pool, Postgres>> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| database_error(operation, error))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_ADVISORY_LOCK)
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error(operation, error))?;
    Ok(transaction)
}

/// Executes simple idempotent DDL under the shared migration transaction.
pub(crate) async fn migrate<const N: usize>(
    pool: &PgPool,
    operation: &'static str,
    statements: [&'static str; N],
) -> CatgaResult<()> {
    let mut transaction = begin(pool, operation).await?;
    for sql in statements {
        sqlx::query(statement(sql, true))
            .execute(&mut *transaction)
            .await
            .map_err(|error| database_error(operation, error))?;
    }
    transaction
        .commit()
        .await
        .map_err(|error| database_error(operation, error))
}
