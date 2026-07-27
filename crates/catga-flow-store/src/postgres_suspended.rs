//! PostgreSQL continuation schema and shared mutation implementation.

use crate::{
    key::flow_key as continuation_flow_type_key, server_suspended::define_server_suspended,
};
use sqlx::PgPool;

/// Stable PostgreSQL advisory-lock namespace for continuation schema migration.
///
/// PostgreSQL's `CREATE TABLE IF NOT EXISTS` still permits a concurrent initial DDL race, so
/// every continuation-store migrator serializes that initial transaction with this key. The lock
/// is transaction-scoped and is released automatically by commit or rollback.
const CONTINUATION_MIGRATION_ADVISORY_LOCK: i64 = 4_928_346_905_119_623_496;

/// Creates the PostgreSQL continuation table and bounded discovery indexes.
pub(crate) async fn migrate(pool: &PgPool) -> catga_core::CatgaResult<()> {
    let mut tx = pool.begin().await.map_err(|error| {
        crate::error::database_error("begin PostgreSQL continuation migration", error)
    })?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(CONTINUATION_MIGRATION_ADVISORY_LOCK)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            crate::error::database_error("acquire PostgreSQL continuation migration lock", error)
        })?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, flow_type TEXT NOT NULL, flow_type_key BYTEA NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0, updated_at_ms BIGINT NOT NULL DEFAULT 0, updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0, deadline_ms BIGINT NULL, wait_correlation TEXT NULL, wait_correlation_key BYTEA NULL, revision BIGINT NOT NULL, due_token BYTEA NULL, lease_until_ms BIGINT NULL, payload BYTEA NOT NULL)",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS flow_type_key BYTEA NULL",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS created_at_subsec_ns BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation TEXT NULL",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation_key BYTEA NULL",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_wait_correlation_idx ON catga_flow_continuations(wait_correlation_key, flow_key)",
    ] {
        sqlx::query(crate::sql_backend::statement(sql, true))
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                crate::error::database_error("create PostgreSQL continuation schema", error)
            })?;
    }
    let rows = sqlx::query(crate::sql_backend::statement(
        "SELECT flow_key, flow_type FROM catga_flow_continuations WHERE flow_type_key IS NULL",
        true,
    ))
    .fetch_all(&mut *tx)
    .await
    .map_err(|error| {
        crate::error::database_error("read PostgreSQL continuation type-key backfill", error)
    })?;
    for row in rows {
        let row_key: Vec<u8> = row.try_get("flow_key").map_err(|error| {
            crate::error::database_error("decode PostgreSQL continuation type-key row", error)
        })?;
        let flow_type: String = row.try_get("flow_type").map_err(|error| {
            crate::error::database_error("decode PostgreSQL continuation type-key flow type", error)
        })?;
        let flow_type_key = continuation_flow_type_key(&flow_type);
        sqlx::query(crate::sql_backend::statement(
            "UPDATE catga_flow_continuations SET flow_type_key = ? \
             WHERE flow_key = ? AND flow_type_key IS NULL",
            true,
        ))
        .bind(flow_type_key.as_slice())
        .bind(row_key)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            crate::error::database_error("backfill PostgreSQL continuation type key", error)
        })?;
    }
    sqlx::query(crate::sql_backend::statement(
        "ALTER TABLE catga_flow_continuations ALTER COLUMN flow_type_key SET NOT NULL",
        true,
    ))
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        crate::error::database_error("require PostgreSQL continuation type keys", error)
    })?;
    for sql in [
        "DROP INDEX IF EXISTS catga_flow_continuations_query_idx",
        "CREATE INDEX catga_flow_continuations_query_idx ON catga_flow_continuations(status, created_at_ms, created_at_subsec_ns, flow_key)",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_type_query_idx ON catga_flow_continuations(flow_type_key, status, created_at_ms, created_at_subsec_ns, flow_key)",
    ] {
        sqlx::query(crate::sql_backend::statement(sql, true))
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                crate::error::database_error("create PostgreSQL continuation query indexes", error)
            })?;
    }
    sqlx::query(crate::sql_backend::statement("UPDATE catga_flow_continuations SET updated_at_ms = created_at_ms, updated_at_subsec_ns = created_at_subsec_ns WHERE updated_at_ms = 0 AND updated_at_subsec_ns = 0", true))
        .execute(&mut *tx)
        .await
        .map_err(|error| crate::error::database_error("backfill PostgreSQL continuation update time", error))?;
    tx.commit().await.map_err(|error| {
        crate::error::database_error("commit PostgreSQL continuation migration", error)
    })
}

define_server_suspended!(PgPool, true, "PostgreSQL");
