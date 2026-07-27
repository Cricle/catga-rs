//! PostgreSQL continuation schema and shared mutation implementation.

use crate::server_suspended::define_server_suspended;
use sqlx::PgPool;

/// Creates the PostgreSQL continuation table and bounded discovery indexes.
pub(crate) async fn migrate(pool: &PgPool) -> catga_core::CatgaResult<()> {
    let mut tx = pool.begin().await.map_err(|error| {
        crate::error::database_error("begin PostgreSQL continuation migration", error)
    })?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, flow_type TEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0, updated_at_ms BIGINT NOT NULL DEFAULT 0, updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0, deadline_ms BIGINT NULL, wait_correlation TEXT NULL, wait_correlation_key BYTEA NULL, revision BIGINT NOT NULL, due_token BYTEA NULL, lease_until_ms BIGINT NULL, payload BYTEA NOT NULL)",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS created_at_subsec_ns BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation TEXT NULL",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation_key BYTEA NULL",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_query_idx ON catga_flow_continuations(status, flow_type, created_at_ms, flow_key)",
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
    sqlx::query(crate::sql_backend::statement("UPDATE catga_flow_continuations SET updated_at_ms = created_at_ms, updated_at_subsec_ns = created_at_subsec_ns WHERE updated_at_ms = 0 AND updated_at_subsec_ns = 0", true))
        .execute(&mut *tx)
        .await
        .map_err(|error| crate::error::database_error("backfill PostgreSQL continuation update time", error))?;
    tx.commit().await.map_err(|error| {
        crate::error::database_error("commit PostgreSQL continuation migration", error)
    })
}

define_server_suspended!(PgPool, true, "PostgreSQL");
