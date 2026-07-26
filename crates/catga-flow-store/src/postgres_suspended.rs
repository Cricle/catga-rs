//! PostgreSQL continuation schema and shared mutation implementation.

use crate::server_suspended::define_server_suspended;
use sqlx::PgPool;

/// Creates the PostgreSQL continuation table and bounded discovery indexes.
pub(crate) async fn migrate(pool: &PgPool) -> catga_core::CatgaResult<()> {
    let mut tx = pool.begin().await.map_err(|error| {
        crate::error::database_error("begin PostgreSQL continuation migration", error)
    })?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, flow_type TEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0, deadline_ms BIGINT NULL, revision BIGINT NOT NULL, due_token BYTEA NULL, lease_until_ms BIGINT NULL, payload BYTEA NOT NULL)",
        "ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS created_at_subsec_ns BIGINT NOT NULL DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_query_idx ON catga_flow_continuations(status, flow_type, created_at_ms, flow_key)",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)",
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)",
    ] {
        sqlx::query(crate::sql_backend::statement(sql, true))
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                crate::error::database_error("create PostgreSQL continuation schema", error)
            })?;
    }
    tx.commit().await.map_err(|error| {
        crate::error::database_error("commit PostgreSQL continuation migration", error)
    })
}

define_server_suspended!(PgPool, true, "PostgreSQL");
