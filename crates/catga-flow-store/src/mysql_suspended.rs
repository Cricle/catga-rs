//! MySQL 8 continuation schema and shared mutation implementation.

use crate::server_suspended::define_server_suspended;
use sqlx::MySqlPool;

/// Creates the MySQL continuation table with discovery and timeout indexes.
pub(crate) async fn migrate(pool: &MySqlPool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (\
         flow_key BINARY(32) PRIMARY KEY NOT NULL, flow_id LONGTEXT NOT NULL,\
         flow_type LONGTEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL,\
         created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0,\
         deadline_ms BIGINT NULL, revision BIGINT NOT NULL,\
         due_token BINARY(16) NULL, lease_until_ms BIGINT NULL, payload LONGBLOB NOT NULL,\
         INDEX catga_flow_continuations_query_idx(status, created_at_ms, flow_key),\
         INDEX catga_flow_continuations_order_idx(created_at_ms, created_at_subsec_ns, flow_key),\
         INDEX catga_flow_continuations_due_idx(deadline_ms, lease_until_ms, flow_key)) ENGINE=InnoDB",
    ).execute(pool).await.map_err(|error| crate::error::database_error("create MySQL continuation table", error))?;
    sqlx::query(
        "ALTER TABLE catga_flow_continuations \
         DROP INDEX IF EXISTS catga_flow_continuations_query_idx, \
         DROP INDEX IF EXISTS flow_id, \
         MODIFY COLUMN flow_id LONGTEXT NOT NULL, \
         MODIFY COLUMN flow_type LONGTEXT NOT NULL, \
         ADD INDEX catga_flow_continuations_query_idx(status, created_at_ms, flow_key)",
    )
    .execute(pool)
    .await
    .map_err(|error| crate::error::database_error("widen MySQL continuation identities", error))?;
    sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS created_at_subsec_ns BIGINT NOT NULL DEFAULT 0")
        .execute(pool).await.map_err(|error| crate::error::database_error("add MySQL continuation precision column", error))?;
    sqlx::query("CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)")
        .execute(pool).await.map_err(|error| crate::error::database_error("create MySQL continuation order index", error))?;
    Ok(())
}

define_server_suspended!(MySqlPool, false, "MySQL");
