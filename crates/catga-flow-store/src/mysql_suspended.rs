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
         updated_at_ms BIGINT NOT NULL DEFAULT 0, updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0,\
         deadline_ms BIGINT NULL, wait_correlation LONGTEXT NULL, \
         wait_correlation_key BINARY(32) NULL, revision BIGINT NOT NULL,\
         due_token BINARY(16) NULL, lease_until_ms BIGINT NULL, payload LONGBLOB NOT NULL,\
         INDEX catga_flow_continuations_query_idx(status, created_at_ms, flow_key),\
         INDEX catga_flow_continuations_order_idx(created_at_ms, created_at_subsec_ns, flow_key),\
         INDEX catga_flow_continuations_due_idx(deadline_ms, lease_until_ms, flow_key),\
         INDEX catga_flow_continuations_wait_correlation_idx(wait_correlation_key, flow_key)) ENGINE=InnoDB",
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
    sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_ms BIGINT NOT NULL DEFAULT 0")
        .execute(pool).await.map_err(|error| crate::error::database_error("add MySQL continuation update column", error))?;
    sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0")
        .execute(pool).await.map_err(|error| crate::error::database_error("add MySQL continuation update precision column", error))?;
    sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation LONGTEXT NULL")
        .execute(pool).await.map_err(|error| crate::error::database_error("add MySQL continuation wait correlation column", error))?;
    sqlx::query(
        "ALTER TABLE catga_flow_continuations MODIFY COLUMN wait_correlation LONGTEXT NULL",
    )
    .execute(pool)
    .await
    .map_err(|error| {
        crate::error::database_error("widen MySQL continuation wait correlation", error)
    })?;
    sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN IF NOT EXISTS wait_correlation_key BINARY(32) NULL")
        .execute(pool).await.map_err(|error| crate::error::database_error("add MySQL continuation wait correlation key", error))?;
    sqlx::query("UPDATE catga_flow_continuations SET updated_at_ms = created_at_ms, updated_at_subsec_ns = created_at_subsec_ns WHERE updated_at_ms = 0 AND updated_at_subsec_ns = 0")
        .execute(pool).await.map_err(|error| crate::error::database_error("backfill MySQL continuation update time", error))?;
    sqlx::query("CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)")
        .execute(pool).await.map_err(|error| crate::error::database_error("create MySQL continuation order index", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx \
         ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)",
    )
    .execute(pool)
    .await
    .map_err(|error| crate::error::database_error("create MySQL continuation due index", error))?;
    sqlx::query("DROP INDEX IF EXISTS catga_flow_continuations_wait_correlation_idx ON catga_flow_continuations")
    .execute(pool)
    .await
    .map_err(|error| crate::error::database_error("replace MySQL continuation wait correlation index", error))?;
    sqlx::query(
        "CREATE INDEX catga_flow_continuations_wait_correlation_idx \
         ON catga_flow_continuations(wait_correlation_key, flow_key)",
    )
    .execute(pool)
    .await
    .map_err(|error| {
        crate::error::database_error("create MySQL continuation wait correlation index", error)
    })?;
    Ok(())
}

define_server_suspended!(MySqlPool, false, "MySQL");
