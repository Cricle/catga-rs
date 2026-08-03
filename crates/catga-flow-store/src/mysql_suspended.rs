//! MySQL 8 continuation schema and shared mutation implementation.

use crate::server_suspended::define_server_suspended;
use sqlx::MySqlPool;

/// Creates the current MySQL continuation table with discovery and timeout indexes.
///
/// Catga Rust has no historical schema compatibility requirement, so this migration deliberately
/// creates the current format without destructive legacy-column or index rewrites.
pub(crate) async fn migrate(pool: &MySqlPool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (\
         flow_key BINARY(32) PRIMARY KEY NOT NULL, flow_id LONGTEXT NOT NULL,\
         flow_type LONGTEXT NOT NULL, flow_type_key BINARY(32) NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL,\
         created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0,\
         updated_at_ms BIGINT NOT NULL DEFAULT 0, updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0,\
         deadline_ms BIGINT NULL, wait_correlation LONGTEXT NULL, \
         wait_correlation_key BINARY(32) NULL, revision BIGINT NOT NULL,\
         due_token BINARY(16) NULL, lease_until_ms BIGINT NULL, payload LONGBLOB NOT NULL,\
         INDEX catga_flow_continuations_query_idx(status, created_at_ms, created_at_subsec_ns, flow_key),\
         INDEX catga_flow_continuations_type_query_idx(flow_type_key, status, created_at_ms, created_at_subsec_ns, flow_key),\
         INDEX catga_flow_continuations_order_idx(created_at_ms, created_at_subsec_ns, flow_key),\
         INDEX catga_flow_continuations_due_idx(deadline_ms, lease_until_ms, flow_key),\
         INDEX catga_flow_continuations_wait_correlation_idx(wait_correlation_key, flow_key)) ENGINE=InnoDB",
    ).execute(pool).await.map_err(|error| crate::error::database_error("create MySQL continuation table", error))?;
    Ok(())
}

define_server_suspended!(MySqlPool, false, "MySQL");
