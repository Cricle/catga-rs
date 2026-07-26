//! MySQL 8 schema and shared operations for durable DSL step progress.

use sqlx::MySqlPool;

use crate::server_dsl_progress::define_server_dsl_progress;

/// Creates the MySQL DSL step-progress table.
pub(crate) async fn migrate(pool: &MySqlPool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_dsl_step_progress (\
         flow_key BINARY(32) NOT NULL, flow_id LONGTEXT NOT NULL, step_index BIGINT NOT NULL, \
         version BIGINT NOT NULL, revision BIGINT NOT NULL, payload LONGBLOB NOT NULL, \
         PRIMARY KEY(flow_key, step_index)) ENGINE=InnoDB",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| crate::error::database_error("create MySQL DSL step-progress table", error))
}

define_server_dsl_progress!(MySqlPool, false, "MySQL");
