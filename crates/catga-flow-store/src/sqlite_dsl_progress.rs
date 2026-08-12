//! SQLite statements for durable DSL step progress.

use crate::server_dsl_progress::define_server_dsl_progress;

/// Creates the SQLite step-progress table.
pub(crate) async fn migrate(pool: &sqlx::SqlitePool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_dsl_step_progress (\
         flow_key BLOB NOT NULL, flow_id TEXT NOT NULL, step_index INTEGER NOT NULL, \
         version INTEGER NOT NULL, revision INTEGER NOT NULL, payload BLOB NOT NULL, \
         PRIMARY KEY(flow_key, step_index))",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| crate::error::database_error("create SQLite DSL step-progress table", error))
}

define_server_dsl_progress!(sqlx::SqlitePool, false, true, "SQLite");
