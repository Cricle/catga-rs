//! PostgreSQL schema and shared operations for durable state-machine snapshots.

use sqlx::PgPool;

use crate::server_state_machine::define_server_state_machine;

/// Creates the PostgreSQL state-machine snapshot table.
pub(crate) async fn migrate(pool: &PgPool) -> catga_core::CatgaResult<()> {
    sqlx::query(crate::sql_backend::statement(
        "CREATE TABLE IF NOT EXISTS catga_state_machine_snapshots (\
         instance_key BYTEA PRIMARY KEY NOT NULL, instance_id TEXT NOT NULL, \
         version BIGINT NOT NULL, revision BIGINT NOT NULL, payload BYTEA NOT NULL)",
        true,
    ))
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| {
        crate::error::database_error("create PostgreSQL state-machine snapshot table", error)
    })
}

define_server_state_machine!(PgPool, true, "PostgreSQL");
