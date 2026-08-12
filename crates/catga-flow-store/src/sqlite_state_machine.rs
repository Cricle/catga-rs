//! SQLite statements for durable state-machine snapshots.

use crate::server_state_machine::define_server_state_machine;

/// Creates the SQLite state-machine snapshot table.
pub(crate) async fn migrate(pool: &sqlx::SqlitePool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_state_machine_snapshots (\
         instance_key BLOB PRIMARY KEY NOT NULL, instance_id TEXT NOT NULL, \
         version INTEGER NOT NULL, revision INTEGER NOT NULL, payload BLOB NOT NULL)",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| {
        crate::error::database_error("create SQLite state-machine snapshot table", error)
    })
}

define_server_state_machine!(sqlx::SqlitePool, false, true, "SQLite");
