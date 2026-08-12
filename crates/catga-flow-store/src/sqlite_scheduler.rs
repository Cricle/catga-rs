//! SQLite durable Flow-resume scheduling.

use crate::server_scheduler::define_server_scheduler;

/// Applies the scheduler table and due-index DDL inside one SQLite write transaction.
async fn migrate_schema(
    pool: &sqlx::SqlitePool,
    schema: &'static str,
    index: &'static str,
) -> catga_core::CatgaResult<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| crate::error::database_error("begin SQLite scheduler migration", error))?;
    sqlx::query(schema)
        .execute(&mut *tx)
        .await
        .map_err(|error| crate::error::database_error("create SQLite scheduler table", error))?;
    sqlx::query(index)
        .execute(&mut *tx)
        .await
        .map_err(|error| {
            crate::error::database_error("create SQLite scheduler due index", error)
        })?;
    tx.commit()
        .await
        .map_err(|error| crate::error::database_error("commit SQLite scheduler migration", error))
}

define_server_scheduler!(
    sqlx::SqlitePool,
    sqlx::sqlite::SqliteRow,
    false,
    true,
    "SQLite",
    "CREATE TABLE IF NOT EXISTS catga_flow_schedules (\
       schedule_id TEXT PRIMARY KEY NOT NULL, target_key BLOB NOT NULL UNIQUE, \
       flow_id TEXT NOT NULL, state_id TEXT NOT NULL, due_at_ms INTEGER NOT NULL, \
       due_at_subsec_ns INTEGER NOT NULL, lease_owner TEXT NULL, lease_until_ms INTEGER NULL)",
    "CREATE INDEX IF NOT EXISTS catga_flow_schedules_due_idx \
       ON catga_flow_schedules(due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id)",
    migrate_schema
);
