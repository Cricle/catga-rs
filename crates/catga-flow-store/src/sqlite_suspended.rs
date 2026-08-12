//! SQLite statements for durable Flow continuations.

use crate::server_suspended::define_server_suspended;

/// Creates the continuation table and its bounded-discovery indexes.
pub(crate) async fn migrate(pool: &sqlx::SqlitePool) -> catga_core::CatgaResult<()> {
    let mut transaction = pool.begin().await.map_err(|error| {
        crate::error::database_error("begin SQLite continuation migration", error)
    })?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_flow_continuations (\
             flow_key BLOB PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, \
             flow_type TEXT NOT NULL, status INTEGER NOT NULL, version INTEGER NOT NULL, \
             created_at_ms INTEGER NOT NULL, created_at_subsec_ns INTEGER NOT NULL DEFAULT 0, \
             updated_at_ms INTEGER NOT NULL DEFAULT 0, updated_at_subsec_ns INTEGER NOT NULL DEFAULT 0, \
             deadline_ms INTEGER NULL, wait_correlation TEXT NULL, revision INTEGER NOT NULL, \
             due_token BLOB NULL, lease_until_ms INTEGER NULL, payload BLOB NOT NULL)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| crate::error::database_error("create SQLite continuation table", error))?;
    let has_subsec_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') \
         WHERE name = ?",
    )
    .bind("created_at_subsec_ns")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("inspect SQLite continuation precision column", error)
    })?;
    if has_subsec_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations \
             ADD COLUMN created_at_subsec_ns INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            crate::error::database_error("add SQLite continuation precision column", error)
        })?;
    }
    let has_updated_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') WHERE name = ?",
    )
    .bind("updated_at_ms")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("inspect SQLite continuation update column", error)
    })?;
    if has_updated_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            crate::error::database_error("add SQLite continuation update column", error)
        })?;
    }
    let has_updated_subsec_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') \
         WHERE name = ?",
    )
    .bind("updated_at_subsec_ns")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("inspect SQLite continuation update precision column", error)
    })?;
    if has_updated_subsec_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations ADD COLUMN updated_at_subsec_ns INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            crate::error::database_error("add SQLite continuation update precision column", error)
        })?;
    }
    if has_updated_column == 0 {
        sqlx::query(
            "UPDATE catga_flow_continuations SET updated_at_ms = created_at_ms, \
             updated_at_subsec_ns = created_at_subsec_ns",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            crate::error::database_error("backfill SQLite continuation update time", error)
        })?;
    }
    let has_wait_correlation_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') WHERE name = ?",
    )
    .bind("wait_correlation")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("inspect SQLite wait correlation column", error)
    })?;
    if has_wait_correlation_column == 0 {
        sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN wait_correlation TEXT NULL")
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                crate::error::database_error("add SQLite wait correlation column", error)
            })?;
    }
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_query_idx \
         ON catga_flow_continuations(status, flow_type, created_at_ms, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("create SQLite continuation query index", error)
    })?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx \
         ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        crate::error::database_error("create SQLite continuation order index", error)
    })?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx \
         ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| crate::error::database_error("create SQLite continuation due index", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_wait_correlation_idx \
         ON catga_flow_continuations(wait_correlation, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| crate::error::database_error("create SQLite wait correlation index", error))?;
    transaction.commit().await.map_err(|error| {
        crate::error::database_error("commit SQLite continuation migration", error)
    })
}

define_server_suspended!(sqlx::SqlitePool, false, true, "SQLite");
