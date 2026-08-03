//! MySQL 8 schema and shared operations for durable state-machine snapshots.

use sqlx::MySqlPool;

use crate::server_state_machine::define_server_state_machine;

/// Creates the MySQL state-machine snapshot table.
pub(crate) async fn migrate(pool: &MySqlPool) -> catga_core::CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_state_machine_snapshots (\
         instance_key BINARY(32) PRIMARY KEY NOT NULL, instance_id LONGTEXT NOT NULL, \
         version BIGINT NOT NULL, revision BIGINT NOT NULL, payload LONGBLOB NOT NULL) \
         ENGINE=InnoDB",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| {
        crate::error::database_error("create MySQL state-machine snapshot table", error)
    })
}

define_server_state_machine!(MySqlPool, false, "MySQL");
