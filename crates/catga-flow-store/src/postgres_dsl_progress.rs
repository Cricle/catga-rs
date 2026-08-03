//! PostgreSQL schema and shared operations for durable DSL step progress.

use sqlx::PgPool;

use crate::server_dsl_progress::define_server_dsl_progress;

/// Creates the PostgreSQL DSL step-progress table.
pub(crate) async fn migrate(pool: &PgPool) -> catga_core::CatgaResult<()> {
    crate::postgres_schema::migrate(
        pool,
        "create PostgreSQL DSL step-progress table",
        ["CREATE TABLE IF NOT EXISTS catga_dsl_step_progress (\
           flow_key BYTEA NOT NULL, flow_id TEXT NOT NULL, step_index BIGINT NOT NULL, \
           version BIGINT NOT NULL, revision BIGINT NOT NULL, payload BYTEA NOT NULL, \
           PRIMARY KEY(flow_key, step_index))"],
    )
    .await
}

define_server_dsl_progress!(PgPool, true, "PostgreSQL");
