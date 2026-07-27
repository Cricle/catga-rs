//! PostgreSQL durable Flow-resume scheduling.

use crate::server_scheduler::define_server_scheduler;

define_server_scheduler!(
    sqlx::PgPool,
    sqlx::postgres::PgRow,
    true,
    "PostgreSQL",
    "CREATE TABLE IF NOT EXISTS catga_flow_schedules (\
       schedule_id TEXT PRIMARY KEY NOT NULL, target_key BYTEA NOT NULL UNIQUE, \
       flow_id TEXT NOT NULL, state_id TEXT NOT NULL, due_at_ms BIGINT NOT NULL, \
       due_at_subsec_ns BIGINT NOT NULL, lease_owner TEXT NULL, lease_until_ms BIGINT NULL)",
    "CREATE INDEX IF NOT EXISTS catga_flow_schedules_due_idx \
       ON catga_flow_schedules(due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id)"
);
