//! MySQL durable Flow-resume scheduling.

use crate::server_scheduler::define_server_scheduler;

define_server_scheduler!(
    sqlx::MySqlPool,
    sqlx::mysql::MySqlRow,
    false,
    "MySQL",
    "CREATE TABLE IF NOT EXISTS catga_flow_schedules (\
       schedule_id CHAR(36) PRIMARY KEY NOT NULL, target_key BINARY(32) NOT NULL UNIQUE, \
       flow_id LONGTEXT NOT NULL, state_id LONGTEXT NOT NULL, due_at_ms BIGINT NOT NULL, \
       due_at_subsec_ns BIGINT NOT NULL, lease_owner LONGTEXT NULL, lease_until_ms BIGINT NULL, \
       INDEX catga_flow_schedules_due_idx(due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id)) ENGINE=InnoDB",
    ""
);
