//! Shared SQLx server-dialect durable schedule operations.

macro_rules! define_server_scheduler {
    ($pool:ty, $row:ty, $postgres:expr, $label:literal, $schema:expr, $index:expr) => {
        use std::time::{Duration, SystemTime};

        use catga_core::{CatgaError, CatgaResult, ErrorCode};
        use catga_flow::ScheduledResume;
        use sqlx::Row;

        use crate::{
            error::database_error,
            key::schedule_target_key,
            scheduler_common::{claim_times, current_millis, schedule_times},
            sql_backend::statement,
            sql_common::system_time_from_unix_millis_and_subsec_nanos,
        };

        pub(crate) async fn migrate(pool: &$pool) -> CatgaResult<()> {
            sqlx::query(statement($schema, $postgres))
                .execute(pool)
                .await
                .map_err(|error| database_error(concat!("create ", $label, " scheduler table"), error))?;
            if !$index.is_empty() {
                sqlx::query(statement($index, $postgres)).execute(pool).await
                    .map_err(|error| database_error(concat!("create ", $label, " scheduler due index"), error))?;
            }
            Ok(())
        }

        pub(crate) async fn schedule_resume(
            pool: &$pool,
            flow_id: &str,
            state_id: &str,
            due_at: SystemTime,
        ) -> CatgaResult<Box<str>> {
            let target_key = schedule_target_key(flow_id, state_id);
            let (due_at_ms, due_at_subsec_ns) = schedule_times(due_at)?;
            let schedule_id = uuid::Uuid::new_v4().to_string();
            let insert = if $postgres {
                "INSERT INTO catga_flow_schedules (schedule_id, target_key, flow_id, state_id, due_at_ms, due_at_subsec_ns) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(target_key) DO NOTHING"
            } else {
                "INSERT INTO catga_flow_schedules (schedule_id, target_key, flow_id, state_id, due_at_ms, due_at_subsec_ns) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE schedule_id = schedule_id"
            };
            sqlx::query(statement(insert, $postgres))
                .bind(&schedule_id).bind(target_key.as_slice()).bind(flow_id).bind(state_id)
                .bind(due_at_ms).bind(due_at_subsec_ns).execute(pool).await
                .map_err(|error| database_error(concat!("schedule ", $label, " flow resume"), error))?;
            existing_schedule(pool, target_key.as_slice(), flow_id, state_id).await
        }

        pub(crate) async fn cancel_resume(pool: &$pool, schedule_id: &str) -> CatgaResult<bool> {
            let now = current_millis()?;
            sqlx::query(statement(
                "DELETE FROM catga_flow_schedules WHERE schedule_id = ? AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?)", $postgres))
                .bind(schedule_id).bind(now).execute(pool).await.map(|result| result.rows_affected() == 1)
                .map_err(|error| database_error(concat!("cancel ", $label, " flow resume"), error))
        }

        pub(crate) async fn claim_due(
            pool: &$pool, owner: &str, now: SystemTime, lease_for: Duration, limit: usize,
        ) -> CatgaResult<Vec<ScheduledResume>> {
            if limit == 0 { return Ok(Vec::new()); }
            let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
            let (_, now_subsec_ns) = schedule_times(now)?;
            let limit = i64::try_from(limit).map_err(|_| CatgaError::new(ErrorCode::Validation, concat!($label, " schedule claim limit exceeds i64")))?;
            let mut tx = pool.begin().await.map_err(|error| database_error(concat!("begin ", $label, " schedule claim"), error))?;
            let rows = sqlx::query(statement(
                "SELECT schedule_id, flow_id, state_id, due_at_ms, due_at_subsec_ns FROM catga_flow_schedules \
                 WHERE (due_at_ms < ? OR (due_at_ms = ? AND due_at_subsec_ns <= ?)) \
                   AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) \
                 ORDER BY due_at_ms ASC, due_at_subsec_ns ASC, schedule_id ASC LIMIT ? FOR UPDATE SKIP LOCKED", $postgres))
                .bind(now_ms).bind(now_ms).bind(now_subsec_ns).bind(now_ms).bind(limit)
                .fetch_all(&mut *tx).await.map_err(|error| database_error(concat!("select ", $label, " due flow resumes"), error))?;
            let mut claimed = Vec::with_capacity(rows.len());
            for row in rows {
                let schedule_id: String = row.try_get("schedule_id").map_err(|error| database_error(concat!("decode ", $label, " schedule identity"), error))?;
                let changed = sqlx::query(statement(
                    "UPDATE catga_flow_schedules SET lease_owner = ?, lease_until_ms = ? \
                     WHERE schedule_id = ? AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?)", $postgres))
                    .bind(owner).bind(lease_until_ms).bind(&schedule_id).bind(now_ms).execute(&mut *tx).await
                    .map_err(|error| database_error(concat!("lease ", $label, " due flow resume"), error))?;
                if changed.rows_affected() != 1 {
                    return Err(CatgaError::new(ErrorCode::Transient, concat!($label, " selected schedule lost its claim eligibility")));
                }
                claimed.push(decode_resume(row)?);
            }
            tx.commit().await.map_err(|error| database_error(concat!("commit ", $label, " schedule claim"), error))?;
            Ok(claimed)
        }

        pub(crate) async fn ack_due(pool: &$pool, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
            sqlx::query(statement("DELETE FROM catga_flow_schedules WHERE schedule_id = ? AND lease_owner = ?", $postgres))
                .bind(schedule_id).bind(owner).execute(pool).await.map(|result| result.rows_affected() == 1)
                .map_err(|error| database_error(concat!("acknowledge ", $label, " due flow resume"), error))
        }

        pub(crate) async fn release_due(pool: &$pool, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
            sqlx::query(statement(
                "UPDATE catga_flow_schedules SET lease_owner = NULL, lease_until_ms = NULL WHERE schedule_id = ? AND lease_owner = ?", $postgres))
                .bind(schedule_id).bind(owner).execute(pool).await.map(|result| result.rows_affected() == 1)
                .map_err(|error| database_error(concat!("release ", $label, " due flow resume"), error))
        }

        pub(crate) async fn renew_due(
            pool: &$pool, owner: &str, schedule_id: &str, now: SystemTime, lease_for: Duration,
        ) -> CatgaResult<bool> {
            let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
            sqlx::query(statement(
                "UPDATE catga_flow_schedules SET lease_until_ms = ? \
                 WHERE schedule_id = ? AND lease_owner = ? AND lease_until_ms > ?", $postgres))
                .bind(lease_until_ms).bind(schedule_id).bind(owner).bind(now_ms).execute(pool).await
                .map(|result| result.rows_affected() == 1)
                .map_err(|error| database_error(concat!("renew ", $label, " due flow resume"), error))
        }

        async fn existing_schedule(pool: &$pool, target_key: &[u8], flow_id: &str, state_id: &str) -> CatgaResult<Box<str>> {
            let row = sqlx::query(statement(
                "SELECT schedule_id, flow_id, state_id FROM catga_flow_schedules WHERE target_key = ?", $postgres))
                .bind(target_key).fetch_optional(pool).await
                .map_err(|error| database_error(concat!("read ", $label, " scheduled flow resume"), error))?
                .ok_or_else(|| CatgaError::new(ErrorCode::Transient, concat!($label, " scheduled flow resume disappeared after creation conflict")))?;
            let existing_flow_id: String = row.try_get("flow_id").map_err(|error| database_error(concat!("decode ", $label, " schedule flow identity"), error))?;
            let existing_state_id: String = row.try_get("state_id").map_err(|error| database_error(concat!("decode ", $label, " schedule state identity"), error))?;
            if existing_flow_id != flow_id || existing_state_id != state_id {
                return Err(CatgaError::new(ErrorCode::Internal, "SHA-256 collision between SQL schedule targets"));
            }
            row.try_get::<String, _>("schedule_id").map(Into::into)
                .map_err(|error| database_error(concat!("decode ", $label, " schedule identity"), error))
        }

        fn decode_resume(row: $row) -> CatgaResult<ScheduledResume> {
            let due_at_ms: i64 = row.try_get("due_at_ms").map_err(|error| database_error(concat!("decode ", $label, " schedule due milliseconds"), error))?;
            let due_at_subsec_ns: i64 = row.try_get("due_at_subsec_ns").map_err(|error| database_error(concat!("decode ", $label, " schedule due precision"), error))?;
            Ok(ScheduledResume::new(
                row.try_get::<String, _>("schedule_id").map_err(|error| database_error(concat!("decode ", $label, " schedule identity"), error))?,
                row.try_get::<String, _>("flow_id").map_err(|error| database_error(concat!("decode ", $label, " schedule flow identity"), error))?,
                row.try_get::<String, _>("state_id").map_err(|error| database_error(concat!("decode ", $label, " schedule state identity"), error))?,
                system_time_from_unix_millis_and_subsec_nanos(due_at_ms, due_at_subsec_ns)?,
            ))
        }
    };
}

pub(crate) use define_server_scheduler;
