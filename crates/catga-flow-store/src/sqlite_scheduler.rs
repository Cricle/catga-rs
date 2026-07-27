//! SQLite durable Flow-resume scheduling.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::ScheduledResume;
use sqlx::{Row, SqlitePool};

use crate::{
    error::database_error,
    key::schedule_target_key,
    scheduler_common::{claim_times, current_millis, schedule_times},
    sql_common::system_time_from_unix_millis_and_subsec_nanos,
};

pub(crate) async fn migrate(pool: &SqlitePool) -> CatgaResult<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin SQLite scheduler migration", error))?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_flow_schedules (\
           schedule_id TEXT PRIMARY KEY NOT NULL, target_key BLOB NOT NULL UNIQUE, \
           flow_id TEXT NOT NULL, state_id TEXT NOT NULL, due_at_ms INTEGER NOT NULL, \
           due_at_subsec_ns INTEGER NOT NULL, lease_owner TEXT NULL, lease_until_ms INTEGER NULL)",
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| database_error("create SQLite scheduler table", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_schedules_due_idx \
         ON catga_flow_schedules(due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id)",
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| database_error("create SQLite scheduler due index", error))?;
    tx.commit()
        .await
        .map_err(|error| database_error("commit SQLite scheduler migration", error))
}

pub(crate) async fn schedule_resume(
    pool: &SqlitePool,
    flow_id: &str,
    state_id: &str,
    due_at: SystemTime,
) -> CatgaResult<Box<str>> {
    let target_key = schedule_target_key(flow_id, state_id);
    let (due_at_ms, due_at_subsec_ns) = schedule_times(due_at)?;
    let schedule_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO catga_flow_schedules \
         (schedule_id, target_key, flow_id, state_id, due_at_ms, due_at_subsec_ns) \
         VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(target_key) DO NOTHING",
    )
    .bind(&schedule_id)
    .bind(target_key.as_slice())
    .bind(flow_id)
    .bind(state_id)
    .bind(due_at_ms)
    .bind(due_at_subsec_ns)
    .execute(pool)
    .await
    .map_err(|error| database_error("schedule SQLite flow resume", error))?;
    existing_schedule(pool, &target_key, flow_id, state_id).await
}

pub(crate) async fn cancel_resume(pool: &SqlitePool, schedule_id: &str) -> CatgaResult<bool> {
    let now = current_millis()?;
    sqlx::query(
        "DELETE FROM catga_flow_schedules WHERE schedule_id = ? \
         AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?)",
    )
    .bind(schedule_id)
    .bind(now)
    .execute(pool)
    .await
    .map(|result| result.rows_affected() == 1)
    .map_err(|error| database_error("cancel SQLite flow resume", error))
}

pub(crate) async fn claim_due(
    pool: &SqlitePool,
    owner: &str,
    now: SystemTime,
    lease_for: Duration,
    limit: usize,
) -> CatgaResult<Vec<ScheduledResume>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
    let (_, now_subsec_ns) = schedule_times(now)?;
    let limit = i64::try_from(limit).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQLite schedule claim limit exceeds i64",
        )
    })?;
    let rows = sqlx::query(
        "UPDATE catga_flow_schedules SET lease_owner = ?, lease_until_ms = ? \
         WHERE schedule_id IN (\
           SELECT schedule_id FROM catga_flow_schedules \
           WHERE (due_at_ms < ? OR (due_at_ms = ? AND due_at_subsec_ns <= ?)) \
             AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) \
           ORDER BY due_at_ms ASC, due_at_subsec_ns ASC, schedule_id ASC LIMIT ?) \
           AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) \
         RETURNING schedule_id, flow_id, state_id, due_at_ms, due_at_subsec_ns",
    )
    .bind(owner)
    .bind(lease_until_ms)
    .bind(now_ms)
    .bind(now_ms)
    .bind(now_subsec_ns)
    .bind(now_ms)
    .bind(limit)
    .bind(now_ms)
    .fetch_all(pool)
    .await
    .map_err(|error| database_error("claim SQLite due flow resumes", error))?;
    rows.into_iter().map(decode_resume).collect()
}

pub(crate) async fn ack_due(
    pool: &SqlitePool,
    owner: &str,
    schedule_id: &str,
) -> CatgaResult<bool> {
    sqlx::query("DELETE FROM catga_flow_schedules WHERE schedule_id = ? AND lease_owner = ?")
        .bind(schedule_id)
        .bind(owner)
        .execute(pool)
        .await
        .map(|result| result.rows_affected() == 1)
        .map_err(|error| database_error("acknowledge SQLite due flow resume", error))
}

pub(crate) async fn release_due(
    pool: &SqlitePool,
    owner: &str,
    schedule_id: &str,
) -> CatgaResult<bool> {
    sqlx::query(
        "UPDATE catga_flow_schedules SET lease_owner = NULL, lease_until_ms = NULL \
         WHERE schedule_id = ? AND lease_owner = ?",
    )
    .bind(schedule_id)
    .bind(owner)
    .execute(pool)
    .await
    .map(|result| result.rows_affected() == 1)
    .map_err(|error| database_error("release SQLite due flow resume", error))
}

pub(crate) async fn renew_due(
    pool: &SqlitePool,
    owner: &str,
    schedule_id: &str,
    now: SystemTime,
    lease_for: Duration,
) -> CatgaResult<bool> {
    let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
    sqlx::query(
        "UPDATE catga_flow_schedules SET lease_until_ms = ? \
         WHERE schedule_id = ? AND lease_owner = ? AND lease_until_ms > ?",
    )
    .bind(lease_until_ms)
    .bind(schedule_id)
    .bind(owner)
    .bind(now_ms)
    .execute(pool)
    .await
    .map(|result| result.rows_affected() == 1)
    .map_err(|error| database_error("renew SQLite due flow resume", error))
}

async fn existing_schedule(
    pool: &SqlitePool,
    target_key: &[u8],
    flow_id: &str,
    state_id: &str,
) -> CatgaResult<Box<str>> {
    let row = sqlx::query(
        "SELECT schedule_id, flow_id, state_id FROM catga_flow_schedules WHERE target_key = ?",
    )
    .bind(target_key)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read SQLite scheduled flow resume", error))?
    .ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Transient,
            "SQLite scheduled flow resume disappeared after creation conflict",
        )
    })?;
    let existing_flow_id: String = row
        .try_get("flow_id")
        .map_err(|error| database_error("decode SQLite schedule flow identity", error))?;
    let existing_state_id: String = row
        .try_get("state_id")
        .map_err(|error| database_error("decode SQLite schedule state identity", error))?;
    if existing_flow_id != flow_id || existing_state_id != state_id {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL schedule targets",
        ));
    }
    row.try_get::<String, _>("schedule_id")
        .map(Into::into)
        .map_err(|error| database_error("decode SQLite schedule identity", error))
}

fn decode_resume(row: sqlx::sqlite::SqliteRow) -> CatgaResult<ScheduledResume> {
    let due_at_ms: i64 = row
        .try_get("due_at_ms")
        .map_err(|error| database_error("decode SQLite schedule due milliseconds", error))?;
    let due_at_subsec_ns: i64 = row
        .try_get("due_at_subsec_ns")
        .map_err(|error| database_error("decode SQLite schedule due precision", error))?;
    Ok(ScheduledResume::new(
        row.try_get::<String, _>("schedule_id")
            .map_err(|error| database_error("decode SQLite schedule identity", error))?,
        row.try_get::<String, _>("flow_id")
            .map_err(|error| database_error("decode SQLite schedule flow identity", error))?,
        row.try_get::<String, _>("state_id")
            .map_err(|error| database_error("decode SQLite schedule state identity", error))?,
        system_time_from_unix_millis_and_subsec_nanos(due_at_ms, due_at_subsec_ns)?,
    ))
}
