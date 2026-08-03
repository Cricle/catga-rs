//! SQLite statements for durable DSL step progress.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::DslStepProgress;
use sqlx::{Row, SqlitePool};

use crate::{
    dsl_progress_codec::{advances_version, decode_progress, encode_progress, validate_progress},
    error::database_error,
    key::flow_key,
};

const MAX_CAS_RETRIES: usize = 8;

struct StoredProgress {
    progress: DslStepProgress,
    revision: i64,
}

/// Creates the SQLite step-progress table.
pub(crate) async fn migrate(pool: &SqlitePool) -> CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_dsl_step_progress (\
         flow_key BLOB NOT NULL, flow_id TEXT NOT NULL, step_index INTEGER NOT NULL, \
         version INTEGER NOT NULL, revision INTEGER NOT NULL, payload BLOB NOT NULL, \
         PRIMARY KEY(flow_key, step_index))",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| database_error("create SQLite DSL step-progress table", error))
}

/// Inserts progress without overwriting the existing flow-step identity.
pub(crate) async fn create(pool: &SqlitePool, progress: DslStepProgress) -> CatgaResult<bool> {
    validate_progress(&progress)?;
    let key = flow_key(progress.flow_id());
    let step_index = i64::from(progress.step_index());
    let result = sqlx::query(
        "INSERT INTO catga_dsl_step_progress \
         (flow_key, flow_id, step_index, version, revision, payload) \
         VALUES (?, ?, ?, ?, 0, ?) ON CONFLICT(flow_key, step_index) DO NOTHING",
    )
    .bind(key.as_slice())
    .bind(progress.flow_id())
    .bind(step_index)
    .bind(progress.version())
    .bind(encode_progress(&progress)?)
    .execute(pool)
    .await
    .map_err(|error| database_error("create SQLite DSL step progress", error))?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    conflict_result(pool, &key, step_index, progress.flow_id()).await
}

/// Loads progress for one raw flow identity and step index.
pub(crate) async fn get(
    pool: &SqlitePool,
    flow_id: &str,
    step_index: u32,
) -> CatgaResult<Option<DslStepProgress>> {
    load(pool, flow_id, step_index)
        .await
        .map(|stored| stored.map(|stored| stored.progress))
}

/// Replaces exactly one logical version using a bounded physical-revision CAS loop.
pub(crate) async fn update(
    pool: &SqlitePool,
    expected_version: i64,
    next: DslStepProgress,
) -> CatgaResult<bool> {
    validate_progress(&next)?;
    if !advances_version(expected_version, next.version()) {
        return Ok(false);
    }
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, next.flow_id(), next.step_index()).await? else {
            return Ok(false);
        };
        if current.progress.version() != expected_version {
            return Ok(false);
        }
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("update SQLite DSL step progress"))
}

/// Deletes one flow-step row through its physical revision guard.
pub(crate) async fn delete(pool: &SqlitePool, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id, step_index).await? else {
            return Ok(false);
        };
        let key = flow_key(flow_id);
        let result = sqlx::query(
            "DELETE FROM catga_dsl_step_progress \
             WHERE flow_key = ? AND flow_id = ? AND step_index = ? AND revision = ?",
        )
        .bind(key.as_slice())
        .bind(flow_id)
        .bind(i64::from(step_index))
        .bind(current.revision)
        .execute(pool)
        .await
        .map_err(|error| database_error("delete SQLite DSL step progress", error))?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
    }
    Err(cas_error("delete SQLite DSL step progress"))
}

async fn load(
    pool: &SqlitePool,
    flow_id: &str,
    step_index: u32,
) -> CatgaResult<Option<StoredProgress>> {
    let key = flow_key(flow_id);
    let row = sqlx::query(
        "SELECT version, revision, payload FROM catga_dsl_step_progress \
         WHERE flow_key = ? AND flow_id = ? AND step_index = ?",
    )
    .bind(key.as_slice())
    .bind(flow_id)
    .bind(i64::from(step_index))
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read SQLite DSL step progress", error))?;
    row.map(|row| decode_row(row, flow_id, step_index, "SQLite"))
        .transpose()
}

async fn replace(
    pool: &SqlitePool,
    current: &StoredProgress,
    next: &DslStepProgress,
) -> CatgaResult<bool> {
    let key = flow_key(next.flow_id());
    let result = sqlx::query(
        "UPDATE catga_dsl_step_progress SET version = ?, payload = ?, revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND step_index = ? AND revision = ?",
    )
    .bind(next.version())
    .bind(encode_progress(next)?)
    .bind(key.as_slice())
    .bind(next.flow_id())
    .bind(i64::from(next.step_index()))
    .bind(current.revision)
    .execute(pool)
    .await
    .map_err(|error| database_error("replace SQLite DSL step progress", error))?;
    Ok(result.rows_affected() == 1)
}

async fn conflict_result(
    pool: &SqlitePool,
    key: &[u8; 32],
    step_index: i64,
    flow_id: &str,
) -> CatgaResult<bool> {
    let row = sqlx::query(
        "SELECT flow_id FROM catga_dsl_step_progress WHERE flow_key = ? AND step_index = ?",
    )
    .bind(key.as_slice())
    .bind(step_index)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read conflicting SQLite DSL step progress", error))?
    .ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Transient,
            "SQLite DSL step progress disappeared after a conflicting create",
        )
    })?;
    let existing: String = row
        .try_get("flow_id")
        .map_err(|error| database_error("decode SQLite DSL progress identity", error))?;
    if existing == flow_id {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL DSL progress identities",
        ))
    }
}

fn decode_row(
    row: sqlx::sqlite::SqliteRow,
    flow_id: &str,
    step_index: u32,
    backend: &str,
) -> CatgaResult<StoredProgress> {
    let version: i64 = row
        .try_get("version")
        .map_err(|error| database_error("decode SQL DSL progress version", error))?;
    let revision: i64 = row
        .try_get("revision")
        .map_err(|error| database_error("decode SQL DSL progress revision", error))?;
    let frame: Vec<u8> = row
        .try_get("payload")
        .map_err(|error| database_error("decode SQL DSL progress frame", error))?;
    let progress = decode_progress(&frame)?;
    if progress.flow_id() != flow_id
        || progress.step_index() != step_index
        || progress.version() != version
    {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            format!("{backend} DSL step-progress row does not match its frame"),
        ));
    }
    Ok(StoredProgress { progress, revision })
}

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("SQL DSL step-progress store could not {operation} after bounded retries"),
    )
}
