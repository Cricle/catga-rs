//! SQLite statements for durable Flow continuations.

use std::time::SystemTime;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowStatus, FlowSummary, decode_continuation, encode_continuation,
    flow_timeout_deadline_unix_ms,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};

use crate::sql_common::{
    status_from_code, system_time_from_unix_millis_and_subsec_nanos, unix_millis_and_subsec_nanos,
};
use crate::{error::database_error, key::flow_key};

const MAX_CAS_RETRIES: usize = 8;

struct StoredContinuation {
    continuation: FlowContinuation,
    revision: i64,
}

/// Creates the continuation table and its bounded-discovery indexes.
pub(crate) async fn migrate(pool: &SqlitePool) -> CatgaResult<()> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| database_error("begin SQLite continuation migration", error))?;
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
    .map_err(|error| database_error("create SQLite continuation table", error))?;
    let has_subsec_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') \
         WHERE name = ?",
    )
    .bind("created_at_subsec_ns")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| database_error("inspect SQLite continuation precision column", error))?;
    if has_subsec_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations \
             ADD COLUMN created_at_subsec_ns INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error("add SQLite continuation precision column", error))?;
    }
    let has_updated_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') WHERE name = ?",
    )
    .bind("updated_at_ms")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| database_error("inspect SQLite continuation update column", error))?;
    if has_updated_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error("add SQLite continuation update column", error))?;
    }
    let has_updated_subsec_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') \
         WHERE name = ?",
    )
    .bind("updated_at_subsec_ns")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| {
        database_error("inspect SQLite continuation update precision column", error)
    })?;
    if has_updated_subsec_column == 0 {
        sqlx::query(
            "ALTER TABLE catga_flow_continuations ADD COLUMN updated_at_subsec_ns INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error("add SQLite continuation update precision column", error))?;
    }
    if has_updated_column == 0 {
        sqlx::query(
            "UPDATE catga_flow_continuations SET updated_at_ms = created_at_ms, \
             updated_at_subsec_ns = created_at_subsec_ns",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error("backfill SQLite continuation update time", error))?;
    }
    let has_wait_correlation_column: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pragma_table_info('catga_flow_continuations') WHERE name = ?",
    )
    .bind("wait_correlation")
    .fetch_one(&mut *transaction)
    .await
    .map_err(|error| database_error("inspect SQLite wait correlation column", error))?;
    if has_wait_correlation_column == 0 {
        sqlx::query("ALTER TABLE catga_flow_continuations ADD COLUMN wait_correlation TEXT NULL")
            .execute(&mut *transaction)
            .await
            .map_err(|error| database_error("add SQLite wait correlation column", error))?;
    }
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_query_idx \
         ON catga_flow_continuations(status, flow_type, created_at_ms, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite continuation query index", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_order_idx \
         ON catga_flow_continuations(created_at_ms, created_at_subsec_ns, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite continuation order index", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx \
         ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite continuation due index", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_wait_correlation_idx \
         ON catga_flow_continuations(wait_correlation, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite wait correlation index", error))?;
    transaction
        .commit()
        .await
        .map_err(|error| database_error("commit SQLite continuation migration", error))
}

/// Inserts an encoded continuation without replacing an existing identity.
pub(crate) async fn create(pool: &SqlitePool, continuation: FlowContinuation) -> CatgaResult<bool> {
    continuation.validate()?;
    let key = flow_key(continuation.state().id());
    let (created_at_ms, created_at_subsec_ns) =
        unix_millis_and_subsec_nanos(continuation.created_at())?;
    let (updated_at_ms, updated_at_subsec_ns) =
        unix_millis_and_subsec_nanos(continuation.updated_at())?;
    let result = sqlx::query(
        "INSERT INTO catga_flow_continuations \
         (flow_key, flow_id, flow_type, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns, deadline_ms, wait_correlation, revision, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?) ON CONFLICT(flow_key) DO NOTHING",
    )
    .bind(key.as_slice())
    .bind(continuation.state().id())
    .bind(continuation.state().flow_type())
    .bind(status_code(continuation.state().status()))
    .bind(continuation.state().version())
    .bind(created_at_ms)
    .bind(created_at_subsec_ns)
    .bind(updated_at_ms)
    .bind(updated_at_subsec_ns)
    .bind(deadline_millis(&continuation)?)
    .bind(wait_correlation(&continuation))
    .bind(encode_continuation(&continuation)?)
    .execute(pool)
    .await
    .map_err(|error| database_error("create SQLite continuation", error))?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    let existing = sqlx::query("SELECT flow_id FROM catga_flow_continuations WHERE flow_key = ?")
        .bind(key.as_slice())
        .fetch_optional(pool)
        .await
        .map_err(|error| database_error("read conflicting SQLite continuation", error))?;
    let Some(existing) = existing else {
        return Err(CatgaError::new(
            ErrorCode::Transient,
            "SQLite continuation disappeared after a conflicting create",
        ));
    };
    let existing_id: String = existing
        .try_get("flow_id")
        .map_err(|error| database_error("decode SQLite continuation identity", error))?;
    if existing_id == continuation.state().id() {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL continuation identities",
        ))
    }
}

/// Loads one continuation by its original identity.
pub(crate) async fn get(pool: &SqlitePool, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
    load(pool, flow_id)
        .await
        .map(|value| value.map(|value| value.continuation))
}

/// Loads exactly one continuation by its indexed active wait correlation.
pub(crate) async fn get_by_wait_correlation(
    pool: &SqlitePool,
    correlation_id: &str,
) -> CatgaResult<Option<FlowContinuation>> {
    let rows = sqlx::query(
        "SELECT payload FROM catga_flow_continuations \
         WHERE wait_correlation = ? ORDER BY flow_key ASC LIMIT 2",
    )
    .bind(correlation_id)
    .fetch_all(pool)
    .await
    .map_err(|error| database_error("read SQLite wait correlation", error))?;
    if rows.len() > 1 {
        return Err(CatgaError::new(
            ErrorCode::Conflict,
            "flow wait correlation identifies multiple active flows",
        ));
    }
    rows.into_iter()
        .next()
        .map(|row| {
            let frame: Vec<u8> = row
                .try_get("payload")
                .map_err(|error| database_error("decode SQLite wait correlation frame", error))?;
            let continuation = decode_continuation(&frame)?;
            if continuation
                .wait()
                .is_some_and(|wait| wait.correlation_id() == correlation_id)
            {
                Ok(continuation)
            } else {
                Err(CatgaError::new(
                    ErrorCode::Internal,
                    "SQLite wait correlation index does not match its continuation frame",
                ))
            }
        })
        .transpose()
}

/// Returns summaries after inspecting at most the caller's scan bound.
pub(crate) async fn query(pool: &SqlitePool, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
    let limit = i64::try_from(query.max_scan()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "continuation query scan limit exceeds i64",
        )
    })?;
    let mut statement = QueryBuilder::<Sqlite>::new(
        "SELECT flow_id, flow_type, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns \
         FROM catga_flow_continuations WHERE 1 = 1",
    );
    if let Some(status) = query.status() {
        statement
            .push(" AND status = ")
            .push_bind(status_code(status));
    }
    if let Some(flow_type) = query.flow_type() {
        statement.push(" AND flow_type = ").push_bind(flow_type);
    }
    if let Some((start, end)) = query.created_at_range() {
        let (start_ms, start_subsec_ns) = unix_millis_and_subsec_nanos(start)?;
        let (end_ms, end_subsec_ns) = unix_millis_and_subsec_nanos(end)?;
        statement
            .push(" AND (created_at_ms > ")
            .push_bind(start_ms)
            .push(" OR (created_at_ms = ")
            .push_bind(start_ms)
            .push(" AND created_at_subsec_ns >= ")
            .push_bind(start_subsec_ns)
            .push(")) AND (created_at_ms < ")
            .push_bind(end_ms)
            .push(" OR (created_at_ms = ")
            .push_bind(end_ms)
            .push(" AND created_at_subsec_ns < ")
            .push_bind(end_subsec_ns)
            .push("))");
    }
    statement
        .push(" ORDER BY created_at_ms ASC, created_at_subsec_ns ASC, flow_key ASC LIMIT ")
        .push_bind(limit);
    let rows = statement
        .build()
        .fetch_all(pool)
        .await
        .map_err(|error| database_error("query SQLite continuations", error))?;
    let mut summaries = Vec::with_capacity(query.max_results());
    for row in rows {
        let id: String = row
            .try_get("flow_id")
            .map_err(|error| database_error("decode SQLite summary identity", error))?;
        let flow_type: String = row
            .try_get("flow_type")
            .map_err(|error| database_error("decode SQLite summary flow type", error))?;
        let status = row
            .try_get("status")
            .map_err(|error| database_error("decode SQLite summary status", error))?;
        let version = row
            .try_get("version")
            .map_err(|error| database_error("decode SQLite summary version", error))?;
        let created_at = row
            .try_get("created_at_ms")
            .map_err(|error| database_error("decode SQLite summary creation time", error))?;
        let created_at_subsec_ns = row
            .try_get("created_at_subsec_ns")
            .map_err(|error| database_error("decode SQLite summary creation precision", error))?;
        let updated_at = row
            .try_get("updated_at_ms")
            .map_err(|error| database_error("decode SQLite summary update time", error))?;
        let updated_at_subsec_ns = row
            .try_get("updated_at_subsec_ns")
            .map_err(|error| database_error("decode SQLite summary update precision", error))?;
        let summary = FlowSummary::new(
            id,
            flow_type,
            status_from_code(status)?,
            version,
            system_time_from_unix_millis_and_subsec_nanos(created_at, created_at_subsec_ns)?,
        )
        .with_updated_at(system_time_from_unix_millis_and_subsec_nanos(
            updated_at,
            updated_at_subsec_ns,
        )?);
        if query.matches_summary(&summary) {
            summaries.push(summary);
            if summaries.len() == query.max_results() {
                break;
            }
        }
    }
    Ok(summaries)
}

/// Records a child result through a bounded physical-revision retry loop.
pub(crate) async fn record_wait_success(
    pool: &SqlitePool,
    flow_id: &str,
    version: i64,
    child_id: &str,
    payload: Vec<u8>,
) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id).await? else {
            return Ok(false);
        };
        if current.continuation.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.continuation.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_success(child_id, payload.clone());
        if next_wait.completed_count() == wait.completed_count() {
            return Ok(true);
        }
        let next = current.continuation.clone().with_wait(next_wait);
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("record a SQLite wait result"))
}

/// Deletes a continuation only while both its business version and physical revision match.
pub(crate) async fn delete(
    pool: &SqlitePool,
    flow_id: &str,
    expected_version: i64,
) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id).await? else {
            return Ok(false);
        };
        if current.continuation.state().version() != expected_version {
            return Ok(false);
        }
        let key = flow_key(flow_id);
        let result = sqlx::query(
            "DELETE FROM catga_flow_continuations \
             WHERE flow_key = ? AND flow_id = ? AND revision = ?",
        )
        .bind(key.as_slice())
        .bind(flow_id)
        .bind(current.revision)
        .execute(pool)
        .await
        .map_err(|error| database_error("delete SQLite continuation", error))?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
    }
    Err(cas_error("delete a SQLite continuation"))
}

/// Replaces a continuation after exactly one business-version transition.
pub(crate) async fn update(
    pool: &SqlitePool,
    expected_version: i64,
    next: FlowContinuation,
) -> CatgaResult<bool> {
    if !catga_flow::FlowState::is_next_version(expected_version, next.state().version()) {
        return Ok(false);
    }
    next.validate()?;
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, next.state().id()).await? else {
            return Ok(false);
        };
        if current.continuation.state().version() != expected_version {
            return Ok(false);
        }
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("update a SQLite continuation"))
}

/// Atomically replaces a continuation only when the complete expected snapshot still matches.
pub(crate) async fn claim(
    pool: &SqlitePool,
    expected: &FlowContinuation,
    next: FlowContinuation,
) -> CatgaResult<bool> {
    if next.state().id() != expected.state().id()
        || !catga_flow::FlowState::is_next_version(
            expected.state().version(),
            next.state().version(),
        )
    {
        return Ok(false);
    }
    next.validate()?;
    let Some(current) = load(pool, expected.state().id()).await? else {
        return Ok(false);
    };
    if current.continuation != *expected {
        return Ok(false);
    }
    replace(pool, &current, &next).await
}

/// Records an idempotent child failure under the same bounded CAS loop as successes.
pub(crate) async fn record_wait_failure(
    pool: &SqlitePool,
    flow_id: &str,
    version: i64,
    child_id: &str,
    error: CatgaError,
) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id).await? else {
            return Ok(false);
        };
        if current.continuation.state().version() != version {
            return Ok(false);
        }
        let Some(wait) = current.continuation.wait() else {
            return Ok(false);
        };
        let next_wait = wait.record_failure(child_id, error.clone());
        if next_wait.completed_count() == wait.completed_count() {
            return Ok(true);
        }
        let next = current.continuation.clone().with_wait(next_wait);
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("record a failed SQLite wait result"))
}

/// Refreshes the current owner while preserving the continuation's business version.
pub(crate) async fn heartbeat(
    pool: &SqlitePool,
    flow_id: &str,
    owner: &str,
    version: i64,
) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id).await? else {
            return Ok(false);
        };
        if current.continuation.state().owner() != Some(owner)
            || current.continuation.state().version() != version
        {
            return Ok(false);
        }
        let next_state = current
            .continuation
            .state()
            .clone()
            .heartbeated_at(SystemTime::now());
        let next = current.continuation.clone().with_state(next_state);
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("heartbeat a SQLite continuation"))
}

async fn load(pool: &SqlitePool, flow_id: &str) -> CatgaResult<Option<StoredContinuation>> {
    let key = flow_key(flow_id);
    let row = sqlx::query(
        "SELECT payload, revision FROM catga_flow_continuations WHERE flow_key = ? AND flow_id = ?",
    )
    .bind(key.as_slice())
    .bind(flow_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read SQLite continuation", error))?;
    row.map(|row| {
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode SQLite continuation frame", error))?;
        let continuation = decode_continuation(&frame)?;
        if continuation.state().id() != flow_id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SQLite continuation row identity does not match its frame",
            ));
        }
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| database_error("decode SQLite continuation revision", error))?;
        Ok(StoredContinuation {
            continuation,
            revision,
        })
    })
    .transpose()
}

async fn replace(
    pool: &SqlitePool,
    current: &StoredContinuation,
    next: &FlowContinuation,
) -> CatgaResult<bool> {
    let key = flow_key(next.state().id());
    let (created_at_ms, created_at_subsec_ns) = unix_millis_and_subsec_nanos(next.created_at())?;
    let (updated_at_ms, updated_at_subsec_ns) = unix_millis_and_subsec_nanos(next.updated_at())?;
    let result = sqlx::query(
        "UPDATE catga_flow_continuations SET \
             flow_type = ?, status = ?, version = ?, created_at_ms = ?, created_at_subsec_ns = ?, updated_at_ms = ?, updated_at_subsec_ns = ?, deadline_ms = ?, \
             wait_correlation = ?, payload = ?, revision = revision + 1, due_token = NULL, lease_until_ms = NULL \
         WHERE flow_key = ? AND flow_id = ? AND revision = ?",
    )
    .bind(next.state().flow_type())
    .bind(status_code(next.state().status()))
    .bind(next.state().version())
    .bind(created_at_ms)
    .bind(created_at_subsec_ns)
    .bind(updated_at_ms)
    .bind(updated_at_subsec_ns)
    .bind(deadline_millis(next)?)
    .bind(wait_correlation(next))
    .bind(encode_continuation(next)?)
    .bind(key.as_slice())
    .bind(next.state().id())
    .bind(current.revision)
    .execute(pool)
    .await
    .map_err(|error| database_error("replace SQLite continuation", error))?;
    Ok(result.rows_affected() == 1)
}

fn wait_correlation(continuation: &FlowContinuation) -> Option<&str> {
    continuation.wait().map(|wait| wait.correlation_id())
}

fn deadline_millis(continuation: &FlowContinuation) -> CatgaResult<Option<i64>> {
    flow_timeout_deadline_unix_ms(continuation)?
        .map(|value| {
            i64::try_from(value).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "flow wait deadline exceeds signed SQL milliseconds",
                )
            })
        })
        .transpose()
}

fn status_code(status: FlowStatus) -> i64 {
    match status {
        FlowStatus::Running => 0,
        FlowStatus::Compensating => 1,
        FlowStatus::Suspended => 2,
        FlowStatus::Done => 3,
        FlowStatus::Failed => 4,
        FlowStatus::Cancelled => 5,
    }
}

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("SQL FlowStore could not {operation} after bounded retries"),
    )
}
