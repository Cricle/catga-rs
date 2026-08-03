//! SQLite schema and statements for the plain FlowStore table.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{FlowState, FlowStatus, validate_flow_batch_size};
use sqlx::{Row, SqlitePool};

use crate::{
    error::database_error,
    key::flow_key,
    sql_common::{stale_before_unix_millis, unix_millis},
    state_codec::{decode_state, encode_state},
};

const MAX_CAS_RETRIES: usize = 8;

struct StoredState {
    state: FlowState,
    revision: i64,
}

/// Creates the plain FlowStore schema and its stale-claim index atomically.
pub(crate) async fn migrate(pool: &SqlitePool) -> CatgaResult<()> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| database_error("begin SQLite migration", error))?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_flow_states (\
             flow_key BLOB PRIMARY KEY NOT NULL,\
             flow_id TEXT NOT NULL UNIQUE,\
             flow_type TEXT NOT NULL,\
             status INTEGER NOT NULL,\
             version INTEGER NOT NULL,\
             heartbeat_ms INTEGER NOT NULL,\
             revision INTEGER NOT NULL,\
             payload BLOB NOT NULL\
         )",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite FlowStore table", error))?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catga_flow_states_stale_idx \
         ON catga_flow_states(flow_type, status, heartbeat_ms, flow_key)",
    )
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("create SQLite FlowStore index", error))?;
    transaction
        .commit()
        .await
        .map_err(|error| database_error("commit SQLite migration", error))
}

/// Inserts one state without replacing an existing identity.
pub(crate) async fn create(pool: &SqlitePool, state: FlowState) -> CatgaResult<bool> {
    let key = flow_key(state.id());
    let frame = encode_state(&state)?;
    let heartbeat = unix_millis(state.heartbeat())?;
    let result = sqlx::query(
        "INSERT INTO catga_flow_states \
         (flow_key, flow_id, flow_type, status, version, heartbeat_ms, revision, payload) \
         VALUES (?, ?, ?, ?, ?, ?, 0, ?) \
         ON CONFLICT(flow_key) DO NOTHING",
    )
    .bind(key.as_slice())
    .bind(state.id())
    .bind(state.flow_type())
    .bind(status_code(state.status()))
    .bind(state.version())
    .bind(heartbeat)
    .bind(frame)
    .execute(pool)
    .await
    .map_err(|error| database_error("create SQLite flow state", error))?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    let existing = sqlx::query("SELECT flow_id FROM catga_flow_states WHERE flow_key = ?")
        .bind(key.as_slice())
        .fetch_optional(pool)
        .await
        .map_err(|error| database_error("read conflicting SQLite flow state", error))?;
    let Some(existing) = existing else {
        return Err(CatgaError::new(
            ErrorCode::Transient,
            "SQLite flow state disappeared after a conflicting create",
        ));
    };
    let existing_id: String = existing
        .try_get("flow_id")
        .map_err(|error| database_error("decode SQLite flow identity", error))?;
    if existing_id == state.id() {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL FlowStore identities",
        ))
    }
}

/// Inserts many flow states in one transaction so the whole batch pays a single durability flush.
///
/// Per-row conflict handling mirrors [`create`]: a row whose identity already exists yields
/// `false`, while a fixed-width key collision returns an internal error.
pub(crate) async fn create_batch(
    pool: &SqlitePool,
    states: Vec<FlowState>,
) -> CatgaResult<Vec<bool>> {
    validate_flow_batch_size(states.len())?;
    if states.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin SQLite flow batch", error))?;
    let mut created = Vec::with_capacity(states.len());
    for state in &states {
        let key = flow_key(state.id());
        let frame = encode_state(state)?;
        let heartbeat = unix_millis(state.heartbeat())?;
        let result = sqlx::query(
            "INSERT INTO catga_flow_states \
             (flow_key, flow_id, flow_type, status, version, heartbeat_ms, revision, payload) \
             VALUES (?, ?, ?, ?, ?, ?, 0, ?) \
             ON CONFLICT(flow_key) DO NOTHING",
        )
        .bind(key.as_slice())
        .bind(state.id())
        .bind(state.flow_type())
        .bind(status_code(state.status()))
        .bind(state.version())
        .bind(heartbeat)
        .bind(frame)
        .execute(&mut *tx)
        .await
        .map_err(|error| database_error("create SQLite flow state in batch", error))?;
        if result.rows_affected() == 1 {
            created.push(true);
            continue;
        }
        let existing = sqlx::query("SELECT flow_id FROM catga_flow_states WHERE flow_key = ?")
            .bind(key.as_slice())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|error| {
                database_error("read conflicting SQLite flow state in batch", error)
            })?;
        let Some(existing) = existing else {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "SQLite flow state disappeared after a conflicting create",
            ));
        };
        let existing_id: String = existing
            .try_get("flow_id")
            .map_err(|error| database_error("decode SQLite flow identity in batch", error))?;
        if existing_id == state.id() {
            created.push(false);
        } else {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SHA-256 collision between SQL FlowStore identities",
            ));
        }
    }
    tx.commit()
        .await
        .map_err(|error| database_error("commit SQLite flow batch", error))?;
    Ok(created)
}

/// Loads and validates one state by its unhashed identity.
pub(crate) async fn get(pool: &SqlitePool, id: &str) -> CatgaResult<Option<FlowState>> {
    load(pool, id)
        .await
        .map(|state| state.map(|state| state.state))
}

/// Replaces a business-state version after a physical-revision guarded retry loop.
pub(crate) async fn update(
    pool: &SqlitePool,
    expected_version: i64,
    next: FlowState,
) -> CatgaResult<bool> {
    if !FlowState::is_next_version(expected_version, next.version()) {
        return Ok(false);
    }
    replace_business_version(pool, expected_version, &next).await
}

/// Claims one stale running state of `flow_type` using bounded indexed candidates.
pub(crate) async fn try_claim(
    pool: &SqlitePool,
    flow_type: &str,
    owner: &str,
    stale_after: Duration,
) -> CatgaResult<Option<FlowState>> {
    let now = SystemTime::now();
    let stale_before = stale_before_unix_millis(now, stale_after)?;
    let candidates = sqlx::query(
        "SELECT flow_id, payload, revision FROM catga_flow_states \
         WHERE flow_type = ? AND status = ? AND heartbeat_ms <= ? \
         ORDER BY heartbeat_ms ASC, flow_key ASC LIMIT ?",
    )
    .bind(flow_type)
    .bind(status_code(FlowStatus::Running))
    .bind(stale_before)
    .bind(i64::try_from(MAX_CAS_RETRIES).map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "SQLite claim retry bound exceeds i64")
    })?)
    .fetch_all(pool)
    .await
    .map_err(|error| database_error("find stale SQLite flow states", error))?;
    for candidate in candidates {
        let id: String = candidate
            .try_get("flow_id")
            .map_err(|error| database_error("decode stale SQLite flow identity", error))?;
        let frame: Vec<u8> = candidate
            .try_get("payload")
            .map_err(|error| database_error("decode stale SQLite flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SQLite stale flow row identity does not match its state frame",
            ));
        }
        let revision: i64 = candidate
            .try_get("revision")
            .map_err(|error| database_error("decode stale SQLite flow revision", error))?;
        let current = StoredState { state, revision };
        if current.state.flow_type() != flow_type
            || current.state.status() != FlowStatus::Running
            || !is_stale(current.state.heartbeat(), now, stale_after)
        {
            continue;
        }
        let claimed = current.state.clone().claimed_by(owner).next_version()?;
        if replace(pool, &current, &claimed).await? {
            return Ok(Some(claimed));
        }
    }
    Ok(None)
}

/// Refreshes an owner's liveness without changing the business-state version.
pub(crate) async fn heartbeat(
    pool: &SqlitePool,
    id: &str,
    owner: &str,
    version: i64,
) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, id).await? else {
            return Ok(false);
        };
        if current.state.owner() != Some(owner) || current.state.version() != version {
            return Ok(false);
        }
        let heartbeated = current.state.clone().heartbeated_at(SystemTime::now());
        if replace(pool, &current, &heartbeated).await? {
            return Ok(true);
        }
    }
    Err(cas_error("heartbeat SQLite flow state"))
}

async fn load(pool: &SqlitePool, id: &str) -> CatgaResult<Option<StoredState>> {
    let key = flow_key(id);
    let row = sqlx::query(
        "SELECT payload, revision FROM catga_flow_states WHERE flow_key = ? AND flow_id = ?",
    )
    .bind(key.as_slice())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read SQLite flow state", error))?;
    row.map(|row| {
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode SQLite flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SQLite flow row identity does not match its state frame",
            ));
        }
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| database_error("decode SQLite flow revision", error))?;
        Ok(StoredState { state, revision })
    })
    .transpose()
}

async fn replace(pool: &SqlitePool, current: &StoredState, next: &FlowState) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let result = sqlx::query(
        "UPDATE catga_flow_states SET \
             flow_type = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND revision = ?",
    )
    .bind(next.flow_type())
    .bind(status_code(next.status()))
    .bind(next.version())
    .bind(unix_millis(next.heartbeat())?)
    .bind(encode_state(next)?)
    .bind(key.as_slice())
    .bind(next.id())
    .bind(current.revision)
    .execute(pool)
    .await
    .map_err(|error| database_error("replace SQLite flow state", error))?;
    Ok(result.rows_affected() == 1)
}

/// Applies one business-version transition without first reading the physical revision.
///
/// Business versions advance exactly once, so the version predicate is an atomic CAS fence for
/// competing transitions. Heartbeats retain their stricter physical-revision fence in [`replace`]
/// because they intentionally preserve the same business version.
async fn replace_business_version(
    pool: &SqlitePool,
    expected_version: i64,
    next: &FlowState,
) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let result = sqlx::query(
        "UPDATE catga_flow_states SET \
             flow_type = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND version = ?",
    )
    .bind(next.flow_type())
    .bind(status_code(next.status()))
    .bind(next.version())
    .bind(unix_millis(next.heartbeat())?)
    .bind(encode_state(next)?)
    .bind(key.as_slice())
    .bind(next.id())
    .bind(expected_version)
    .execute(pool)
    .await
    .map_err(|error| database_error("update SQLite flow state", error))?;
    Ok(result.rows_affected() == 1)
}

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("SQL FlowStore could not {operation} after bounded retries"),
    )
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

fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
