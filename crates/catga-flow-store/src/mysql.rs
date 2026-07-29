//! MySQL 8 statements for the plain durable FlowStore table.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus, validate_flow_batch_size};
use sqlx::{MySqlPool, Row};

use crate::{
    error::{database_error, is_mysql_duplicate_key},
    key::flow_key,
    sql_backend::{
        MAX_CAS_RETRIES, cas_error, is_stale, stale_before_unix_millis, statement, status_code,
        unix_millis,
    },
    state_codec::{decode_state, encode_state},
};

struct StoredState {
    state: FlowState,
    revision: i64,
}

/// Creates the current MySQL 8 flow-state table and its stale-claim covering index.
///
/// Catga Rust has no historical schema compatibility requirement, so this migration deliberately
/// creates the current format without destructive legacy-column or index rewrites.
pub(crate) async fn migrate(pool: &MySqlPool) -> CatgaResult<()> {
    sqlx::query(statement(
        "CREATE TABLE IF NOT EXISTS catga_flow_states (\
         flow_key BINARY(32) PRIMARY KEY NOT NULL, flow_id LONGTEXT NOT NULL,\
         flow_type LONGTEXT NOT NULL, flow_type_key BINARY(32) NOT NULL,\
         status BIGINT NOT NULL, version BIGINT NOT NULL,\
         heartbeat_ms BIGINT NOT NULL, revision BIGINT NOT NULL, payload LONGBLOB NOT NULL,\
         INDEX catga_flow_states_stale_idx(flow_type_key, status, heartbeat_ms, flow_key)) ENGINE=InnoDB",
        false,
    ))
    .execute(pool)
    .await
    .map_err(|error| database_error("create MySQL FlowStore table", error))?;
    Ok(())
}

/// Inserts a flow state and confirms the unhashed identity after a key collision.
pub(crate) async fn create(pool: &MySqlPool, state: FlowState) -> CatgaResult<bool> {
    let key = flow_key(state.id());
    let result = sqlx::query(statement(
        "INSERT INTO catga_flow_states \
         (flow_key, flow_id, flow_type, flow_type_key, status, version, heartbeat_ms, revision, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
        false,
    ))
    .bind(key.as_slice())
    .bind(state.id())
    .bind(state.flow_type())
    .bind(flow_key(state.flow_type()).as_slice())
    .bind(status_code(state.status()))
    .bind(state.version())
    .bind(unix_millis(state.heartbeat())?)
    .bind(encode_state(&state)?)
    .execute(pool)
    .await;
    let created = match result {
        Ok(result) => result.rows_affected() == 1,
        Err(error) if is_mysql_duplicate_key(&error) => false,
        Err(error) => return Err(database_error("create MySQL flow state", error)),
    };
    if created {
        return Ok(true);
    }
    let row = sqlx::query(statement(
        "SELECT flow_id FROM catga_flow_states WHERE flow_key = ?",
        false,
    ))
    .bind(key.as_slice())
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read conflicting MySQL flow state", error))?;
    let Some(row) = row else {
        return Err(CatgaError::new(
            ErrorCode::Transient,
            "MySQL flow state disappeared after a conflicting create",
        ));
    };
    let existing: String = row
        .try_get("flow_id")
        .map_err(|error| database_error("decode MySQL flow identity", error))?;
    if existing == state.id() {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL FlowStore identities",
        ))
    }
}

/// Inserts many flow states in one transaction so the whole batch pays a single redo-log flush.
pub(crate) async fn create_batch(
    pool: &MySqlPool,
    states: Vec<FlowState>,
) -> CatgaResult<Vec<bool>> {
    validate_flow_batch_size(states.len())?;
    if states.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin MySQL flow batch", error))?;
    let mut created = Vec::with_capacity(states.len());
    for state in &states {
        let key = flow_key(state.id());
        let result = sqlx::query(statement(
            "INSERT INTO catga_flow_states \
             (flow_key, flow_id, flow_type, flow_type_key, status, version, heartbeat_ms, revision, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
            false,
        ))
        .bind(key.as_slice())
        .bind(state.id())
        .bind(state.flow_type())
        .bind(flow_key(state.flow_type()).as_slice())
        .bind(status_code(state.status()))
        .bind(state.version())
        .bind(unix_millis(state.heartbeat())?)
        .bind(encode_state(state)?)
        .execute(&mut *tx)
        .await;
        let inserted = match result {
            Ok(result) => result.rows_affected() == 1,
            Err(error) if is_mysql_duplicate_key(&error) => false,
            Err(error) => return Err(database_error("create MySQL flow state in batch", error)),
        };
        if inserted {
            created.push(true);
            continue;
        }
        let row = sqlx::query(statement(
            "SELECT flow_id FROM catga_flow_states WHERE flow_key = ?",
            false,
        ))
        .bind(key.as_slice())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|error| database_error("read conflicting MySQL flow state in batch", error))?;
        let Some(row) = row else {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "MySQL flow state disappeared after a conflicting create",
            ));
        };
        let existing: String = row
            .try_get("flow_id")
            .map_err(|error| database_error("decode MySQL flow identity in batch", error))?;
        if existing == state.id() {
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
        .map_err(|error| database_error("commit MySQL flow batch", error))?;
    Ok(created)
}

/// Loads a state only if its raw identity agrees with its fixed-width key.
pub(crate) async fn get(pool: &MySqlPool, id: &str) -> CatgaResult<Option<FlowState>> {
    load(pool, id)
        .await
        .map(|stored| stored.map(|stored| stored.state))
}

/// Applies exactly one business-version transition under physical revision CAS.
pub(crate) async fn update(
    pool: &MySqlPool,
    expected_version: i64,
    next: FlowState,
) -> CatgaResult<bool> {
    if !FlowState::is_next_version(expected_version, next.version()) {
        return Ok(false);
    }
    replace_business_version(pool, expected_version, &next).await
}

/// Takes one bounded stale claim in an indexed `FOR UPDATE SKIP LOCKED` transaction.
pub(crate) async fn try_claim(
    pool: &MySqlPool,
    flow_type: &str,
    owner: &str,
    stale_after: Duration,
) -> CatgaResult<Option<FlowState>> {
    let now = SystemTime::now();
    let stale_before = stale_before_unix_millis(now, stale_after)?;
    let limit = i64::try_from(MAX_CAS_RETRIES)
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "MySQL claim retry bound exceeds i64"))?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin MySQL stale claim", error))?;
    let rows = sqlx::query(statement(
        "SELECT flow_id, payload, revision FROM catga_flow_states \
         WHERE flow_type_key = ? AND status = ? AND heartbeat_ms <= ? \
         ORDER BY heartbeat_ms ASC, flow_key ASC LIMIT ? FOR UPDATE SKIP LOCKED",
        false,
    ))
    .bind(flow_key(flow_type).as_slice())
    .bind(status_code(FlowStatus::Running))
    .bind(stale_before)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await
    .map_err(|error| database_error("find stale MySQL flow states", error))?;
    for row in rows {
        let id: String = row
            .try_get("flow_id")
            .map_err(|error| database_error("decode stale MySQL flow identity", error))?;
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode stale MySQL flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "MySQL stale flow row identity does not match its state frame",
            ));
        }
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| database_error("decode stale MySQL flow revision", error))?;
        if state.flow_type() != flow_type
            || state.status() != FlowStatus::Running
            || !is_stale(state.heartbeat(), now, stale_after)
        {
            continue;
        }
        let claimed = state.clone().claimed_by(owner).next_version()?;
        let result = sqlx::query(statement(
            "UPDATE catga_flow_states SET flow_type = ?, flow_type_key = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 \
             WHERE flow_key = ? AND flow_id = ? AND revision = ?", false))
            .bind(claimed.flow_type()).bind(flow_key(claimed.flow_type()).as_slice()).bind(status_code(claimed.status())).bind(claimed.version()).bind(unix_millis(claimed.heartbeat())?).bind(encode_state(&claimed)?)
            .bind(flow_key(claimed.id()).as_slice()).bind(claimed.id()).bind(revision)
            .execute(&mut *tx).await.map_err(|error| database_error("claim stale MySQL flow state", error))?;
        if result.rows_affected() == 1 {
            tx.commit()
                .await
                .map_err(|error| database_error("commit MySQL stale claim", error))?;
            return Ok(Some(claimed));
        }
    }
    tx.commit()
        .await
        .map_err(|error| database_error("commit empty MySQL stale claim", error))?;
    Ok(None)
}

/// Refreshes a fenced owner heartbeat without incrementing its business version.
pub(crate) async fn heartbeat(
    pool: &MySqlPool,
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
        let next = current.state.clone().heartbeated_at(SystemTime::now());
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("heartbeat MySQL flow state"))
}

async fn load(pool: &MySqlPool, id: &str) -> CatgaResult<Option<StoredState>> {
    let key = flow_key(id);
    let row = sqlx::query(statement(
        "SELECT payload, revision FROM catga_flow_states WHERE flow_key = ? AND flow_id = ?",
        false,
    ))
    .bind(key.as_slice())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read MySQL flow state", error))?;
    row.map(|row| {
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode MySQL flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "MySQL flow row identity does not match its state frame",
            ));
        }
        let revision = row
            .try_get("revision")
            .map_err(|error| database_error("decode MySQL flow revision", error))?;
        Ok(StoredState { state, revision })
    })
    .transpose()
}

async fn replace(pool: &MySqlPool, current: &StoredState, next: &FlowState) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let result = sqlx::query(statement(
        "UPDATE catga_flow_states SET flow_type = ?, flow_type_key = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND revision = ?", false))
        .bind(next.flow_type()).bind(flow_key(next.flow_type()).as_slice()).bind(status_code(next.status())).bind(next.version()).bind(unix_millis(next.heartbeat())?).bind(encode_state(next)?)
        .bind(key.as_slice()).bind(next.id()).bind(current.revision).execute(pool).await
        .map_err(|error| database_error("replace MySQL flow state", error))?;
    Ok(result.rows_affected() == 1)
}

/// Applies one business-version transition with one atomic database compare-and-swap.
///
/// A transition increments the public version, so matching that version prevents competing
/// updates without the extra read used by heartbeat's physical-revision fence.
async fn replace_business_version(
    pool: &MySqlPool,
    expected_version: i64,
    next: &FlowState,
) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let result = sqlx::query(statement(
        "UPDATE catga_flow_states SET flow_type = ?, flow_type_key = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND version = ?",
        false,
    ))
    .bind(next.flow_type())
    .bind(flow_key(next.flow_type()).as_slice())
    .bind(status_code(next.status()))
    .bind(next.version())
    .bind(unix_millis(next.heartbeat())?)
    .bind(encode_state(next)?)
    .bind(key.as_slice())
    .bind(next.id())
    .bind(expected_version)
    .execute(pool)
    .await
    .map_err(|error| database_error("update MySQL flow state", error))?;
    Ok(result.rows_affected() == 1)
}
