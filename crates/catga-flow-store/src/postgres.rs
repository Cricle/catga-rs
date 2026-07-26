//! PostgreSQL statements for the plain durable FlowStore table.

use crate::{
    error::database_error,
    key::flow_key,
    sql_backend::{
        MAX_CAS_RETRIES, cas_error, is_stale, stale_before_unix_millis, statement, status_code,
        unix_millis,
    },
    state_codec::{decode_state, encode_state},
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus};
use sqlx::{PgPool, Row};
use std::time::{Duration, SystemTime};

struct StoredState {
    state: FlowState,
    revision: i64,
}
const PG: bool = true;

/// Creates the PostgreSQL flow-state table and its stale-claim index.
pub(crate) async fn migrate(pool: &PgPool) -> CatgaResult<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin PostgreSQL FlowStore migration", error))?;
    for sql in [
        "CREATE TABLE IF NOT EXISTS catga_flow_states (flow_key BYTEA PRIMARY KEY NOT NULL, flow_id TEXT NOT NULL UNIQUE, flow_type TEXT NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, heartbeat_ms BIGINT NOT NULL, revision BIGINT NOT NULL, payload BYTEA NOT NULL)",
        "CREATE INDEX IF NOT EXISTS catga_flow_states_stale_idx ON catga_flow_states(flow_type, status, heartbeat_ms, flow_key)",
    ] {
        sqlx::query(statement(sql, PG))
            .execute(&mut *tx)
            .await
            .map_err(|error| database_error("create PostgreSQL FlowStore schema", error))?;
    }
    tx.commit()
        .await
        .map_err(|error| database_error("commit PostgreSQL FlowStore migration", error))
}

/// Inserts a flow state and checks the raw identifier on hash-key conflict.
pub(crate) async fn create(pool: &PgPool, state: FlowState) -> CatgaResult<bool> {
    let key = flow_key(state.id());
    let result = sqlx::query(statement("INSERT INTO catga_flow_states (flow_key, flow_id, flow_type, status, version, heartbeat_ms, revision, payload) VALUES (?, ?, ?, ?, ?, ?, 0, ?) ON CONFLICT(flow_key) DO NOTHING", PG))
        .bind(key.as_slice()).bind(state.id()).bind(state.flow_type()).bind(status_code(state.status())).bind(state.version()).bind(unix_millis(state.heartbeat())?).bind(encode_state(&state)?)
        .execute(pool).await.map_err(|error| database_error("create PostgreSQL flow state", error))?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    let row = sqlx::query(statement(
        "SELECT flow_id FROM catga_flow_states WHERE flow_key = ?",
        PG,
    ))
    .bind(key.as_slice())
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read conflicting PostgreSQL flow state", error))?;
    let Some(row) = row else {
        return Err(CatgaError::new(
            ErrorCode::Transient,
            "PostgreSQL flow state disappeared after a conflicting create",
        ));
    };
    let existing: String = row
        .try_get("flow_id")
        .map_err(|error| database_error("decode PostgreSQL flow identity", error))?;
    if existing == state.id() {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL FlowStore identities",
        ))
    }
}

/// Loads one flow state by both its fixed key and raw identity.
pub(crate) async fn get(pool: &PgPool, id: &str) -> CatgaResult<Option<FlowState>> {
    load(pool, id)
        .await
        .map(|stored| stored.map(|stored| stored.state))
}

/// Replaces exactly one business version under bounded physical-revision retries.
pub(crate) async fn update(
    pool: &PgPool,
    expected_version: i64,
    next: FlowState,
) -> CatgaResult<bool> {
    if next.version() != expected_version.saturating_add(1) {
        return Ok(false);
    }
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, next.id()).await? else {
            return Ok(false);
        };
        if current.state.version() != expected_version {
            return Ok(false);
        }
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("update PostgreSQL flow state"))
}

/// Claims one stale state using an indexed, bounded `FOR UPDATE SKIP LOCKED` transaction.
pub(crate) async fn try_claim(
    pool: &PgPool,
    flow_type: &str,
    owner: &str,
    stale_after: Duration,
) -> CatgaResult<Option<FlowState>> {
    let now = SystemTime::now();
    let stale_before = stale_before_unix_millis(now, stale_after)?;
    let limit = i64::try_from(MAX_CAS_RETRIES).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "PostgreSQL claim retry bound exceeds i64",
        )
    })?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| database_error("begin PostgreSQL stale claim", error))?;
    let rows = sqlx::query(statement("SELECT flow_id, payload, revision FROM catga_flow_states WHERE flow_type = ? AND status = ? AND heartbeat_ms <= ? ORDER BY heartbeat_ms ASC, flow_key ASC LIMIT ? FOR UPDATE SKIP LOCKED", PG))
        .bind(flow_type).bind(status_code(FlowStatus::Running)).bind(stale_before).bind(limit).fetch_all(&mut *tx).await.map_err(|error| database_error("find stale PostgreSQL flow states", error))?;
    for row in rows {
        let id: String = row
            .try_get("flow_id")
            .map_err(|error| database_error("decode stale PostgreSQL flow identity", error))?;
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode stale PostgreSQL flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "PostgreSQL stale flow row identity does not match its state frame",
            ));
        }
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| database_error("decode stale PostgreSQL flow revision", error))?;
        if state.flow_type() != flow_type
            || state.status() != FlowStatus::Running
            || !is_stale(state.heartbeat(), now, stale_after)
        {
            continue;
        }
        let claimed = state.clone().claimed_by(owner).next_version();
        let key = flow_key(claimed.id());
        let changed = sqlx::query(statement("UPDATE catga_flow_states SET flow_type = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 WHERE flow_key = ? AND flow_id = ? AND revision = ?", PG))
            .bind(claimed.flow_type()).bind(status_code(claimed.status())).bind(claimed.version()).bind(unix_millis(claimed.heartbeat())?).bind(encode_state(&claimed)?).bind(key.as_slice()).bind(claimed.id()).bind(revision)
            .execute(&mut *tx).await.map_err(|error| database_error("claim stale PostgreSQL flow state", error))?;
        if changed.rows_affected() == 1 {
            tx.commit()
                .await
                .map_err(|error| database_error("commit PostgreSQL stale claim", error))?;
            return Ok(Some(claimed));
        }
    }
    tx.commit()
        .await
        .map_err(|error| database_error("commit empty PostgreSQL stale claim", error))?;
    Ok(None)
}

/// Refreshes an owner heartbeat while retaining the business version.
pub(crate) async fn heartbeat(
    pool: &PgPool,
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
    Err(cas_error("heartbeat PostgreSQL flow state"))
}

async fn load(pool: &PgPool, id: &str) -> CatgaResult<Option<StoredState>> {
    let key = flow_key(id);
    let row = sqlx::query(statement(
        "SELECT payload, revision FROM catga_flow_states WHERE flow_key = ? AND flow_id = ?",
        PG,
    ))
    .bind(key.as_slice())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read PostgreSQL flow state", error))?;
    row.map(|row| {
        let frame: Vec<u8> = row
            .try_get("payload")
            .map_err(|error| database_error("decode PostgreSQL flow frame", error))?;
        let state = decode_state(&frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "PostgreSQL flow row identity does not match its state frame",
            ));
        }
        let revision = row
            .try_get("revision")
            .map_err(|error| database_error("decode PostgreSQL flow revision", error))?;
        Ok(StoredState { state, revision })
    })
    .transpose()
}

async fn replace(pool: &PgPool, current: &StoredState, next: &FlowState) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let result = sqlx::query(statement("UPDATE catga_flow_states SET flow_type = ?, status = ?, version = ?, heartbeat_ms = ?, payload = ?, revision = revision + 1 WHERE flow_key = ? AND flow_id = ? AND revision = ?", PG))
        .bind(next.flow_type()).bind(status_code(next.status())).bind(next.version()).bind(unix_millis(next.heartbeat())?).bind(encode_state(next)?).bind(key.as_slice()).bind(next.id()).bind(current.revision).execute(pool).await.map_err(|error| database_error("replace PostgreSQL flow state", error))?;
    Ok(result.rows_affected() == 1)
}
