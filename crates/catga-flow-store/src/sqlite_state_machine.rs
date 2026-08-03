//! SQLite statements for durable state-machine snapshots.

use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};
use catga_core::flow::StateMachineSnapshot;
use sqlx::{Row, SqlitePool};

use crate::{
    error::database_error,
    key::flow_key,
    state_machine_codec::{decode, encode},
};

const MAX_CAS_RETRIES: usize = 8;

struct StoredSnapshot<S> {
    snapshot: StateMachineSnapshot<S>,
    revision: i64,
}

/// Creates the SQLite state-machine snapshot table.
pub(crate) async fn migrate(pool: &SqlitePool) -> CatgaResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS catga_state_machine_snapshots (\
         instance_key BLOB PRIMARY KEY NOT NULL, instance_id TEXT NOT NULL, \
         version INTEGER NOT NULL, revision INTEGER NOT NULL, payload BLOB NOT NULL)",
    )
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| database_error("create SQLite state-machine snapshot table", error))
}

/// Inserts a snapshot without replacing an existing instance identity.
pub(crate) async fn create<S, C>(
    pool: &SqlitePool,
    snapshot: StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<bool>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(snapshot.instance_id());
    let result = sqlx::query(
        "INSERT INTO catga_state_machine_snapshots \
         (instance_key, instance_id, version, revision, payload) VALUES (?, ?, ?, 0, ?) \
         ON CONFLICT(instance_key) DO NOTHING",
    )
    .bind(key.as_slice())
    .bind(snapshot.instance_id())
    .bind(snapshot.version())
    .bind(encode(&snapshot, codec)?)
    .execute(pool)
    .await
    .map_err(|error| database_error("create SQLite state-machine snapshot", error))?;
    if result.rows_affected() == 1 {
        return Ok(true);
    }
    conflict_result(pool, &key, snapshot.instance_id()).await
}

/// Loads one snapshot by its raw instance identity.
pub(crate) async fn get<S, C>(
    pool: &SqlitePool,
    instance_id: &str,
    codec: &C,
) -> CatgaResult<Option<StateMachineSnapshot<S>>>
where
    C: SnapshotCodec<S>,
{
    load(pool, instance_id, codec)
        .await
        .map(|stored| stored.map(|stored| stored.snapshot))
}

/// Replaces one logical version through bounded physical-revision compare-and-set retries.
pub(crate) async fn update<S, C>(
    pool: &SqlitePool,
    expected_version: i64,
    next: StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<bool>
where
    C: SnapshotCodec<S>,
{
    if expected_version.checked_add(1) != Some(next.version()) {
        return Ok(false);
    }
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, next.instance_id(), codec).await? else {
            return Ok(false);
        };
        if current.snapshot.version() != expected_version {
            return Ok(false);
        }
        if replace(pool, &current, &next, codec).await? {
            return Ok(true);
        }
    }
    Err(cas_error("update SQLite state-machine snapshot"))
}

async fn load<S, C>(
    pool: &SqlitePool,
    instance_id: &str,
    codec: &C,
) -> CatgaResult<Option<StoredSnapshot<S>>>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(instance_id);
    let row = sqlx::query(
        "SELECT version, revision, payload FROM catga_state_machine_snapshots \
         WHERE instance_key = ? AND instance_id = ?",
    )
    .bind(key.as_slice())
    .bind(instance_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| database_error("read SQLite state-machine snapshot", error))?;
    row.map(|row| decode_row(row, instance_id, codec))
        .transpose()
}

async fn replace<S, C>(
    pool: &SqlitePool,
    current: &StoredSnapshot<S>,
    next: &StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<bool>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(next.instance_id());
    let result = sqlx::query(
        "UPDATE catga_state_machine_snapshots SET version = ?, payload = ?, \
         revision = revision + 1 WHERE instance_key = ? AND instance_id = ? AND revision = ?",
    )
    .bind(next.version())
    .bind(encode(next, codec)?)
    .bind(key.as_slice())
    .bind(next.instance_id())
    .bind(current.revision)
    .execute(pool)
    .await
    .map_err(|error| database_error("replace SQLite state-machine snapshot", error))?;
    Ok(result.rows_affected() == 1)
}

async fn conflict_result(
    pool: &SqlitePool,
    key: &[u8; 32],
    instance_id: &str,
) -> CatgaResult<bool> {
    let row =
        sqlx::query("SELECT instance_id FROM catga_state_machine_snapshots WHERE instance_key = ?")
            .bind(key.as_slice())
            .fetch_optional(pool)
            .await
            .map_err(|error| {
                database_error("read conflicting SQLite state-machine snapshot", error)
            })?
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "SQLite state-machine snapshot disappeared after a conflicting create",
                )
            })?;
    let existing: String = row
        .try_get("instance_id")
        .map_err(|error| database_error("decode SQLite state-machine identity", error))?;
    if existing == instance_id {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL state-machine identities",
        ))
    }
}

fn decode_row<S, C>(
    row: sqlx::sqlite::SqliteRow,
    instance_id: &str,
    codec: &C,
) -> CatgaResult<StoredSnapshot<S>>
where
    C: SnapshotCodec<S>,
{
    let version: i64 = row
        .try_get("version")
        .map_err(|error| database_error("decode SQLite state-machine version", error))?;
    let revision: i64 = row
        .try_get("revision")
        .map_err(|error| database_error("decode SQLite state-machine revision", error))?;
    let frame: Vec<u8> = row
        .try_get("payload")
        .map_err(|error| database_error("decode SQLite state-machine frame", error))?;
    let snapshot = decode(instance_id, &frame, codec)?;
    if snapshot.instance_id() != instance_id || snapshot.version() != version {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SQLite state-machine row does not match its snapshot frame",
        ));
    }
    Ok(StoredSnapshot { snapshot, revision })
}

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("SQL state-machine store could not {operation} after bounded retries"),
    )
}
