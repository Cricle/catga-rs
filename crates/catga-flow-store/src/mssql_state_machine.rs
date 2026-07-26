//! SQL Server statements for durable state-machine snapshots.

use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};
use catga_flow::StateMachineSnapshot;
use tiberius::Query;

use crate::{
    MssqlPool,
    error::database_error,
    key::flow_key,
    mssql::{missing_column, required_bytes, required_i64, required_str},
    sql_common::{MAX_CAS_RETRIES, cas_error},
    state_machine_codec::{decode, encode},
};

struct StoredSnapshot<S> {
    snapshot: StateMachineSnapshot<S>,
    revision: i64,
}

/// Creates the SQL Server state-machine snapshot table.
pub(crate) async fn migrate(pool: &MssqlPool) -> CatgaResult<()> {
    let mut connection = pool.get().await.map_err(|error| {
        database_error(
            "acquire SQL Server state-machine migration connection",
            error,
        )
    })?;
    connection
        .execute(
            "IF OBJECT_ID(N'dbo.catga_state_machine_snapshots', N'U') IS NULL BEGIN \
             CREATE TABLE dbo.catga_state_machine_snapshots (\
             instance_key BINARY(32) NOT NULL, instance_id NVARCHAR(MAX) NOT NULL, \
             version BIGINT NOT NULL, revision BIGINT NOT NULL, payload VARBINARY(MAX) NOT NULL, \
             CONSTRAINT PK_catga_state_machine_snapshots PRIMARY KEY(instance_key)); END;",
            &[],
        )
        .await
        .map(|_| ())
        .map_err(|error| database_error("create SQL Server state-machine snapshot table", error))
}

/// Inserts a snapshot without using SQL Server `MERGE`.
pub(crate) async fn create<S, C>(
    pool: &MssqlPool,
    snapshot: StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<bool>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(snapshot.instance_id());
    let frame = encode(&snapshot, codec)?;
    let mut query = Query::new(
        "IF NOT EXISTS (SELECT 1 FROM dbo.catga_state_machine_snapshots WITH (UPDLOCK, HOLDLOCK) \
         WHERE instance_key = @P1) BEGIN INSERT INTO dbo.catga_state_machine_snapshots \
         (instance_key, instance_id, version, revision, payload) VALUES (@P1, @P2, @P3, 0, @P4); \
         SELECT CAST(1 AS BIGINT) AS inserted; END ELSE SELECT CAST(0 AS BIGINT) AS inserted;",
    );
    query.bind(key.as_slice());
    query.bind(snapshot.instance_id());
    query.bind(snapshot.version());
    query.bind(frame.as_slice());
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server state-machine create connection", error)
    })?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("create SQL Server state-machine snapshot", error))?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server state-machine create result", error))?
        .ok_or_else(|| missing_column("SQL Server state-machine create result row"))?;
    if required_i64(&row, "inserted", "SQL Server state-machine create result")? == 1 {
        return Ok(true);
    }
    conflict_result(&mut connection, &key, snapshot.instance_id()).await
}

/// Loads one snapshot by its raw instance identity.
pub(crate) async fn get<S, C>(
    pool: &MssqlPool,
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
    pool: &MssqlPool,
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
    Err(cas_error("update SQL Server state-machine snapshot"))
}

async fn load<S, C>(
    pool: &MssqlPool,
    instance_id: &str,
    codec: &C,
) -> CatgaResult<Option<StoredSnapshot<S>>>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(instance_id);
    let mut query = Query::new(
        "SELECT version, revision, payload FROM dbo.catga_state_machine_snapshots \
         WHERE instance_key = @P1 AND instance_id = @P2",
    );
    query.bind(key.as_slice());
    query.bind(instance_id);
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server state-machine read connection", error)
    })?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read SQL Server state-machine snapshot", error))?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server state-machine snapshot row", error))?;
    row.map(|row| decode_row(&row, instance_id, codec))
        .transpose()
}

async fn replace<S, C>(
    pool: &MssqlPool,
    current: &StoredSnapshot<S>,
    next: &StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<bool>
where
    C: SnapshotCodec<S>,
{
    let key = flow_key(next.instance_id());
    let frame = encode(next, codec)?;
    let mut query = Query::new(
        "UPDATE dbo.catga_state_machine_snapshots SET version = @P1, payload = @P2, \
         revision = revision + 1 WHERE instance_key = @P3 AND instance_id = @P4 AND revision = @P5",
    );
    query.bind(next.version());
    query.bind(frame.as_slice());
    query.bind(key.as_slice());
    query.bind(next.instance_id());
    query.bind(current.revision);
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server state-machine update connection", error)
    })?;
    query
        .execute(&mut connection)
        .await
        .map(|result| result.total() == 1)
        .map_err(|error| database_error("replace SQL Server state-machine snapshot", error))
}

async fn conflict_result(
    connection: &mut bb8::PooledConnection<'_, bb8_tiberius::ConnectionManager>,
    key: &[u8; 32],
    instance_id: &str,
) -> CatgaResult<bool> {
    let mut query = Query::new(
        "SELECT instance_id FROM dbo.catga_state_machine_snapshots WHERE instance_key = @P1",
    );
    query.bind(key.as_slice());
    let row = query
        .query(connection)
        .await
        .map_err(|error| {
            database_error("read conflicting SQL Server state-machine snapshot", error)
        })?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server state-machine conflict row", error))?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "SQL Server state-machine snapshot disappeared after a conflicting create",
            )
        })?;
    let existing = required_str(
        &row,
        "instance_id",
        "SQL Server state-machine conflict identity",
    )?;
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
    row: &tiberius::Row,
    instance_id: &str,
    codec: &C,
) -> CatgaResult<StoredSnapshot<S>>
where
    C: SnapshotCodec<S>,
{
    let version = required_i64(row, "version", "SQL Server state-machine version")?;
    let revision = required_i64(row, "revision", "SQL Server state-machine revision")?;
    let snapshot = decode(
        instance_id,
        required_bytes(row, "payload", "SQL Server state-machine frame")?,
        codec,
    )?;
    if snapshot.instance_id() != instance_id || snapshot.version() != version {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SQL Server state-machine row does not match its snapshot frame",
        ));
    }
    Ok(StoredSnapshot { snapshot, revision })
}
