//! SQL Server statements for the plain durable FlowStore table.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus};
use tiberius::{Query, Row};

use crate::{
    MssqlPool,
    error::database_error,
    key::flow_key,
    sql_common::{
        MAX_CAS_RETRIES, cas_error, is_stale, stale_before_unix_millis, status_code, unix_millis,
    },
    state_codec::{decode_state, encode_state},
};

struct StoredState {
    state: FlowState,
    revision: i64,
}

/// Creates the SQL Server state table and stale-claim index in one guarded batch.
pub(crate) async fn migrate(pool: &MssqlPool) -> CatgaResult<()> {
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server migration connection", error))?;
    connection
        .execute(
            "BEGIN TRANSACTION; \
             DECLARE @result INT; \
             EXEC @result = sys.sp_getapplock @Resource = N'catga_flow_states_schema', \
               @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 5000; \
             IF @result < 0 THROW 50000, 'could not acquire the Catga FlowStore schema lock', 1;",
            &[],
        )
        .await
        .map(|_| ())
        .map_err(|error| database_error("lock SQL Server FlowStore schema migration", error))?;
    let result = async {
        connection
            .execute(
                "IF OBJECT_ID(N'dbo.catga_flow_states', N'U') IS NULL BEGIN \
                 CREATE TABLE dbo.catga_flow_states (\
                   flow_key BINARY(32) NOT NULL PRIMARY KEY, flow_id NVARCHAR(MAX) NOT NULL, \
                   flow_type NVARCHAR(MAX) NOT NULL, flow_type_key BINARY(32) NOT NULL, \
                   status BIGINT NOT NULL, version BIGINT NOT NULL, \
                   heartbeat_ms BIGINT NOT NULL, revision BIGINT NOT NULL, payload VARBINARY(MAX) NOT NULL); \
                 CREATE INDEX catga_flow_states_stale_idx ON dbo.catga_flow_states \
                   (flow_type_key, status, heartbeat_ms, flow_key); END; \
                 DECLARE @drop_flow_id_unique nvarchar(max) = N''; \
                 SELECT @drop_flow_id_unique += N'ALTER TABLE dbo.catga_flow_states DROP CONSTRAINT ' \
                   + QUOTENAME(key_constraint.name) + N';' \
                 FROM sys.key_constraints AS key_constraint \
                 INNER JOIN sys.index_columns AS index_column \
                   ON index_column.object_id = key_constraint.parent_object_id \
                   AND index_column.index_id = key_constraint.unique_index_id \
                 WHERE key_constraint.parent_object_id = OBJECT_ID(N'dbo.catga_flow_states') \
                   AND key_constraint.type = N'UQ' \
                   AND COL_NAME(index_column.object_id, index_column.column_id) = N'flow_id'; \
                 IF @drop_flow_id_unique <> N'' EXEC sys.sp_executesql @drop_flow_id_unique; \
                 IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_states') \
                   AND name = N'catga_flow_states_stale_idx') \
                   DROP INDEX catga_flow_states_stale_idx ON dbo.catga_flow_states; \
                 IF COL_LENGTH(N'dbo.catga_flow_states', N'flow_type_key') IS NULL \
                   ALTER TABLE dbo.catga_flow_states ADD flow_type_key BINARY(32) NULL; \
                 IF COL_LENGTH(N'dbo.catga_flow_states', N'flow_id') <> -1 \
                   ALTER TABLE dbo.catga_flow_states ALTER COLUMN flow_id NVARCHAR(MAX) NOT NULL; \
                 IF COL_LENGTH(N'dbo.catga_flow_states', N'flow_type') <> -1 \
                   ALTER TABLE dbo.catga_flow_states ALTER COLUMN flow_type NVARCHAR(MAX) NOT NULL;",
                &[],
            )
            .await
            .map(|_| ())
            .map_err(|error| database_error("create SQL Server FlowStore schema", error))?;
        backfill_flow_type_keys(&mut connection).await?;
        connection
            .execute(
                "ALTER TABLE dbo.catga_flow_states ALTER COLUMN flow_type_key BINARY(32) NOT NULL; \
                 IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_states') \
                   AND name = N'catga_flow_states_stale_idx') \
                   CREATE INDEX catga_flow_states_stale_idx ON dbo.catga_flow_states \
                     (flow_type_key, status, heartbeat_ms, flow_key);",
                &[],
            )
            .await
            .map(|_| ())
            .map_err(|error| database_error("finalize SQL Server FlowStore migration", error))
    }
    .await;
    match result {
        Ok(()) => connection
            .execute("COMMIT TRANSACTION", &[])
            .await
            .map(|_| ())
            .map_err(|error| database_error("commit SQL Server FlowStore schema migration", error)),
        Err(error) => {
            let _ = connection
                .execute("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION", &[])
                .await;
            Err(error)
        }
    }
}

/// Inserts one state without using SQL Server `MERGE`.
pub(crate) async fn create(pool: &MssqlPool, state: FlowState) -> CatgaResult<bool> {
    let key = flow_key(state.id());
    let type_key = flow_key(state.flow_type());
    let frame = encode_state(&state)?;
    let heartbeat = unix_millis(state.heartbeat())?;
    let mut query = Query::new(
        "IF NOT EXISTS (SELECT 1 FROM dbo.catga_flow_states WITH (UPDLOCK, HOLDLOCK) \
           WHERE flow_key = @P1) BEGIN \
           INSERT INTO dbo.catga_flow_states \
             (flow_key, flow_id, flow_type, flow_type_key, status, version, heartbeat_ms, revision, payload) \
           VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, 0, @P8); \
           SELECT CAST(1 AS BIGINT) AS inserted; END \
         ELSE SELECT CAST(0 AS BIGINT) AS inserted;",
    );
    query.bind(key.as_slice());
    query.bind(state.id());
    query.bind(state.flow_type());
    query.bind(type_key.as_slice());
    query.bind(status_code(state.status()));
    query.bind(state.version());
    query.bind(heartbeat);
    query.bind(frame.as_slice());
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server create connection", error))?;
    let stream = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("create SQL Server flow state", error))?;
    let row = stream
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server create result", error))?
        .ok_or_else(|| missing_column("SQL Server create result row"))?;
    let inserted = required_i64(&row, "inserted", "SQL Server create result")? == 1;
    if inserted {
        return Ok(true);
    }
    collision_result(&mut connection, &key, state.id()).await
}

/// Loads one state by its fixed key and original identity.
pub(crate) async fn get(pool: &MssqlPool, id: &str) -> CatgaResult<Option<FlowState>> {
    load(pool, id)
        .await
        .map(|stored| stored.map(|stored| stored.state))
}

/// Applies one business-version transition under bounded revision CAS.
pub(crate) async fn update(
    pool: &MssqlPool,
    expected_version: i64,
    next: FlowState,
) -> CatgaResult<bool> {
    if !FlowState::is_next_version(expected_version, next.version()) {
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
    Err(cas_error("update SQL Server flow state"))
}

/// Claims one of at most eight indexed stale candidates with revision fencing.
pub(crate) async fn try_claim(
    pool: &MssqlPool,
    flow_type: &str,
    owner: &str,
    stale_after: Duration,
) -> CatgaResult<Option<FlowState>> {
    let now = SystemTime::now();
    let stale_before = stale_before_unix_millis(now, stale_after)?;
    let type_key = flow_key(flow_type);
    let limit = i64::try_from(MAX_CAS_RETRIES).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQL Server claim retry bound exceeds i64",
        )
    })?;
    let mut query = Query::new(
        "SELECT TOP (@P4) flow_id, payload, revision FROM dbo.catga_flow_states \
         WITH (UPDLOCK, READPAST, ROWLOCK, INDEX(catga_flow_states_stale_idx)) \
         WHERE flow_type_key = @P1 AND status = @P2 AND heartbeat_ms <= @P3 \
         ORDER BY heartbeat_ms ASC, flow_key ASC",
    );
    query.bind(type_key.as_slice());
    query.bind(status_code(FlowStatus::Running));
    query.bind(stale_before);
    query.bind(limit);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server claim connection", error))?;
    connection
        .simple_query("BEGIN TRANSACTION")
        .await
        .map_err(|error| database_error("begin SQL Server stale claim", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("begin SQL Server stale claim", error))?;
    let result = async {
        let stream = query
            .query(&mut connection)
            .await
            .map_err(|error| database_error("find stale SQL Server flow states", error))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|error| database_error("read stale SQL Server flow states", error))?;

        for row in rows {
            let id = required_str(&row, "flow_id", "stale SQL Server flow identity")?;
            let frame = required_bytes(&row, "payload", "stale SQL Server flow frame")?;
            let revision = required_i64(&row, "revision", "stale SQL Server flow revision")?;
            let state = decode_state(frame)?;
            if state.id() != id {
                return Err(CatgaError::new(
                    ErrorCode::Internal,
                    "SQL Server stale flow identity does not match its frame",
                ));
            }
            if state.flow_type() != flow_type
                || state.status() != FlowStatus::Running
                || !is_stale(state.heartbeat(), now, stale_after)
            {
                continue;
            }
            let claimed = state.claimed_by(owner).next_version()?;
            if replace_revision_on_connection(&mut connection, &claimed, revision).await? {
                return Ok(Some(claimed));
            }
        }
        Ok(None)
    }
    .await;
    match result {
        Ok(claimed) => {
            connection
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(|error| database_error("commit SQL Server stale claim", error))?
                .into_first_result()
                .await
                .map_err(|error| database_error("commit SQL Server stale claim", error))?;
            Ok(claimed)
        }
        Err(error) => {
            if let Ok(stream) = connection
                .simple_query("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
                .await
            {
                let _ = stream.into_first_result().await;
            }
            Err(error)
        }
    }
}

/// Refreshes the current owner's heartbeat without changing business version.
pub(crate) async fn heartbeat(
    pool: &MssqlPool,
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
    Err(cas_error("heartbeat SQL Server flow state"))
}

async fn load(pool: &MssqlPool, id: &str) -> CatgaResult<Option<StoredState>> {
    let key = flow_key(id);
    let mut query = Query::new(
        "SELECT payload, revision FROM dbo.catga_flow_states \
         WHERE flow_key = @P1 AND flow_id = @P2",
    );
    query.bind(key.as_slice());
    query.bind(id);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server read connection", error))?;
    let stream = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read SQL Server flow state", error))?;
    let row = stream
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server flow state row", error))?;
    row.map(|row| {
        let frame = required_bytes(&row, "payload", "SQL Server flow frame")?;
        let state = decode_state(frame)?;
        if state.id() != id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SQL Server flow identity does not match its frame",
            ));
        }
        let revision = required_i64(&row, "revision", "SQL Server flow revision")?;
        Ok(StoredState { state, revision })
    })
    .transpose()
}

async fn replace(pool: &MssqlPool, current: &StoredState, next: &FlowState) -> CatgaResult<bool> {
    replace_revision(pool, next, current.revision).await
}

async fn replace_revision(pool: &MssqlPool, next: &FlowState, revision: i64) -> CatgaResult<bool> {
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server update connection", error))?;
    replace_revision_on_connection(&mut connection, next, revision).await
}

async fn replace_revision_on_connection(
    connection: &mut bb8::PooledConnection<'_, bb8_tiberius::ConnectionManager>,
    next: &FlowState,
    revision: i64,
) -> CatgaResult<bool> {
    let key = flow_key(next.id());
    let type_key = flow_key(next.flow_type());
    let frame = encode_state(next)?;
    let mut query = Query::new(
        "UPDATE dbo.catga_flow_states SET flow_type = @P1, flow_type_key = @P2, status = @P3, version = @P4, \
           heartbeat_ms = @P5, payload = @P6, revision = revision + 1 \
         WHERE flow_key = @P7 AND flow_id = @P8 AND revision = @P9",
    );
    query.bind(next.flow_type());
    query.bind(type_key.as_slice());
    query.bind(status_code(next.status()));
    query.bind(next.version());
    query.bind(unix_millis(next.heartbeat())?);
    query.bind(frame.as_slice());
    query.bind(key.as_slice());
    query.bind(next.id());
    query.bind(revision);
    query
        .execute(connection)
        .await
        .map(|result| result.total() == 1)
        .map_err(|error| database_error("replace SQL Server flow state", error))
}

async fn backfill_flow_type_keys(
    connection: &mut bb8::PooledConnection<'_, bb8_tiberius::ConnectionManager>,
) -> CatgaResult<()> {
    let rows = connection
        .query(
            "SELECT flow_key, flow_type FROM dbo.catga_flow_states WHERE flow_type_key IS NULL",
            &[],
        )
        .await
        .map_err(|error| database_error("read SQL Server FlowStore type-key backfill", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("read SQL Server FlowStore type-key backfill", error))?;
    for row in rows {
        let row_key = required_bytes(&row, "flow_key", "SQL Server FlowStore type-key row")?;
        let type_key = flow_key(required_str(
            &row,
            "flow_type",
            "SQL Server FlowStore type-key flow type",
        )?);
        let mut update = Query::new(
            "UPDATE dbo.catga_flow_states SET flow_type_key = @P1 \
             WHERE flow_key = @P2 AND flow_type_key IS NULL",
        );
        update.bind(type_key.as_slice());
        update.bind(row_key);
        update
            .execute(connection)
            .await
            .map_err(|error| database_error("backfill SQL Server FlowStore type key", error))?;
    }
    Ok(())
}

async fn collision_result(
    connection: &mut bb8::PooledConnection<'_, bb8_tiberius::ConnectionManager>,
    key: &[u8; 32],
    id: &str,
) -> CatgaResult<bool> {
    let mut query = Query::new("SELECT flow_id FROM dbo.catga_flow_states WHERE flow_key = @P1");
    query.bind(key.as_slice());
    let stream = query
        .query(connection)
        .await
        .map_err(|error| database_error("read conflicting SQL Server flow state", error))?;
    let row = stream
        .into_row()
        .await
        .map_err(|error| database_error("read conflicting SQL Server flow state row", error))?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "SQL Server flow state disappeared after a conflicting create",
            )
        })?;
    if required_str(&row, "flow_id", "conflicting SQL Server flow identity")? == id {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL FlowStore identities",
        ))
    }
}

pub(crate) fn required_i64(row: &Row, column: &str, description: &'static str) -> CatgaResult<i64> {
    row.try_get(column)
        .map_err(|error| database_error(description, error))?
        .ok_or_else(|| missing_column(description))
}

pub(crate) fn required_str<'a>(
    row: &'a Row,
    column: &str,
    description: &'static str,
) -> CatgaResult<&'a str> {
    row.try_get(column)
        .map_err(|error| database_error(description, error))?
        .ok_or_else(|| missing_column(description))
}

pub(crate) fn required_bytes<'a>(
    row: &'a Row,
    column: &str,
    description: &'static str,
) -> CatgaResult<&'a [u8]> {
    row.try_get(column)
        .map_err(|error| database_error(description, error))?
        .ok_or_else(|| missing_column(description))
}

pub(crate) fn missing_column(description: &'static str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Internal,
        format!("{description} is unexpectedly NULL or absent"),
    )
}
