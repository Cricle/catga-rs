//! SQL Server statements for durable DSL step progress.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::DslStepProgress;
use tiberius::Query;

use crate::{
    MssqlPool,
    dsl_progress_codec::{advances_version, decode_progress, encode_progress, validate_progress},
    error::database_error,
    key::flow_key,
    mssql::{missing_column, required_bytes, required_i64, required_str},
    sql_common::{MAX_CAS_RETRIES, cas_error},
};

struct StoredProgress {
    progress: DslStepProgress,
    revision: i64,
}

/// Creates the SQL Server DSL step-progress table.
pub(crate) async fn migrate(pool: &MssqlPool) -> CatgaResult<()> {
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server DSL progress migration", error))?;
    connection
        .execute(
            "BEGIN TRY \
               BEGIN TRANSACTION; \
               DECLARE @result INT; \
               EXEC @result = sys.sp_getapplock @Resource = N'catga_dsl_step_progress_schema', \
                 @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 5000; \
               IF @result < 0 THROW 50000, 'could not acquire the Catga DSL progress schema lock', 1; \
               IF OBJECT_ID(N'dbo.catga_dsl_step_progress', N'U') IS NULL BEGIN \
                 CREATE TABLE dbo.catga_dsl_step_progress (\
                 flow_key BINARY(32) NOT NULL, flow_id NVARCHAR(MAX) NOT NULL, \
                 step_index BIGINT NOT NULL, version BIGINT NOT NULL, revision BIGINT NOT NULL, \
                 payload VARBINARY(MAX) NOT NULL, \
                 CONSTRAINT PK_catga_dsl_step_progress PRIMARY KEY(flow_key, step_index)); \
               END; \
               COMMIT TRANSACTION; \
             END TRY \
             BEGIN CATCH \
               IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; \
               THROW; \
             END CATCH;",
            &[],
        )
        .await
        .map(|_| ())
        .map_err(|error| database_error("create SQL Server DSL step-progress table", error))
}

/// Inserts progress without using SQL Server `MERGE`.
pub(crate) async fn create(pool: &MssqlPool, progress: DslStepProgress) -> CatgaResult<bool> {
    validate_progress(&progress)?;
    let key = flow_key(progress.flow_id());
    let mut query = Query::new(
        "IF NOT EXISTS (SELECT 1 FROM dbo.catga_dsl_step_progress WITH (UPDLOCK, HOLDLOCK) \
         WHERE flow_key = @P1 AND step_index = @P2) BEGIN \
         INSERT INTO dbo.catga_dsl_step_progress \
         (flow_key, flow_id, step_index, version, revision, payload) \
         VALUES (@P1, @P3, @P2, @P4, 0, @P5); \
         SELECT CAST(1 AS BIGINT) AS inserted; END \
         ELSE SELECT CAST(0 AS BIGINT) AS inserted;",
    );
    query.bind(key.as_slice());
    query.bind(i64::from(progress.step_index()));
    query.bind(progress.flow_id());
    query.bind(progress.version());
    let frame = encode_progress(&progress)?;
    query.bind(frame.as_slice());
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server DSL progress create connection", error)
    })?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("create SQL Server DSL step progress", error))?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server DSL progress create result", error))?
        .ok_or_else(|| missing_column("SQL Server DSL progress create result row"))?;
    if required_i64(&row, "inserted", "SQL Server DSL progress create result")? == 1 {
        return Ok(true);
    }
    conflict_result(
        &mut connection,
        &key,
        progress.step_index(),
        progress.flow_id(),
    )
    .await
}

/// Loads progress for one raw flow-step identity.
pub(crate) async fn get(
    pool: &MssqlPool,
    flow_id: &str,
    step_index: u32,
) -> CatgaResult<Option<DslStepProgress>> {
    load(pool, flow_id, step_index)
        .await
        .map(|stored| stored.map(|stored| stored.progress))
}

/// Replaces one expected logical version through bounded physical-revision CAS retries.
pub(crate) async fn update(
    pool: &MssqlPool,
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
    Err(cas_error("update SQL Server DSL step progress"))
}

/// Deletes progress through bounded physical-revision CAS retries.
pub(crate) async fn delete(pool: &MssqlPool, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
    for _ in 0..MAX_CAS_RETRIES {
        let Some(current) = load(pool, flow_id, step_index).await? else {
            return Ok(false);
        };
        let key = flow_key(flow_id);
        let mut query = Query::new(
            "DELETE FROM dbo.catga_dsl_step_progress \
             WHERE flow_key = @P1 AND flow_id = @P2 AND step_index = @P3 AND revision = @P4",
        );
        query.bind(key.as_slice());
        query.bind(flow_id);
        query.bind(i64::from(step_index));
        query.bind(current.revision);
        let mut connection = pool.get().await.map_err(|error| {
            database_error("acquire SQL Server DSL progress delete connection", error)
        })?;
        if query
            .execute(&mut connection)
            .await
            .map_err(|error| database_error("delete SQL Server DSL step progress", error))?
            .total()
            == 1
        {
            return Ok(true);
        }
    }
    Err(cas_error("delete SQL Server DSL step progress"))
}

async fn load(
    pool: &MssqlPool,
    flow_id: &str,
    step_index: u32,
) -> CatgaResult<Option<StoredProgress>> {
    let key = flow_key(flow_id);
    let mut query = Query::new(
        "SELECT version, revision, payload FROM dbo.catga_dsl_step_progress \
         WHERE flow_key = @P1 AND flow_id = @P2 AND step_index = @P3",
    );
    query.bind(key.as_slice());
    query.bind(flow_id);
    query.bind(i64::from(step_index));
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server DSL progress read connection", error)
    })?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read SQL Server DSL step progress", error))?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server DSL step progress row", error))?;
    row.map(|row| decode_row(&row, flow_id, step_index))
        .transpose()
}

async fn replace(
    pool: &MssqlPool,
    current: &StoredProgress,
    next: &DslStepProgress,
) -> CatgaResult<bool> {
    let key = flow_key(next.flow_id());
    let frame = encode_progress(next)?;
    let mut query = Query::new(
        "UPDATE dbo.catga_dsl_step_progress SET version = @P1, payload = @P2, \
         revision = revision + 1 WHERE flow_key = @P3 AND flow_id = @P4 \
         AND step_index = @P5 AND revision = @P6",
    );
    query.bind(next.version());
    query.bind(frame.as_slice());
    query.bind(key.as_slice());
    query.bind(next.flow_id());
    query.bind(i64::from(next.step_index()));
    query.bind(current.revision);
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server DSL progress update connection", error)
    })?;
    query
        .execute(&mut connection)
        .await
        .map(|result| result.total() == 1)
        .map_err(|error| database_error("replace SQL Server DSL step progress", error))
}

fn decode_row(row: &tiberius::Row, flow_id: &str, step_index: u32) -> CatgaResult<StoredProgress> {
    let version = required_i64(row, "version", "SQL Server DSL progress version")?;
    let revision = required_i64(row, "revision", "SQL Server DSL progress revision")?;
    let progress = decode_progress(required_bytes(
        row,
        "payload",
        "SQL Server DSL progress frame",
    )?)?;
    if progress.flow_id() != flow_id
        || progress.step_index() != step_index
        || progress.version() != version
    {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SQL Server DSL step-progress row does not match its frame",
        ));
    }
    Ok(StoredProgress { progress, revision })
}

async fn conflict_result(
    connection: &mut bb8::PooledConnection<'_, bb8_tiberius::ConnectionManager>,
    key: &[u8; 32],
    step_index: u32,
    flow_id: &str,
) -> CatgaResult<bool> {
    let mut query = Query::new(
        "SELECT flow_id FROM dbo.catga_dsl_step_progress WHERE flow_key = @P1 AND step_index = @P2",
    );
    query.bind(key.as_slice());
    query.bind(i64::from(step_index));
    let row = query
        .query(connection)
        .await
        .map_err(|error| database_error("read conflicting SQL Server DSL step progress", error))?
        .into_row()
        .await
        .map_err(|error| {
            database_error("read conflicting SQL Server DSL step progress row", error)
        })?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "SQL Server DSL step progress disappeared after a conflicting create",
            )
        })?;
    if required_str(
        &row,
        "flow_id",
        "conflicting SQL Server DSL progress identity",
    )? == flow_id
    {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL DSL progress identities",
        ))
    }
}
