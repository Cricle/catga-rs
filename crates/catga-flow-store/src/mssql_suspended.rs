//! SQL Server persistence for durable Flow continuations.

use std::time::SystemTime;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowSummary, decode_continuation, encode_continuation,
};
use tiberius::Query;

use crate::{
    MssqlPool,
    error::database_error,
    key::flow_key,
    mssql::{missing_column, required_bytes, required_i64, required_str},
    sql_common::{
        MAX_CAS_RETRIES, cas_error, deadline_millis, status_code, status_from_code,
        system_time_from_unix_millis_and_subsec_nanos, unix_millis_and_subsec_nanos,
    },
};

struct StoredContinuation {
    continuation: FlowContinuation,
    revision: i64,
}

/// Creates the continuation table and its bounded discovery indexes.
pub(crate) async fn migrate(pool: &MssqlPool) -> CatgaResult<()> {
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server continuation migration", error))?;
    connection
        .execute(
            "IF OBJECT_ID(N'dbo.catga_flow_continuations', N'U') IS NULL BEGIN \
             CREATE TABLE dbo.catga_flow_continuations (\
               flow_key BINARY(32) NOT NULL PRIMARY KEY, flow_id NVARCHAR(MAX) NOT NULL, \
               flow_type NVARCHAR(MAX) NOT NULL, status BIGINT NOT NULL, version BIGINT NOT NULL, \
               created_at_ms BIGINT NOT NULL, created_at_subsec_ns BIGINT NOT NULL DEFAULT 0, \
               updated_at_ms BIGINT NOT NULL DEFAULT 0, updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0, \
               deadline_ms BIGINT NULL, wait_correlation NVARCHAR(MAX) NULL, \
               wait_correlation_key BINARY(32) NULL, revision BIGINT NOT NULL, \
               due_token BINARY(16) NULL, lease_until_ms BIGINT NULL, payload VARBINARY(MAX) NOT NULL); \
             CREATE INDEX catga_flow_continuations_query_idx ON dbo.catga_flow_continuations \
               (status, created_at_ms, flow_key); \
             CREATE INDEX catga_flow_continuations_order_idx ON dbo.catga_flow_continuations \
               (created_at_ms, created_at_subsec_ns, flow_key); \
             CREATE INDEX catga_flow_continuations_due_idx ON dbo.catga_flow_continuations \
               (deadline_ms, lease_until_ms, flow_key); \
             CREATE INDEX catga_flow_continuations_wait_correlation_idx ON dbo.catga_flow_continuations \
               (wait_correlation_key, flow_key); END; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'created_at_subsec_ns') IS NULL \
               ALTER TABLE dbo.catga_flow_continuations ADD created_at_subsec_ns BIGINT NOT NULL DEFAULT 0; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'updated_at_ms') IS NULL BEGIN \
               ALTER TABLE dbo.catga_flow_continuations ADD updated_at_ms BIGINT NOT NULL DEFAULT 0; \
               ALTER TABLE dbo.catga_flow_continuations ADD updated_at_subsec_ns BIGINT NOT NULL DEFAULT 0; \
               UPDATE dbo.catga_flow_continuations SET updated_at_ms = created_at_ms, updated_at_subsec_ns = created_at_subsec_ns; END; \
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND name = N'catga_flow_continuations_order_idx') \
               CREATE INDEX catga_flow_continuations_order_idx ON dbo.catga_flow_continuations \
                 (created_at_ms, created_at_subsec_ns, flow_key); \
             DECLARE @drop_continuation_id_unique nvarchar(max) = N''; \
             SELECT @drop_continuation_id_unique += N'ALTER TABLE dbo.catga_flow_continuations DROP CONSTRAINT ' \
               + QUOTENAME(key_constraint.name) + N';' \
             FROM sys.key_constraints AS key_constraint \
             INNER JOIN sys.index_columns AS index_column \
               ON index_column.object_id = key_constraint.parent_object_id \
               AND index_column.index_id = key_constraint.unique_index_id \
             WHERE key_constraint.parent_object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND key_constraint.type = N'UQ' \
               AND COL_NAME(index_column.object_id, index_column.column_id) = N'flow_id'; \
             IF @drop_continuation_id_unique <> N'' EXEC sys.sp_executesql @drop_continuation_id_unique; \
             IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND name = N'catga_flow_continuations_query_idx') \
               DROP INDEX catga_flow_continuations_query_idx ON dbo.catga_flow_continuations; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'flow_id') <> -1 \
               ALTER TABLE dbo.catga_flow_continuations ALTER COLUMN flow_id NVARCHAR(MAX) NOT NULL; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'flow_type') <> -1 \
               ALTER TABLE dbo.catga_flow_continuations ALTER COLUMN flow_type NVARCHAR(MAX) NOT NULL; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'wait_correlation') IS NULL \
               ALTER TABLE dbo.catga_flow_continuations ADD wait_correlation NVARCHAR(MAX) NULL; \
             IF COL_LENGTH(N'dbo.catga_flow_continuations', N'wait_correlation_key') IS NULL \
               ALTER TABLE dbo.catga_flow_continuations ADD wait_correlation_key BINARY(32) NULL; \
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND name = N'catga_flow_continuations_query_idx') \
               CREATE INDEX catga_flow_continuations_query_idx ON dbo.catga_flow_continuations \
                 (status, created_at_ms, flow_key); \
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND name = N'catga_flow_continuations_due_idx') \
               CREATE INDEX catga_flow_continuations_due_idx ON dbo.catga_flow_continuations \
                 (deadline_ms, lease_until_ms, flow_key); \
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_continuations') \
               AND name = N'catga_flow_continuations_wait_correlation_idx') \
               CREATE INDEX catga_flow_continuations_wait_correlation_idx ON dbo.catga_flow_continuations \
                 (wait_correlation_key, flow_key);",
            &[],
        )
        .await
        .map(|_| ())
        .map_err(|error| database_error("create SQL Server continuation schema", error))
}

/// Inserts a continuation without replacing an existing identity.
pub(crate) async fn create(pool: &MssqlPool, continuation: FlowContinuation) -> CatgaResult<bool> {
    let key = flow_key(continuation.state().id());
    let frame = encode_continuation(&continuation)?;
    let deadline = deadline_millis(&continuation)?;
    let (created_at_ms, created_at_subsec_ns) =
        unix_millis_and_subsec_nanos(continuation.created_at())?;
    let (updated_at_ms, updated_at_subsec_ns) =
        unix_millis_and_subsec_nanos(continuation.updated_at())?;
    let mut query = Query::new(
        "IF NOT EXISTS (SELECT 1 FROM dbo.catga_flow_continuations WITH (UPDLOCK, HOLDLOCK) \
           WHERE flow_key = @P1) BEGIN \
           INSERT INTO dbo.catga_flow_continuations \
             (flow_key, flow_id, flow_type, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns, deadline_ms, \
              wait_correlation, wait_correlation_key, revision, payload) \
           VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12, 0, @P13); \
           SELECT CAST(1 AS BIGINT) AS inserted; END \
         ELSE SELECT CAST(0 AS BIGINT) AS inserted;",
    );
    query.bind(key.as_slice());
    query.bind(continuation.state().id());
    query.bind(continuation.state().flow_type());
    query.bind(status_code(continuation.state().status()));
    query.bind(continuation.state().version());
    query.bind(created_at_ms);
    query.bind(created_at_subsec_ns);
    query.bind(updated_at_ms);
    query.bind(updated_at_subsec_ns);
    query.bind(deadline);
    query.bind(wait_correlation(&continuation));
    query.bind(wait_correlation_key(&continuation).map(|key| key.to_vec()));
    query.bind(frame.as_slice());
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server continuation create", error))?;
    let stream = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("create SQL Server continuation", error))?;
    let row = stream
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server continuation create result", error))?
        .ok_or_else(|| missing_column("SQL Server continuation create result"))?;
    if required_i64(&row, "inserted", "SQL Server continuation create result")? == 1 {
        return Ok(true);
    }
    let mut query =
        Query::new("SELECT flow_id FROM dbo.catga_flow_continuations WHERE flow_key = @P1");
    query.bind(key.as_slice());
    let stream = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read conflicting SQL Server continuation", error))?;
    let row = stream
        .into_row()
        .await
        .map_err(|error| database_error("read conflicting SQL Server continuation", error))?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "SQL Server continuation disappeared after conflicting create",
            )
        })?;
    if required_str(
        &row,
        "flow_id",
        "conflicting SQL Server continuation identity",
    )? == continuation.state().id()
    {
        Ok(false)
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL continuation identities",
        ))
    }
}

/// Loads one continuation by identity.
pub(crate) async fn get(pool: &MssqlPool, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
    load(pool, flow_id)
        .await
        .map(|value| value.map(|value| value.continuation))
}

/// Loads exactly one continuation by its indexed active wait correlation.
pub(crate) async fn get_by_wait_correlation(
    pool: &MssqlPool,
    correlation_id: &str,
) -> CatgaResult<Option<FlowContinuation>> {
    let correlation_key = flow_key(correlation_id);
    let mut query = Query::new(
        "SELECT TOP (2) payload FROM dbo.catga_flow_continuations \
         WHERE wait_correlation_key = @P1 AND wait_correlation = @P2 \
         ORDER BY flow_key ASC",
    );
    query.bind(correlation_key.as_slice());
    query.bind(correlation_id);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server wait correlation read", error))?;
    let rows = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read SQL Server wait correlation", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("read SQL Server wait correlation rows", error))?;
    if rows.len() > 1 {
        return Err(CatgaError::new(
            ErrorCode::Conflict,
            "flow wait correlation identifies multiple active flows",
        ));
    }
    rows.into_iter()
        .next()
        .map(|row| {
            let frame = required_bytes(&row, "payload", "SQL Server wait correlation frame")?;
            let continuation = decode_continuation(frame)?;
            if continuation
                .wait()
                .is_some_and(|wait| wait.correlation_id() == correlation_id)
            {
                Ok(continuation)
            } else {
                Err(CatgaError::new(
                    ErrorCode::Internal,
                    "SQL Server wait correlation index does not match its continuation frame",
                ))
            }
        })
        .transpose()
}

/// Returns matching summaries after fetching at most the configured scan bound.
pub(crate) async fn query(pool: &MssqlPool, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
    let limit = i64::try_from(query.max_scan()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQL Server continuation scan limit exceeds i64",
        )
    })?;
    let mut next_parameter: usize = 2;
    let mut template = String::from(
        "SELECT TOP (@P1) flow_id, flow_type, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns \
         FROM dbo.catga_flow_continuations WHERE 1 = 1",
    );
    if query.status().is_some() {
        template.push_str(&format!(" AND status = @P{next_parameter}"));
        next_parameter = next_parameter.saturating_add(1);
    }
    if query.flow_type().is_some() {
        template.push_str(&format!(" AND flow_type = @P{next_parameter}"));
        next_parameter = next_parameter.saturating_add(1);
    }
    let created_range = query
        .created_at_range()
        .map(|(start, end)| {
            Ok::<_, CatgaError>((
                unix_millis_and_subsec_nanos(start)?,
                unix_millis_and_subsec_nanos(end)?,
            ))
        })
        .transpose()?;
    if created_range.is_some() {
        let start_ms = next_parameter;
        let start_subsec_ns = next_parameter.saturating_add(1);
        let end_ms = next_parameter.saturating_add(2);
        let end_subsec_ns = next_parameter.saturating_add(3);
        template.push_str(&format!(
            " AND (created_at_ms > @P{start_ms} OR (created_at_ms = @P{start_ms} AND created_at_subsec_ns >= @P{start_subsec_ns})) \
             AND (created_at_ms < @P{end_ms} OR (created_at_ms = @P{end_ms} AND created_at_subsec_ns < @P{end_subsec_ns}))"
        ));
    }
    template.push_str(" ORDER BY created_at_ms ASC, created_at_subsec_ns ASC, flow_key ASC");
    let mut statement = Query::new(template);
    statement.bind(limit);
    if let Some(status) = query.status() {
        statement.bind(status_code(status));
    }
    if let Some(flow_type) = query.flow_type() {
        statement.bind(flow_type);
    }
    if let Some(((start_ms, start_subsec_ns), (end_ms, end_subsec_ns))) = created_range {
        statement.bind(start_ms);
        statement.bind(start_subsec_ns);
        statement.bind(end_ms);
        statement.bind(end_subsec_ns);
    }
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server continuation query", error))?;
    let rows = statement
        .query(&mut connection)
        .await
        .map_err(|error| database_error("query SQL Server continuations", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("read SQL Server continuation query", error))?;
    let mut summaries = Vec::with_capacity(query.max_results());
    for row in rows {
        let id = required_str(&row, "flow_id", "SQL Server summary identity")?;
        let flow_type = required_str(&row, "flow_type", "SQL Server summary flow type")?;
        let status = required_i64(&row, "status", "SQL Server summary status")?;
        let version = required_i64(&row, "version", "SQL Server summary version")?;
        let created_at = required_i64(&row, "created_at_ms", "SQL Server summary creation time")?;
        let created_at_subsec_ns = required_i64(
            &row,
            "created_at_subsec_ns",
            "SQL Server summary creation precision",
        )?;
        let updated_at = required_i64(&row, "updated_at_ms", "SQL Server summary update time")?;
        let updated_at_subsec_ns = required_i64(
            &row,
            "updated_at_subsec_ns",
            "SQL Server summary update precision",
        )?;
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

/// Deletes a continuation only while its version and physical revision remain current.
pub(crate) async fn delete(
    pool: &MssqlPool,
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
        let mut query = Query::new(
            "DELETE FROM dbo.catga_flow_continuations \
             WHERE flow_key = @P1 AND flow_id = @P2 AND revision = @P3",
        );
        query.bind(key.as_slice());
        query.bind(flow_id);
        query.bind(current.revision);
        let mut connection = pool
            .get()
            .await
            .map_err(|error| database_error("acquire SQL Server continuation delete", error))?;
        let changed = query
            .execute(&mut connection)
            .await
            .map_err(|error| database_error("delete SQL Server continuation", error))?
            .total();
        if changed == 1 {
            return Ok(true);
        }
    }
    Err(cas_error("delete a SQL Server continuation"))
}

/// Replaces a continuation after exactly one business-version transition.
pub(crate) async fn update(
    pool: &MssqlPool,
    expected_version: i64,
    next: FlowContinuation,
) -> CatgaResult<bool> {
    if next.state().version() != expected_version.saturating_add(1) {
        return Ok(false);
    }
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
    Err(cas_error("update a SQL Server continuation"))
}

/// Claims only when the complete expected snapshot remains current.
pub(crate) async fn claim(
    pool: &MssqlPool,
    expected: &FlowContinuation,
    next: FlowContinuation,
) -> CatgaResult<bool> {
    if next.state().id() != expected.state().id()
        || next.state().version() != expected.state().version().saturating_add(1)
    {
        return Ok(false);
    }
    let Some(current) = load(pool, expected.state().id()).await? else {
        return Ok(false);
    };
    if current.continuation != *expected {
        return Ok(false);
    }
    replace(pool, &current, &next).await
}

/// Records one idempotent child success through bounded revision CAS.
pub(crate) async fn record_wait_success(
    pool: &MssqlPool,
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
    Err(cas_error("record a SQL Server wait result"))
}

/// Records one idempotent child failure through bounded revision CAS.
pub(crate) async fn record_wait_failure(
    pool: &MssqlPool,
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
    Err(cas_error("record a failed SQL Server wait result"))
}

/// Refreshes owner liveness without changing business version.
pub(crate) async fn heartbeat(
    pool: &MssqlPool,
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
        let next = current.continuation.clone().with_state(
            current
                .continuation
                .state()
                .clone()
                .heartbeated_at(SystemTime::now()),
        );
        if replace(pool, &current, &next).await? {
            return Ok(true);
        }
    }
    Err(cas_error("heartbeat a SQL Server continuation"))
}

async fn load(pool: &MssqlPool, flow_id: &str) -> CatgaResult<Option<StoredContinuation>> {
    let key = flow_key(flow_id);
    let mut query = Query::new(
        "SELECT payload, revision FROM dbo.catga_flow_continuations \
         WHERE flow_key = @P1 AND flow_id = @P2",
    );
    query.bind(key.as_slice());
    query.bind(flow_id);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server continuation read", error))?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("read SQL Server continuation", error))?
        .into_row()
        .await
        .map_err(|error| database_error("read SQL Server continuation row", error))?;
    row.map(|row| {
        let frame = required_bytes(&row, "payload", "SQL Server continuation frame")?;
        let continuation = decode_continuation(frame)?;
        if continuation.state().id() != flow_id {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "SQL Server continuation identity does not match its frame",
            ));
        }
        let revision = required_i64(&row, "revision", "SQL Server continuation revision")?;
        Ok(StoredContinuation {
            continuation,
            revision,
        })
    })
    .transpose()
}

async fn replace(
    pool: &MssqlPool,
    current: &StoredContinuation,
    next: &FlowContinuation,
) -> CatgaResult<bool> {
    let key = flow_key(next.state().id());
    let frame = encode_continuation(next)?;
    let deadline = deadline_millis(next)?;
    let (created_at_ms, created_at_subsec_ns) = unix_millis_and_subsec_nanos(next.created_at())?;
    let (updated_at_ms, updated_at_subsec_ns) = unix_millis_and_subsec_nanos(next.updated_at())?;
    let mut query = Query::new(
        "UPDATE dbo.catga_flow_continuations SET flow_type = @P1, status = @P2, \
           version = @P3, created_at_ms = @P4, created_at_subsec_ns = @P5, updated_at_ms = @P6, updated_at_subsec_ns = @P7, deadline_ms = @P8, \
           wait_correlation = @P9, wait_correlation_key = @P10, payload = @P11, \
           revision = revision + 1, due_token = NULL, lease_until_ms = NULL \
         WHERE flow_key = @P12 AND flow_id = @P13 AND revision = @P14",
    );
    query.bind(next.state().flow_type());
    query.bind(status_code(next.state().status()));
    query.bind(next.state().version());
    query.bind(created_at_ms);
    query.bind(created_at_subsec_ns);
    query.bind(updated_at_ms);
    query.bind(updated_at_subsec_ns);
    query.bind(deadline);
    query.bind(wait_correlation(next));
    query.bind(wait_correlation_key(next).map(|key| key.to_vec()));
    query.bind(frame.as_slice());
    query.bind(key.as_slice());
    query.bind(next.state().id());
    query.bind(current.revision);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server continuation update", error))?;
    query
        .execute(&mut connection)
        .await
        .map(|result| result.total() == 1)
        .map_err(|error| database_error("replace SQL Server continuation", error))
}

fn wait_correlation(continuation: &FlowContinuation) -> Option<&str> {
    continuation.wait().map(|wait| wait.correlation_id())
}

fn wait_correlation_key(continuation: &FlowContinuation) -> Option<[u8; 32]> {
    wait_correlation(continuation).map(flow_key)
}
