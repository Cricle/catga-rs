//! SQL Server durable Flow-resume scheduling.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::ScheduledResume;
use tiberius::{Query, Row};

use crate::{
    MssqlPool,
    error::database_error,
    key::schedule_target_key,
    mssql::{required_i64, required_str},
    scheduler_common::{claim_times, current_millis, schedule_times},
    sql_common::system_time_from_unix_millis_and_subsec_nanos,
};

pub(crate) async fn migrate(pool: &MssqlPool) -> CatgaResult<()> {
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server scheduler migration connection", error)
    })?;
    connection
        .execute(
            "IF OBJECT_ID(N'dbo.catga_flow_schedules', N'U') IS NULL BEGIN \
             CREATE TABLE dbo.catga_flow_schedules (\
               schedule_id NVARCHAR(36) NOT NULL PRIMARY KEY, target_key BINARY(32) NOT NULL UNIQUE, \
               flow_id NVARCHAR(MAX) NOT NULL, state_id NVARCHAR(MAX) NOT NULL, \
               due_at_ms BIGINT NOT NULL, due_at_subsec_ns BIGINT NOT NULL, \
               lease_owner NVARCHAR(MAX) NULL, lease_until_ms BIGINT NULL); \
             CREATE INDEX catga_flow_schedules_due_idx ON dbo.catga_flow_schedules \
               (due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id); END; \
             IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.catga_flow_schedules') \
               AND name = N'catga_flow_schedules_due_idx') \
               CREATE INDEX catga_flow_schedules_due_idx ON dbo.catga_flow_schedules \
                 (due_at_ms, due_at_subsec_ns, lease_until_ms, schedule_id);",
            &[],
        )
        .await
        .map(|_| ())
        .map_err(|error| database_error("create SQL Server scheduler table", error))
}

pub(crate) async fn schedule_resume(
    pool: &MssqlPool,
    flow_id: &str,
    state_id: &str,
    due_at: SystemTime,
) -> CatgaResult<Box<str>> {
    let target_key = schedule_target_key(flow_id, state_id);
    let (due_at_ms, due_at_subsec_ns) = schedule_times(due_at)?;
    let schedule_id = uuid::Uuid::new_v4().to_string();
    let mut query = Query::new(
        "IF NOT EXISTS (SELECT 1 FROM dbo.catga_flow_schedules WITH (UPDLOCK, HOLDLOCK) \
           WHERE target_key = @P1) BEGIN \
           INSERT INTO dbo.catga_flow_schedules \
             (schedule_id, target_key, flow_id, state_id, due_at_ms, due_at_subsec_ns) \
           VALUES (@P2, @P1, @P3, @P4, @P5, @P6); END; \
         SELECT schedule_id, flow_id, state_id FROM dbo.catga_flow_schedules WHERE target_key = @P1;",
    );
    query.bind(target_key.as_slice());
    query.bind(&schedule_id);
    query.bind(flow_id);
    query.bind(state_id);
    query.bind(due_at_ms);
    query.bind(due_at_subsec_ns);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server schedule connection", error))?;
    connection
        .simple_query("BEGIN TRANSACTION")
        .await
        .map_err(|error| database_error("begin SQL Server schedule creation", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("begin SQL Server schedule creation", error))?;
    let result = async {
        let row = query
            .query(&mut connection)
            .await
            .map_err(|error| database_error("schedule SQL Server flow resume", error))?
            .into_row()
            .await
            .map_err(|error| database_error("read SQL Server scheduled flow resume", error))?
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "SQL Server scheduled flow resume disappeared after creation conflict",
                )
            })?;
        verify_target(&row, flow_id, state_id)?;
        required_str(&row, "schedule_id", "SQL Server schedule identity").map(Into::into)
    }
    .await;
    match result {
        Ok(schedule_id) => {
            connection
                .simple_query("COMMIT TRANSACTION")
                .await
                .map_err(|error| database_error("commit SQL Server schedule creation", error))?
                .into_first_result()
                .await
                .map_err(|error| database_error("commit SQL Server schedule creation", error))?;
            Ok(schedule_id)
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

pub(crate) async fn cancel_resume(pool: &MssqlPool, schedule_id: &str) -> CatgaResult<bool> {
    let now = current_millis()?;
    let mut query = Query::new(
        "DELETE FROM dbo.catga_flow_schedules WHERE schedule_id = @P1 \
         AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= @P2); \
         SELECT CAST(@@ROWCOUNT AS BIGINT) AS changed;",
    );
    query.bind(schedule_id);
    query.bind(now);
    changed(pool, query, "cancel SQL Server flow resume").await
}

pub(crate) async fn claim_due(
    pool: &MssqlPool,
    owner: &str,
    now: SystemTime,
    lease_for: Duration,
    limit: usize,
) -> CatgaResult<Vec<ScheduledResume>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
    let (_, now_subsec_ns) = schedule_times(now)?;
    let limit = i64::try_from(limit).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQL Server schedule claim limit exceeds i64",
        )
    })?;
    let mut query = Query::new(
        ";WITH due AS (SELECT TOP (@P5) schedule_id, flow_id, state_id, due_at_ms, due_at_subsec_ns, lease_owner, lease_until_ms \
           FROM dbo.catga_flow_schedules WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK, ROWLOCK, INDEX(catga_flow_schedules_due_idx)) \
           WHERE (due_at_ms < @P3 OR (due_at_ms = @P3 AND due_at_subsec_ns <= @P4)) \
             AND (lease_owner IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= @P3) \
           ORDER BY due_at_ms ASC, due_at_subsec_ns ASC, schedule_id ASC) \
         UPDATE due SET lease_owner = @P1, lease_until_ms = @P2 \
         OUTPUT inserted.schedule_id, inserted.flow_id, inserted.state_id, inserted.due_at_ms, inserted.due_at_subsec_ns;",
    );
    query.bind(owner);
    query.bind(lease_until_ms);
    query.bind(now_ms);
    query.bind(now_subsec_ns);
    query.bind(limit);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server schedule claim connection", error))?;
    let rows = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("claim SQL Server due flow resumes", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("read SQL Server due flow resumes", error))?;
    rows.into_iter().map(decode_resume).collect()
}

pub(crate) async fn ack_due(pool: &MssqlPool, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
    let mut query = Query::new(
        "DELETE FROM dbo.catga_flow_schedules WHERE schedule_id = @P1 AND lease_owner = @P2; \
         SELECT CAST(@@ROWCOUNT AS BIGINT) AS changed;",
    );
    query.bind(schedule_id);
    query.bind(owner);
    changed(pool, query, "acknowledge SQL Server due flow resume").await
}

pub(crate) async fn release_due(
    pool: &MssqlPool,
    owner: &str,
    schedule_id: &str,
) -> CatgaResult<bool> {
    let mut query = Query::new(
        "UPDATE dbo.catga_flow_schedules SET lease_owner = NULL, lease_until_ms = NULL \
         WHERE schedule_id = @P1 AND lease_owner = @P2; \
         SELECT CAST(@@ROWCOUNT AS BIGINT) AS changed;",
    );
    query.bind(schedule_id);
    query.bind(owner);
    changed(pool, query, "release SQL Server due flow resume").await
}

pub(crate) async fn renew_due(
    pool: &MssqlPool,
    owner: &str,
    schedule_id: &str,
    now: SystemTime,
    lease_for: Duration,
) -> CatgaResult<bool> {
    let (now_ms, lease_until_ms) = claim_times(now, lease_for)?;
    let mut query = Query::new(
        "UPDATE dbo.catga_flow_schedules SET lease_until_ms = @P1 \
         WHERE schedule_id = @P2 AND lease_owner = @P3 AND lease_until_ms > @P4; \
         SELECT CAST(@@ROWCOUNT AS BIGINT) AS changed;",
    );
    query.bind(lease_until_ms);
    query.bind(schedule_id);
    query.bind(owner);
    query.bind(now_ms);
    changed(pool, query, "renew SQL Server due flow resume").await
}

fn verify_target(row: &Row, flow_id: &str, state_id: &str) -> CatgaResult<()> {
    if required_str(row, "flow_id", "SQL Server schedule flow identity")? == flow_id
        && required_str(row, "state_id", "SQL Server schedule state identity")? == state_id
    {
        Ok(())
    } else {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "SHA-256 collision between SQL schedule targets",
        ))
    }
}

fn decode_resume(row: Row) -> CatgaResult<ScheduledResume> {
    Ok(ScheduledResume::new(
        required_str(&row, "schedule_id", "SQL Server schedule identity")?,
        required_str(&row, "flow_id", "SQL Server schedule flow identity")?,
        required_str(&row, "state_id", "SQL Server schedule state identity")?,
        system_time_from_unix_millis_and_subsec_nanos(
            required_i64(&row, "due_at_ms", "SQL Server schedule due milliseconds")?,
            required_i64(
                &row,
                "due_at_subsec_ns",
                "SQL Server schedule due precision",
            )?,
        )?,
    ))
}

async fn changed<'a>(pool: &MssqlPool, query: Query<'a>, operation: &str) -> CatgaResult<bool> {
    let mut connection = pool.get().await.map_err(|error| {
        database_error("acquire SQL Server schedule settlement connection", error)
    })?;
    let row = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error(operation, error))?
        .into_row()
        .await
        .map_err(|error| database_error(operation, error))?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "SQL Server schedule settlement returned no row",
            )
        })?;
    Ok(required_i64(&row, "changed", "SQL Server schedule settlement result")? == 1)
}
