//! Atomic SQL Server receipt leasing for expired durable waits.

use std::time::Duration;

use catga_core::flow::{TimedOutFlowPoll, TimedOutFlowReceipt};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tiberius::Query;

use crate::{
    MssqlPool,
    error::database_error,
    key::flow_key,
    mssql::{required_bytes, required_str},
    sql_common::unix_millis,
};

const RECEIPT_LEASE: Duration = Duration::from_secs(30);

/// Atomically leases a bounded ordered page using row-level skip-locked semantics.
pub(crate) async fn poll(
    pool: &MssqlPool,
    poll: &TimedOutFlowPoll,
) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
    let now = unix_millis(poll.now())?;
    let lease = i64::try_from(RECEIPT_LEASE.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQL Server timeout lease exceeds signed milliseconds",
        )
    })?;
    let lease_until = now.checked_add(lease).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQL Server timeout receipt deadline overflows",
        )
    })?;
    let candidate_limit = poll.limit().min(poll.scan_limit());
    let limit = i64::try_from(candidate_limit).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQL Server timeout candidate limit exceeds i64",
        )
    })?;
    let mut query = Query::new(
        ";WITH due AS (\
           SELECT TOP (@P3) flow_id, due_token, lease_until_ms, revision \
           FROM dbo.catga_flow_continuations WITH (UPDLOCK, READPAST, ROWLOCK, \
             INDEX(catga_flow_continuations_due_idx)) \
           WHERE deadline_ms IS NOT NULL AND deadline_ms <= @P1 \
             AND (due_token IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= @P1) \
           ORDER BY deadline_ms ASC, flow_key ASC) \
         UPDATE due SET due_token = CONVERT(BINARY(16), NEWID()), lease_until_ms = @P2, \
           revision = revision + 1 \
         OUTPUT inserted.flow_id, inserted.due_token;",
    );
    query.bind(now);
    query.bind(lease_until);
    query.bind(limit);
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server timeout poll connection", error))?;
    let rows = query
        .query(&mut connection)
        .await
        .map_err(|error| database_error("poll SQL Server timed-out continuations", error))?
        .into_first_result()
        .await
        .map_err(|error| database_error("read SQL Server timeout receipts", error))?;
    rows.into_iter()
        .map(|row| {
            let flow_id = required_str(&row, "flow_id", "SQL Server timeout flow identity")?;
            let token = required_bytes(&row, "due_token", "SQL Server timeout token")?;
            Ok(TimedOutFlowReceipt::new(flow_id, token))
        })
        .collect()
}

/// Acknowledges only the currently fenced receipt token.
pub(crate) async fn acknowledge(
    pool: &MssqlPool,
    receipt: &TimedOutFlowReceipt,
) -> CatgaResult<()> {
    settle(pool, receipt, true).await
}

/// Releases only the currently fenced receipt token.
pub(crate) async fn release(pool: &MssqlPool, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    settle(pool, receipt, false).await
}

async fn settle(
    pool: &MssqlPool,
    receipt: &TimedOutFlowReceipt,
    acknowledge: bool,
) -> CatgaResult<()> {
    validate_token(receipt)?;
    let key = flow_key(receipt.flow_id());
    let sql = if acknowledge {
        "UPDATE dbo.catga_flow_continuations SET deadline_ms = NULL, due_token = NULL, \
           lease_until_ms = NULL, revision = revision + 1 \
         WHERE flow_key = @P1 AND flow_id = @P2 AND due_token = @P3"
    } else {
        "UPDATE dbo.catga_flow_continuations SET due_token = NULL, lease_until_ms = NULL, \
           revision = revision + 1 \
         WHERE flow_key = @P1 AND flow_id = @P2 AND due_token = @P3"
    };
    let mut query = Query::new(sql);
    query.bind(key.as_slice());
    query.bind(receipt.flow_id());
    query.bind(receipt.token());
    let mut connection = pool
        .get()
        .await
        .map_err(|error| database_error("acquire SQL Server timeout settlement", error))?;
    query
        .execute(&mut connection)
        .await
        .map(|_| ())
        .map_err(|error| database_error("settle SQL Server timeout receipt", error))
}

fn validate_token(receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    if receipt.token().len() == 16 {
        Ok(())
    } else {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "SQL Server timeout receipt token is invalid",
        ))
    }
}
