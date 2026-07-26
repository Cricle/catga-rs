//! Atomic SQLite receipt leasing for expired durable Flow waits.
//!
//! Timeout candidates live in the continuation table so a continuation compare-and-set can
//! invalidate an outstanding receipt in the same row update. Polling uses one
//! `UPDATE ... RETURNING` statement: candidate selection and lease acquisition therefore happen
//! under SQLite's write lock, preventing concurrent pollers from receiving the same lease.
//! Settlement is fenced by both the flow identity and an opaque random token. A delayed worker
//! consequently cannot acknowledge or release a lease that has already expired and been
//! reassigned.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{TimedOutFlowPoll, TimedOutFlowReceipt};
use sqlx::{Row, SqlitePool};

use crate::{error::database_error, key::flow_key};

/// Duration for which one timeout receipt belongs exclusively to its poller.
///
/// The lease is deliberately finite so process termination cannot strand a due continuation.
const RECEIPT_LEASE: Duration = Duration::from_secs(30);

/// Atomically leases a bounded page of continuations due at the poll's wall-clock instant.
///
/// The statement inspects no more than `scan_limit` indexed candidates and returns no more than
/// `limit` receipts. [`TimedOutFlowPoll`] guarantees `limit <= scan_limit`, so the smaller result
/// bound is sufficient for SQLite's native, non-stale due index. Expired leases are eligible for
/// reassignment and receive a fresh token, fencing every previously issued receipt.
pub(crate) async fn poll(
    pool: &SqlitePool,
    poll: &TimedOutFlowPoll,
) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
    let now_ms = unix_millis(poll.now(), "SQLite timeout poll instant")?;
    let lease_ms = i64::try_from(RECEIPT_LEASE.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQLite timeout receipt lease exceeds signed milliseconds",
        )
    })?;
    let lease_until_ms = now_ms.checked_add(lease_ms).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQLite timeout receipt deadline overflows",
        )
    })?;
    let candidate_limit = poll.limit().min(poll.scan_limit());
    let candidate_limit = i64::try_from(candidate_limit).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "SQLite timeout poll limit exceeds i64",
        )
    })?;
    let rows = sqlx::query(
        "UPDATE catga_flow_continuations SET due_token = randomblob(16), lease_until_ms = ?, \
             revision = revision + 1 \
         WHERE flow_key IN (\
             SELECT flow_key FROM catga_flow_continuations \
             WHERE deadline_ms IS NOT NULL AND deadline_ms <= ? \
               AND (due_token IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) \
             ORDER BY deadline_ms ASC, flow_key ASC LIMIT ?\
         ) \
         AND deadline_ms IS NOT NULL AND deadline_ms <= ? \
         AND (due_token IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) \
         RETURNING flow_id, due_token",
    )
    .bind(lease_until_ms)
    .bind(now_ms)
    .bind(now_ms)
    .bind(candidate_limit)
    .bind(now_ms)
    .bind(now_ms)
    .fetch_all(pool)
    .await
    .map_err(|error| database_error("poll SQLite timed-out continuations", error))?;

    rows.into_iter()
        .map(|row| {
            let flow_id: String = row
                .try_get("flow_id")
                .map_err(|error| database_error("decode SQLite timeout receipt", error))?;
            let token: Vec<u8> = row
                .try_get("due_token")
                .map_err(|error| database_error("decode SQLite timeout receipt token", error))?;
            Ok(TimedOutFlowReceipt::new(flow_id, token))
        })
        .collect()
}

/// Acknowledges a receipt only while its token still owns the row's current lease.
///
/// A successful acknowledgement removes the row from timeout discovery without deleting the
/// continuation. Usually the Flow runtime has already transitioned the continuation and cleared
/// the lease through its own compare-and-set; in that case, or for any stale token, this operation
/// intentionally becomes an idempotent no-op.
pub(crate) async fn acknowledge(
    pool: &SqlitePool,
    receipt: &TimedOutFlowReceipt,
) -> CatgaResult<()> {
    validate_token(receipt)?;
    let key = flow_key(receipt.flow_id());
    sqlx::query(
        "UPDATE catga_flow_continuations \
         SET deadline_ms = NULL, due_token = NULL, lease_until_ms = NULL, \
             revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND due_token = ?",
    )
    .bind(key.as_slice())
    .bind(receipt.flow_id())
    .bind(receipt.token())
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| database_error("acknowledge SQLite timeout receipt", error))
}

/// Releases a receipt only while its token still owns the row's current lease.
///
/// Releasing clears the lease but preserves the indexed deadline, making the continuation
/// immediately eligible for another bounded poll. A stale or forged token cannot alter a newer
/// lease and is treated as an idempotent no-op.
pub(crate) async fn release(pool: &SqlitePool, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    validate_token(receipt)?;
    let key = flow_key(receipt.flow_id());
    sqlx::query(
        "UPDATE catga_flow_continuations SET due_token = NULL, lease_until_ms = NULL, \
             revision = revision + 1 \
         WHERE flow_key = ? AND flow_id = ? AND due_token = ?",
    )
    .bind(key.as_slice())
    .bind(receipt.flow_id())
    .bind(receipt.token())
    .execute(pool)
    .await
    .map(|_| ())
    .map_err(|error| database_error("release SQLite timeout receipt", error))
}

/// Rejects tokens that could not have been issued by this SQLite backend.
fn validate_token(receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
    if receipt.token().len() == 16 {
        Ok(())
    } else {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "SQLite timeout receipt token is invalid",
        ))
    }
}

/// Converts a wall-clock instant to a signed epoch-millisecond value accepted by SQLite.
fn unix_millis(value: SystemTime, description: &str) -> CatgaResult<i64> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                format!("{description} exceeds signed milliseconds"),
            )
        }),
        Err(error) => {
            let milliseconds = i64::try_from(error.duration().as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    format!("{description} exceeds signed milliseconds"),
                )
            })?;
            milliseconds.checked_neg().ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    format!("{description} exceeds signed milliseconds"),
                )
            })
        }
    }
}
