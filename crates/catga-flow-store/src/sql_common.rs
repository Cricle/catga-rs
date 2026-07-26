//! Database-independent SQL FlowStore validation and concurrency helpers.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::FlowStatus;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
use catga_flow::{FlowContinuation, flow_timeout_deadline_unix_ms};

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
/// Maximum read/compare/write attempts for a contested mutable row.
pub(crate) const MAX_CAS_RETRIES: usize = 8;

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
/// Maps a public lifecycle status to its compact indexed representation.
pub(crate) const fn status_code(status: FlowStatus) -> i64 {
    match status {
        FlowStatus::Running => 0,
        FlowStatus::Compensating => 1,
        FlowStatus::Suspended => 2,
        FlowStatus::Done => 3,
        FlowStatus::Failed => 4,
        FlowStatus::Cancelled => 5,
    }
}

/// Decodes a compact indexed lifecycle status.
pub(crate) fn status_from_code(status: i64) -> CatgaResult<FlowStatus> {
    match status {
        0 => Ok(FlowStatus::Running),
        1 => Ok(FlowStatus::Compensating),
        2 => Ok(FlowStatus::Suspended),
        3 => Ok(FlowStatus::Done),
        4 => Ok(FlowStatus::Failed),
        5 => Ok(FlowStatus::Cancelled),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            format!("SQL FlowStore contains unknown status code {status}"),
        )),
    }
}

/// Converts a signed database millisecond value back to `SystemTime`.
pub(crate) fn system_time_from_unix_millis(value: i64) -> CatgaResult<SystemTime> {
    let duration = Duration::from_millis(value.unsigned_abs());
    let result = if value.is_negative() {
        SystemTime::UNIX_EPOCH.checked_sub(duration)
    } else {
        SystemTime::UNIX_EPOCH.checked_add(duration)
    };
    result.ok_or_else(timestamp_error)
}

/// Converts a system time to a signed millisecond database value.
pub(crate) fn unix_millis(value: SystemTime) -> CatgaResult<i64> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).map_err(|_| timestamp_error()),
        Err(error) => {
            let milliseconds =
                i64::try_from(error.duration().as_millis()).map_err(|_| timestamp_error())?;
            milliseconds.checked_neg().ok_or_else(timestamp_error)
        }
    }
}

/// Converts a system time to an exact, order-preserving millisecond and nanosecond remainder.
///
/// The remainder is always in `0..1_000_000`, including for times before the Unix epoch.
pub(crate) fn unix_millis_and_subsec_nanos(value: SystemTime) -> CatgaResult<(i64, i64)> {
    match value.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => Ok((
            i64::try_from(duration.as_millis()).map_err(|_| timestamp_error())?,
            i64::from(duration.subsec_nanos() % 1_000_000),
        )),
        Err(error) => {
            let duration = error.duration();
            let milliseconds =
                i64::try_from(duration.as_millis()).map_err(|_| timestamp_error())?;
            let remainder = i64::from(duration.subsec_nanos() % 1_000_000);
            if remainder == 0 {
                Ok((milliseconds.checked_neg().ok_or_else(timestamp_error)?, 0))
            } else {
                Ok((
                    milliseconds
                        .checked_add(1)
                        .and_then(i64::checked_neg)
                        .ok_or_else(timestamp_error)?,
                    1_000_000 - remainder,
                ))
            }
        }
    }
}

/// Restores an exact system time from the indexed millisecond and nanosecond representation.
pub(crate) fn system_time_from_unix_millis_and_subsec_nanos(
    milliseconds: i64,
    subsec_nanos: i64,
) -> CatgaResult<SystemTime> {
    if !(0..1_000_000).contains(&subsec_nanos) {
        return Err(timestamp_error());
    }
    system_time_from_unix_millis(milliseconds)?
        .checked_add(Duration::from_nanos(
            u64::try_from(subsec_nanos).map_err(|_| timestamp_error())?,
        ))
        .ok_or_else(timestamp_error)
}

/// Returns the signed SQL millisecond threshold, including every persisted value on underflow.
pub(crate) fn stale_before_unix_millis(now: SystemTime, stale_after: Duration) -> CatgaResult<i64> {
    now.checked_sub(stale_after)
        .map(unix_millis)
        .transpose()?
        .map_or(Ok(i64::MIN), Ok)
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
/// Returns the indexed wait deadline for a continuation.
pub(crate) fn deadline_millis(continuation: &FlowContinuation) -> CatgaResult<Option<i64>> {
    flow_timeout_deadline_unix_ms(continuation)?
        .map(|value| {
            i64::try_from(value).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "flow wait deadline exceeds signed SQL milliseconds",
                )
            })
        })
        .transpose()
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
/// Reports exhaustion of a bounded physical revision compare-and-set loop.
pub(crate) fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("SQL FlowStore could not {operation} after bounded retries"),
    )
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
/// Tests staleness against one captured wall-clock instant.
pub(crate) fn is_stale(
    heartbeat: SystemTime,
    now: SystemTime,
    stale_after: std::time::Duration,
) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}

fn timestamp_error() -> CatgaError {
    CatgaError::new(
        ErrorCode::Validation,
        "flow timestamp exceeds signed milliseconds",
    )
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::stale_before_unix_millis;

    #[test]
    fn stale_threshold_underflow_includes_every_signed_millisecond() {
        assert_eq!(
            stale_before_unix_millis(SystemTime::UNIX_EPOCH, Duration::MAX),
            Ok(i64::MIN)
        );
    }
}
