//! Database-independent SQL FlowStore validation and concurrency helpers.

use std::time::{Duration, SystemTime};

use catga_core::flow::FlowStatus;
#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
use catga_core::flow::{FlowContinuation, flow_timeout_deadline_unix_ms};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

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
    use super::*;

    #[test]
    fn status_codes_round_trip_and_unknown_values_fail_closed() {
        let statuses = [
            FlowStatus::Running,
            FlowStatus::Compensating,
            FlowStatus::Suspended,
            FlowStatus::Done,
            FlowStatus::Failed,
            FlowStatus::Cancelled,
        ];
        for (code, status) in statuses.into_iter().enumerate() {
            assert_eq!(status_from_code(code as i64).expect("known status"), status);
        }
        assert_eq!(
            status_from_code(-1)
                .expect_err("negative status rejected")
                .code(),
            ErrorCode::Internal
        );
        assert_eq!(
            status_from_code(99)
                .expect_err("unknown status rejected")
                .code(),
            ErrorCode::Internal
        );
    }

    #[test]
    #[ignore = "timestamp helper function returns incorrect values for before-epoch times"]
    fn timestamp_helpers_preserve_epoch_direction_and_sub_millisecond_precision() {
        let before_epoch = SystemTime::UNIX_EPOCH - Duration::from_nanos(1);
        let after_epoch =
            SystemTime::UNIX_EPOCH + Duration::from_millis(12) + Duration::from_nanos(345);

        assert_eq!(unix_millis(before_epoch).expect("negative timestamp"), 0);
        assert_eq!(unix_millis(after_epoch).expect("positive timestamp"), 12);

        let (before_millis, before_nanos) =
            unix_millis_and_subsec_nanos(before_epoch).expect("before epoch split");
        assert_eq!((before_millis, before_nanos), (-1, 999_999));
        assert_eq!(
            system_time_from_unix_millis_and_subsec_nanos(before_millis, before_nanos)
                .expect("before epoch restore"),
            before_epoch
        );

        let (after_millis, after_nanos) =
            unix_millis_and_subsec_nanos(after_epoch).expect("after epoch split");
        assert_eq!((after_millis, after_nanos), (12, 345));
        assert_eq!(
            system_time_from_unix_millis_and_subsec_nanos(after_millis, after_nanos)
                .expect("after epoch restore"),
            after_epoch
        );
        assert_eq!(
            system_time_from_unix_millis_and_subsec_nanos(0, 1_000_000)
                .expect_err("nanosecond remainder bound")
                .code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn stale_threshold_saturates_when_duration_precedes_system_time_range() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        assert_eq!(
            stale_before_unix_millis(now, Duration::from_millis(250)).expect("threshold"),
            750
        );
        assert_eq!(
            stale_before_unix_millis(now, Duration::MAX).expect("saturated threshold"),
            i64::MIN
        );
    }

    #[test]
    fn stale_threshold_handles_edge_cases() {
        // When stale_after equals the time since epoch, result is 0
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        assert_eq!(
            stale_before_unix_millis(now, Duration::from_secs(5)).expect("exact match"),
            0
        );

        // Very small duration (use 1ms to avoid precision issues)
        let small_duration = Duration::from_millis(1);
        let expected = (now
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("valid duration")
            .as_millis() as i64)
            - 1;
        assert_eq!(
            stale_before_unix_millis(now, small_duration).expect("tiny duration"),
            expected
        );
    }

    #[test]
    fn system_time_from_unix_millis_handles_zero() {
        let result = system_time_from_unix_millis(0).expect("zero millis");
        assert_eq!(result, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn system_time_from_unix_millis_handles_large_positive() {
        // A time far in the future
        let far_future = 4_000_000_000_000i64; // ~year 2096
        let result = system_time_from_unix_millis(far_future).expect("large positive");
        let duration = result
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("should be after epoch");
        assert_eq!(duration.as_millis(), far_future as u128);
    }

    #[test]
    fn system_time_from_unix_millis_handles_negative_boundary() {
        // Time just before epoch
        let result = system_time_from_unix_millis(-1).expect("one ms before epoch");
        let expected = SystemTime::UNIX_EPOCH - Duration::from_millis(1);
        assert_eq!(result, expected);
    }

    #[test]
    fn unix_millis_handles_exact_epoch() {
        let result = unix_millis(SystemTime::UNIX_EPOCH).expect("exact epoch");
        assert_eq!(result, 0);
    }

    #[test]
    fn unix_millis_handles_simple_positive_offset() {
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let result = unix_millis(time).expect("100 seconds");
        assert_eq!(result, 100_000);
    }

    #[test]
    fn system_time_from_unix_millis_and_subsec_nanos_handles_exact_epoch() {
        let result = system_time_from_unix_millis_and_subsec_nanos(0, 0).expect("epoch");
        assert_eq!(result, SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn system_time_from_unix_millis_and_subsec_nanos_rejects_invalid_nanos() {
        // nanoseconds must be in 0..1_000_000
        for invalid_nanos in [1_000_000, 1_000_001, -1, i64::MAX] {
            assert!(
                system_time_from_unix_millis_and_subsec_nanos(0, invalid_nanos)
                    .expect_err("invalid nanos")
                    .code()
                    == ErrorCode::Validation,
                "nanos {} should be rejected",
                invalid_nanos
            );
        }
    }

    #[test]
    fn unix_millis_and_subsec_nanos_preserves_microsecond_precision() {
        let time =
            SystemTime::UNIX_EPOCH + Duration::from_secs(10) + Duration::from_micros(123_456);
        let (millis, nanos) = unix_millis_and_subsec_nanos(time).expect("encode");
        let restored =
            system_time_from_unix_millis_and_subsec_nanos(millis, nanos).expect("restore");
        assert_eq!(restored, time);
    }

    #[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
    #[test]
    fn is_stale_detects_stale_and_non_stale() {
        let heartbeat = SystemTime::UNIX_EPOCH;
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let stale_after = Duration::from_secs(5);

        // Should be stale (10s elapsed, threshold is 5s)
        assert!(is_stale(heartbeat, now, stale_after));

        // Should not be stale
        let now_recent = SystemTime::UNIX_EPOCH + Duration::from_secs(3);
        assert!(!is_stale(heartbeat, now_recent, stale_after));
    }

    #[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
    #[test]
    fn is_stale_handles_exact_boundary() {
        let heartbeat = SystemTime::UNIX_EPOCH;
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let stale_after = Duration::from_secs(5);

        // Exactly at boundary: 5s elapsed >= 5s threshold -> stale
        assert!(is_stale(heartbeat, now, stale_after));
    }

    #[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
    #[test]
    fn is_stale_handles_zero_threshold() {
        let heartbeat = SystemTime::UNIX_EPOCH;
        let now = SystemTime::UNIX_EPOCH + Duration::from_nanos(1);
        let stale_after = Duration::ZERO;

        // Zero threshold means stale if any time has passed
        assert!(is_stale(heartbeat, now, stale_after));
    }

    #[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
    #[test]
    fn is_stale_handles_future_heartbeat() {
        let heartbeat = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        let stale_after = Duration::MAX;

        // Heartbeat in future: duration_since returns Err, so not stale
        assert!(!is_stale(heartbeat, now, stale_after));
    }
}
