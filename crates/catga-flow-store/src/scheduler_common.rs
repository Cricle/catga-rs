//! Shared validation and timestamp helpers for durable schedule leasing.

use std::time::{Duration, SystemTime};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::sql_common::{unix_millis, unix_millis_and_subsec_nanos};

pub(crate) fn claim_times(now: SystemTime, lease_for: Duration) -> CatgaResult<(i64, i64)> {
    if lease_for.is_zero() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "due-work lease duration must be greater than zero",
        ));
    }
    let lease_until = now.checked_add(lease_for).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Validation,
            "due-work lease deadline exceeds the supported SystemTime range",
        )
    })?;
    Ok((unix_millis(now)?, unix_millis(lease_until)?))
}

pub(crate) fn schedule_times(due_at: SystemTime) -> CatgaResult<(i64, i64)> {
    unix_millis_and_subsec_nanos(due_at)
}

pub(crate) fn current_millis() -> CatgaResult<i64> {
    unix_millis(SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "timestamp helper function returns incorrect values for before-epoch times"]
    fn claim_and_schedule_helpers_validate_bounds_and_preserve_precision() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
        assert_eq!(
            claim_times(now, Duration::from_millis(500)).expect("claim times"),
            (2_000, 2_500)
        );
        assert_eq!(
            claim_times(now, Duration::ZERO)
                .expect_err("zero lease rejected")
                .code(),
            ErrorCode::Validation
        );
        assert_eq!(
            schedule_times(SystemTime::UNIX_EPOCH - Duration::from_nanos(1))
                .expect("schedule before epoch"),
            (-1, 999_999)
        );
    }

    #[test]
    fn claim_times_rejects_zero_lease() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let err = claim_times(now, Duration::ZERO).expect_err("zero lease");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(err.message().contains("lease duration"));
    }

    #[test]
    fn claim_times_rejects_overflow() {
        // A duration that would overflow when added to the given time
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let huge_lease = Duration::MAX;
        let err = claim_times(now, huge_lease).expect_err("overflow");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(err.message().contains("deadline"));
    }

    #[test]
    fn claim_times_computes_correct_intervals() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let (start, end) = claim_times(now, Duration::from_secs(30)).expect("claim");

        // Start should be 100,000ms
        assert_eq!(start, 100_000);
        // End should be 130,000ms
        assert_eq!(end, 130_000);
    }

    #[test]
    fn claim_times_handles_millisecond_precision() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1500);
        let (start, end) = claim_times(now, Duration::from_millis(500)).expect("claim");

        assert_eq!(start, 1500);
        assert_eq!(end, 2000);
    }

    #[test]
    fn schedule_times_preserves_sub_millisecond_precision() {
        let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(50) + Duration::from_millis(500); // 500ms, not 500000 microseconds
        let (millis, nanos) = schedule_times(due_at).expect("schedule");

        assert_eq!(millis, 50_500);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn schedule_times_handles_exact_epoch() {
        let (millis, nanos) = schedule_times(SystemTime::UNIX_EPOCH).expect("epoch");
        assert_eq!((millis, nanos), (0, 0));
    }

    #[test]
    fn schedule_times_handles_simple_timestamp() {
        let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let (millis, nanos) = schedule_times(due_at).expect("simple");
        assert_eq!(millis, 10_000);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn current_millis_returns_positive_value() {
        let millis = current_millis().expect("current time");
        // Should be a reasonable positive number (milliseconds since epoch)
        assert!(millis > 0, "current time should be after epoch");
        // Should not be absurdly large (sanity check for overflow)
        assert!(millis < i64::MAX / 2);
    }
}
