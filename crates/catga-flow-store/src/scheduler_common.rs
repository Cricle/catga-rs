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
}
