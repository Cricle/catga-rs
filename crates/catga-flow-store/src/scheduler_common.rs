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
