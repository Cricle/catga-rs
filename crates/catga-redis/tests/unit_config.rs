//! Unit tests for config helper functions.

use catga_core::ErrorCode;
use std::time::Duration;

const DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_REDIS_PENDING_RECLAIM_SCANS: usize = 64;
const MAX_REDIS_PENDING_RECLAIM_SCANS: usize = 1024;

fn redis_command_options_new(timeout: Duration) -> Result<Duration, catga_core::CatgaError> {
    if timeout.is_zero() {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "Redis command response timeout must be greater than zero",
        ));
    }
    Ok(timeout)
}

fn redis_pending_reclaim_options_new(
    minimum_idle: Duration,
    max_scans: usize,
) -> Result<(Duration, u64, usize), catga_core::CatgaError> {
    let minimum_idle_millis = u64::try_from(minimum_idle.as_millis()).map_err(|_| {
        catga_core::CatgaError::new(
            ErrorCode::Validation,
            "Redis pending reclaim idle duration exceeds Redis millisecond precision",
        )
    })?;
    if minimum_idle_millis == 0 {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "Redis pending reclaim idle duration must be at least one millisecond",
        ));
    }
    if !(1..=MAX_REDIS_PENDING_RECLAIM_SCANS).contains(&max_scans) {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            format!(
                "Redis pending reclaim scan limit must be between 1 and {}",
                MAX_REDIS_PENDING_RECLAIM_SCANS
            ),
        ));
    }
    Ok((minimum_idle, minimum_idle_millis, max_scans))
}

#[test]
fn redis_command_options_new_valid() {
    let result = redis_command_options_new(Duration::from_millis(250));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Duration::from_millis(250));
}

#[test]
fn redis_command_options_new_zero_fails() {
    let err = redis_command_options_new(Duration::ZERO).expect_err("zero fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("greater than zero"));
}

#[test]
fn redis_command_options_default() {
    let opts = Duration::from_secs(1);
    assert_eq!(opts, DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT);
}

#[test]
fn redis_pending_reclaim_options_new_valid() {
    let result = redis_pending_reclaim_options_new(Duration::from_secs(5), 4);
    assert!(result.is_ok());
    let (idle, idle_millis, scans) = result.unwrap();
    assert_eq!(idle, Duration::from_secs(5));
    assert_eq!(idle_millis, 5000);
    assert_eq!(scans, 4);
}

#[test]
fn redis_pending_reclaim_options_new_zero_duration_fails() {
    let err = redis_pending_reclaim_options_new(Duration::ZERO, 1).expect_err("zero duration fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("at least one millisecond"));
}

#[test]
fn redis_pending_reclaim_options_new_zero_scans_fails() {
    let err = redis_pending_reclaim_options_new(Duration::from_millis(100), 0)
        .expect_err("zero scans fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("between 1 and"));
}

#[test]
fn redis_pending_reclaim_options_new_max_scans_plus_one_fails() {
    let err = redis_pending_reclaim_options_new(
        Duration::from_millis(100),
        MAX_REDIS_PENDING_RECLAIM_SCANS + 1,
    )
    .expect_err("max+1 scans fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("between 1 and"));
}

#[test]
fn redis_pending_reclaim_options_new_at_max_scans() {
    let result = redis_pending_reclaim_options_new(
        Duration::from_millis(100),
        MAX_REDIS_PENDING_RECLAIM_SCANS,
    );
    assert!(result.is_ok());
    let (_, _, scans) = result.unwrap();
    assert_eq!(scans, MAX_REDIS_PENDING_RECLAIM_SCANS);
}

#[test]
fn redis_pending_reclaim_options_new_minimum_idle_millis() {
    let result = redis_pending_reclaim_options_new(Duration::from_millis(42), 1);
    assert!(result.is_ok());
    let (_, idle_millis, _) = result.unwrap();
    assert_eq!(idle_millis, 42);
}

#[test]
fn redis_pending_reclaim_options_new_one_millisecond() {
    let result = redis_pending_reclaim_options_new(Duration::from_millis(1), 1);
    assert!(result.is_ok());
}

#[test]
fn redis_pending_reclaim_options_sub_millisecond_truncates() {
    let err = redis_pending_reclaim_options_new(Duration::from_nanos(500), 1)
        .expect_err("sub-millisecond fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("at least one millisecond"));
}

#[test]
fn default_response_timeout_value() {
    assert_eq!(
        DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT,
        Duration::from_secs(1)
    );
}
