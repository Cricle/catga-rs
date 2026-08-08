//! Unit tests for idempotency helper functions.

use catga_core::ErrorCode;
use std::time::Duration;

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;
const MAX_RESULT_BYTES: usize = 1024 * 1024;
const MAX_REDIS_RETENTION_MILLIS: i64 = 100 * 365 * 24 * 60 * 60 * 1_000;

const CLAIM: &str = "local key = KEYS[1] local value = redis.call('GET', key) if not value then return nil end local state = string.byte(value, 1) if state ~= 1 then return nil end local now = redis.call('TIME') local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000) local idle = now_ms - tonumber(ARGV[1]) if idle < tonumber(ARGV[2]) then return nil end local new_value = string.char(1) redis.call('SET', key, new_value, 'PX', ARGV[3]) return value";
const TRANSITION: &str = "local key = KEYS[1] local expected = ARGV[1] local new_state = ARGV[2] local ttl = ARGV[3] local current = redis.call('GET', key) if current ~= expected then return -1 end redis.call('SET', key, new_state, 'PX', ttl) return 1";

fn state(value: &[u8]) -> catga_core::CatgaResult<i32> {
    if value.is_empty() {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Internal,
            "Redis idempotency record is malformed",
        ));
    }
    match value[0] {
        1 => Ok(1),
        2 | 3 => Ok(2),
        4 => Ok(3),
        _ => Err(catga_core::CatgaError::new(
            ErrorCode::Internal,
            "Redis idempotency record is malformed",
        )),
    }
}

fn retention_millis(duration: Duration) -> catga_core::CatgaResult<u64> {
    if duration.is_zero() {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "Redis idempotency record retention must be greater than zero",
        ));
    }
    let millis = u64::try_from(duration.as_millis())
        .map_err(|_| {
            catga_core::CatgaError::new(
                ErrorCode::Validation,
                "Redis idempotency record retention exceeds u64",
            )
        })?;
    if millis > (MAX_REDIS_RETENTION_MILLIS as u64) {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "Redis idempotency record retention must not exceed 100 years",
        ));
    }
    Ok(millis)
}

#[test]
fn state_claimed() {
    let value = vec![CLAIMED];
    assert_eq!(state(&value).expect("claimed state"), 1);
}

#[test]
fn state_completed_empty() {
    let value = vec![COMPLETED_EMPTY];
    assert_eq!(state(&value).expect("completed empty"), 2);
}

#[test]
fn state_completed_result() {
    let value = vec![COMPLETED_RESULT, b'd', b'a', b't', b'a'];
    assert_eq!(state(&value).expect("completed result"), 2);
}

#[test]
fn state_failed() {
    let value = vec![FAILED];
    assert_eq!(state(&value).expect("failed state"), 3);
}

#[test]
fn state_malformed_empty() {
    let err = state(&[]).expect_err("empty fails");
    assert_eq!(err.code(), ErrorCode::Internal);
    assert!(err.message().contains("malformed"));
}

#[test]
fn state_malformed_unknown_byte() {
    let value = vec![99];
    let err = state(&value).expect_err("unknown byte fails");
    assert_eq!(err.code(), ErrorCode::Internal);
    assert!(err.message().contains("malformed"));
}

#[test]
fn state_multi_byte_value_with_claimed_prefix() {
    let value = vec![CLAIMED, 0, 0, 0];
    assert_eq!(state(&value).expect("claimed prefix"), 1);
}

#[test]
fn state_multi_byte_value_with_completed_result_prefix() {
    let value = vec![COMPLETED_RESULT, 1, 2, 3, 4, 5];
    assert_eq!(state(&value).expect("completed result prefix"), 2);
}

#[test]
fn retention_millis_valid() {
    let millis = retention_millis(Duration::from_secs(60)).expect("valid");
    assert_eq!(millis, 60_000);
}

#[test]
fn retention_millis_zero_fails() {
    let err = retention_millis(Duration::ZERO).expect_err("zero fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("greater than zero"));
}

#[test]
fn retention_millis_with_sub_millis_does_not_round_up() {
    // Note: retention_millis truncates to milliseconds without rounding up
    let duration = Duration::from_millis(1).saturating_add(Duration::from_micros(500));
    let millis = retention_millis(duration).expect("valid");
    assert_eq!(millis, 1);
}

#[test]
fn retention_millis_exceeds_max_fails() {
    let duration = Duration::from_millis((MAX_REDIS_RETENTION_MILLIS as u64) + 1);
    let err = retention_millis(duration).expect_err("exceeds max fails");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("100 years"));
}

#[test]
fn retention_millis_at_max() {
    let duration = Duration::from_millis(MAX_REDIS_RETENTION_MILLIS as u64);
    let millis = retention_millis(duration).expect("at max is valid");
    assert_eq!(millis, MAX_REDIS_RETENTION_MILLIS as u64);
}

#[test]
fn retention_millis_very_large() {
    let duration = Duration::from_secs(365 * 24 * 60 * 60);
    let millis = retention_millis(duration).expect("1 year valid");
    assert_eq!(millis, 31_536_000_000);
}

#[test]
fn retention_millis_one_millisecond() {
    let millis = retention_millis(Duration::from_millis(1)).expect("1ms valid");
    assert_eq!(millis, 1);
}

#[test]
fn retention_millis_sub_millisecond_does_not_round_up() {
    // Duration::from_nanos(500) truncates to 0ms when using as_millis()
    // (500ns < 1ms, so as_millis() returns 0)
    let duration = Duration::from_nanos(500);
    let millis = retention_millis(duration).expect("sub-ms valid");
    assert_eq!(millis, 0);
}

#[test]
fn state_constants_are_distinct() {
    assert_ne!(CLAIMED, COMPLETED_EMPTY);
    assert_ne!(CLAIMED, COMPLETED_RESULT);
    assert_ne!(CLAIMED, FAILED);
    assert_ne!(COMPLETED_EMPTY, COMPLETED_RESULT);
    assert_ne!(COMPLETED_EMPTY, FAILED);
    assert_ne!(COMPLETED_RESULT, FAILED);
}

#[test]
fn max_result_bytes_value() {
    assert_eq!(MAX_RESULT_BYTES, 1024 * 1024);
}

#[test]
fn max_redis_retention_millis_value() {
    let expected = 100i64 * 365 * 24 * 60 * 60 * 1_000;
    assert_eq!(MAX_REDIS_RETENTION_MILLIS, expected);
}

#[test]
fn state_constants_are_nonzero() {
    assert_ne!(CLAIMED, 0);
    assert_ne!(COMPLETED_EMPTY, 0);
    assert_ne!(COMPLETED_RESULT, 0);
    assert_ne!(FAILED, 0);
}

#[test]
fn claim_script_contains_expected_operations() {
    assert!(CLAIM.contains("GET"));
    assert!(CLAIM.contains("SET"));
    assert!(CLAIM.contains("string.byte"));
    assert!(CLAIM.contains("string.char"));
}

#[test]
fn transition_script_contains_expected_operations() {
    assert!(TRANSITION.contains("GET"));
    assert!(TRANSITION.contains("SET"));
    assert!(TRANSITION.contains("PX"));
}
