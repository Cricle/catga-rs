//! Unit tests for resilience options and executor.

use std::time::Duration;

use catga_core::{
    CatgaError, CircuitBreakerOptions, ErrorCode, ResilienceExecutor, ResilienceOptions, RetryJitter,
};

#[test]
fn resilience_options_default() {
    let options = ResilienceOptions::default();
    assert_eq!(options.max_concurrent, 0);
    assert_eq!(options.max_queued, 0);
    assert_eq!(options.timeout, Duration::from_secs(3));
    assert_eq!(options.max_retries, 0);
    assert_eq!(options.retry_delay, Duration::ZERO);
    assert_eq!(options.circuit_failure_threshold, 20);
    assert_eq!(options.circuit_reset_timeout, Duration::from_secs(30));
}

#[test]
fn resilience_options_validate_accepts_valid_config() {
    let options = ResilienceOptions::default();
    assert!(ResilienceExecutor::new(options).is_ok());
}

#[test]
fn resilience_options_with_max_concurrent_and_queued_valid() {
    let options = ResilienceOptions {
        max_concurrent: 10,
        max_queued: 50,
        timeout: Duration::from_secs(5),
        max_retries: 3,
        retry_delay: Duration::from_millis(100),
        ..ResilienceOptions::default()
    };
    assert!(ResilienceExecutor::new(options).is_ok());
}

#[test]
fn resilience_options_zero_timeout_invalid() {
    let options = ResilienceOptions {
        timeout: Duration::ZERO,
        ..ResilienceOptions::default()
    };
    let result = ResilienceExecutor::new(options);
    match result {
        Err(e) => assert_eq!(e.code(), ErrorCode::Validation),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn resilience_options_zero_concurrent_with_queued_invalid() {
    let options = ResilienceOptions {
        max_concurrent: 0,
        max_queued: 10,
        timeout: Duration::from_secs(5),
        ..ResilienceOptions::default()
    };
    let result = ResilienceExecutor::new(options);
    match result {
        Err(e) => assert_eq!(e.code(), ErrorCode::Validation),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn resilience_executor_new_accepts_valid_options() {
    let options = ResilienceOptions::default();
    let executor = ResilienceExecutor::new(options);
    assert!(executor.is_ok());
}

#[test]
fn resilience_executor_with_jitter_accepts_custom_jitter() {
    let options = ResilienceOptions::default();
    let executor = ResilienceExecutor::with_jitter(options, RetryJitter::none());
    assert!(executor.is_ok());
}

#[test]
fn resilience_executor_with_policies_accepts_valid_input() {
    let options = ResilienceOptions::default();
    let circuit_options = CircuitBreakerOptions::builder(5, Duration::from_secs(30))
        .build()
        .expect("valid options");
    let executor =
        ResilienceExecutor::with_policies(options, circuit_options, RetryJitter::none());
    assert!(executor.is_ok());
}

#[test]
fn resilience_executor_jitter_policy_returns_configured_policy() {
    let options = ResilienceOptions::default();

    let executor =
        ResilienceExecutor::with_jitter(options, RetryJitter::none()).expect("valid options");
    assert_eq!(executor.jitter_policy(), RetryJitter::none());

    let fixed_jitter = RetryJitter::fixed(Duration::from_millis(100));
    let executor =
        ResilienceExecutor::with_jitter(options, fixed_jitter).expect("valid options");
    assert_eq!(executor.jitter_policy(), fixed_jitter);
}

#[test]
fn resilience_executor_jitter_policy_full_jitter() {
    let options = ResilienceOptions::default();
    let full_jitter = RetryJitter::full(42);
    let executor = ResilienceExecutor::with_jitter(options, full_jitter).expect("valid options");
    assert!(matches!(executor.jitter_policy(), RetryJitter::Full { .. }));
}

#[test]
fn error_codes_retryable() {
    let retryable_codes = [
        ErrorCode::Transient,
        ErrorCode::Timeout,
        ErrorCode::FlowTimeout,
        ErrorCode::Unavailable,
        ErrorCode::TransportFailed,
    ];

    for code in retryable_codes {
        let error = CatgaError::new(code, "test");
        assert!(
            error.is_retryable(),
            "ErrorCode::{:?} should be retryable",
            code
        );
    }
}

#[test]
fn error_codes_not_retryable() {
    let non_retryable_codes = [
        ErrorCode::Validation,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::Internal,
        ErrorCode::Cancelled,
    ];

    for code in non_retryable_codes {
        let error = CatgaError::new(code, "test");
        assert!(
            !error.is_retryable(),
            "ErrorCode::{:?} should not be retryable",
            code
        );
    }
}

#[test]
fn error_code_http_status_mapping() {
    assert_eq!(ErrorCode::Validation.http_status_u16(), 422);
    assert_eq!(ErrorCode::NotFound.http_status_u16(), 404);
    assert_eq!(ErrorCode::Conflict.http_status_u16(), 409);
    assert_eq!(ErrorCode::Unauthorized.http_status_u16(), 401);
    assert_eq!(ErrorCode::Forbidden.http_status_u16(), 403);
    assert_eq!(ErrorCode::Internal.http_status_u16(), 500);
}

// Note: CircuitBreakerOptions builder tests removed - accessors like
// failure_threshold(), reset_timeout(), minimum_throughput(),
// failure_ratio_numerator(), failure_ratio_denominator() are private
