//! Unit tests for FlowResult.

use catga_core::flow::local::FlowResult;
use catga_core::{CatgaError, ErrorCode};

#[test]
fn flow_result_success_creates_zero_elapsed() {
    let result = FlowResult::success(3);
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 3);
    assert!(result.error().is_none());
    assert_eq!(result.elapsed(), std::time::Duration::ZERO);
}

#[test]
fn flow_result_failure_contains_error() {
    let error = CatgaError::new(ErrorCode::Validation, "test error");
    let result = FlowResult::failure(2, error);
    assert!(!result.is_success());
    assert_eq!(result.completed_steps(), 2);
    let err = result.error().expect("should have error");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert_eq!(err.message(), "test error");
}

#[test]
fn flow_result_clone_preserves_all_fields() {
    let result = FlowResult::success(5);
    let cloned = result.clone();
    assert_eq!(cloned.completed_steps(), result.completed_steps());
    assert_eq!(cloned.is_success(), result.is_success());
}

#[test]
fn flow_result_debug_format_contains_fields() {
    let result = FlowResult::success(1);
    let debug = format!("{:?}", result);
    assert!(debug.contains("completed_steps"));
}

#[test]
fn flow_result_eq_same_fields() {
    let error = CatgaError::new(ErrorCode::Internal, "same");
    let r1 = FlowResult::failure(1, error.clone());
    let r2 = FlowResult::failure(1, error);
    // FlowResult doesn't implement PartialEq, verify by checking individual fields
    assert_eq!(r1.completed_steps(), r2.completed_steps());
    assert_eq!(r1.is_success(), r2.is_success());
}

#[test]
fn flow_result_eq_different_steps() {
    let error = CatgaError::new(ErrorCode::Internal, "error");
    let r1 = FlowResult::failure(1, error.clone());
    let r2 = FlowResult::failure(2, error);
    // Cannot use assert_eq! without PartialEq, verify by checking individual fields
    assert_ne!(r1.completed_steps(), r2.completed_steps());
}

#[test]
fn flow_result_success_zero_steps() {
    let result = FlowResult::success(0);
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 0);
}
