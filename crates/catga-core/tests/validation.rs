//! Tests for validation module.

use catga_core::{
    EndpointValidation, validate_max_length, validate_min_count, validate_min_length,
    validate_not_empty, validate_positive, validate_range, validate_required,
};

#[test]
fn endpoint_validation_adds_errors() {
    let mut validation = EndpointValidation::new();
    validation.add(Some("error1".into()));
    validation.add(Some("error2".into()));
    validation.add(None);

    assert_eq!(validation.len(), 2);
    assert!(!validation.is_valid());
    assert_eq!(validation.first(), Some("error1"));
}

#[test]
fn endpoint_validation_add_error() {
    let mut validation = EndpointValidation::new();
    validation.add_error("test error");
    validation.add_error("");

    assert_eq!(validation.len(), 1);
}

#[test]
fn endpoint_validation_add_if() {
    let mut validation = EndpointValidation::new();
    validation.add_if(true, "error when true");
    validation.add_if(false, "error when false");

    assert_eq!(validation.len(), 1);
}

#[test]
fn endpoint_validation_iterate_errors() {
    let mut validation = EndpointValidation::new();
    validation.add(Some("first".into()));
    validation.add(Some("second".into()));

    let errors: Vec<_> = validation.errors().collect();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0], "first");
    assert_eq!(errors[1], "second");
}

#[test]
fn endpoint_validation_into_result_ok() {
    let validation = EndpointValidation::new();
    assert!(validation.into_result().is_ok());
}

#[test]
fn endpoint_validation_into_result_err() {
    let mut validation = EndpointValidation::new();
    validation.add(Some("error".into()));
    assert!(validation.into_result().is_err());
}

#[test]
fn endpoint_validation_is_empty() {
    let validation = EndpointValidation::new();
    assert!(validation.is_empty());
    assert!(validation.is_valid());
}

#[test]
fn validate_required_none() {
    assert!(validate_required(None, "field").is_some());
}

#[test]
fn validate_required_empty() {
    assert!(validate_required(Some(""), "field").is_some());
}

#[test]
fn validate_required_whitespace() {
    assert!(validate_required(Some("   "), "field").is_some());
}

#[test]
fn validate_required_valid() {
    assert!(validate_required(Some("value"), "field").is_none());
}

#[test]
fn validate_min_length_too_short() {
    let result = validate_min_length(Some("ab"), 3, "field");
    assert!(result.is_some());
    assert!(result.expect("should have error message").contains("3"));
}

#[test]
fn validate_min_length_valid() {
    assert!(validate_min_length(Some("abc"), 3, "field").is_none());
}

#[test]
fn validate_min_length_none_returns_error() {
    // When value is None, is_none_or returns true, so an error is returned
    assert!(validate_min_length(None, 1, "field").is_some());
}

#[test]
fn validate_min_length_unicode() {
    // Unicode characters should count as single characters
    let result = validate_min_length(Some("你好"), 3, "field");
    assert!(result.is_some());
}

#[test]
fn validate_max_length_too_long() {
    let result = validate_max_length(Some("abcdef"), 5, "field");
    assert!(result.is_some());
    assert!(result.expect("result should be Some").contains("5"));
}

#[test]
fn validate_max_length_valid() {
    assert!(validate_max_length(Some("abc"), 5, "field").is_none());
}

#[test]
fn validate_max_length_none() {
    assert!(validate_max_length(None::<&str>, 5, "field").is_none());
}

#[test]
fn validate_max_length_at_boundary() {
    assert!(validate_max_length(Some("abcde"), 5, "field").is_none());
}

#[test]
fn validate_positive_zero() {
    assert!(validate_positive(0i32, "field").is_some());
}

#[test]
fn validate_positive_negative() {
    assert!(validate_positive(-1i32, "field").is_some());
}

#[test]
fn validate_positive_valid() {
    assert!(validate_positive(1i32, "field").is_none());
}

#[test]
fn validate_positive_u32_zero() {
    assert!(validate_positive(0u32, "field").is_some());
}

#[test]
fn validate_not_empty_none() {
    assert!(validate_not_empty::<i32>(None, "field").is_some());
}

#[test]
fn validate_not_empty_empty() {
    assert!(validate_not_empty::<i32>(Some(&[][..]), "field").is_some());
}

#[test]
fn validate_not_empty_valid() {
    assert!(validate_not_empty(Some(&[1, 2, 3][..]), "field").is_none());
}

#[test]
fn validate_min_count_too_few() {
    assert!(validate_min_count(Some(&[1][..]), 2, "field").is_some());
}

#[test]
fn validate_min_count_valid() {
    assert!(validate_min_count(Some(&[1, 2][..]), 2, "field").is_none());
}

#[test]
fn validate_min_count_at_boundary() {
    assert!(validate_min_count(Some(&[1, 2, 3][..]), 3, "field").is_none());
}

#[test]
fn validate_range_below_min() {
    let result = validate_range(5i32, 10, 20, "field");
    assert!(result.is_some());
}

#[test]
fn validate_range_above_max() {
    let result = validate_range(25i32, 0, 10, "field");
    assert!(result.is_some());
}

#[test]
fn validate_range_valid() {
    assert!(validate_range(7i32, 5, 10, "field").is_none());
}

#[test]
fn validate_range_exact_min() {
    assert!(validate_range(5i32, 5, 10, "field").is_none());
}

#[test]
fn validate_range_exact_max() {
    assert!(validate_range(10i32, 5, 10, "field").is_none());
}
