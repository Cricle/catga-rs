//! Tests for endpoint validation helpers exposed through catga_axum.

use catga_axum::{
    validate_max_length, validate_min_count, validate_min_length, validate_not_empty,
    validate_positive, validate_range, validate_required, EndpointValidation,
};
use catga_core::{CatgaError, ErrorCode};

/// Helper to collect validation errors into a vector
fn collect_errors<F>(f: F) -> Vec<Box<str>>
where
    F: FnOnce(&mut Vec<Box<str>>)
{
    let mut errors = Vec::new();
    f(&mut errors);
    errors
}

// ---------------------------------------------------------------------------
// validate_required tests
// ---------------------------------------------------------------------------

#[test]
fn validate_required_passes_for_some_value() {
    let errors = collect_errors(|errors| {
        validate_required(Some("value"), "field", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_required_fails_for_none() {
    let errors = collect_errors(|errors| {
        validate_required(None::<&str>, "email", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("email"));
    assert!(errors[0].contains("required"));
}

#[test]
fn validate_required_fails_for_empty_string() {
    let errors = collect_errors(|errors| {
        validate_required(Some(""), "name", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("name"));
}

#[test]
fn validate_required_includes_field_name_in_message() {
    let errors = collect_errors(|errors| {
        validate_required(None::<&str>, "username", errors);
    });

    let msg = &errors[0];
    assert!(msg.contains("username"));
}

// ---------------------------------------------------------------------------
// validate_not_empty tests
// ---------------------------------------------------------------------------

#[test]
fn validate_not_empty_passes_for_non_empty_collection() {
    let errors = collect_errors(|errors| {
        validate_not_empty(vec![1, 2, 3], "items", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_not_empty_fails_for_empty_collection() {
    let errors = collect_errors(|errors| {
        validate_not_empty(Vec::<u32>::new(), "items", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("items"));
}

#[test]
fn validate_not_empty_fails_for_empty_string() {
    let errors = collect_errors(|errors| {
        validate_not_empty("", "description", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("description"));
}

// ---------------------------------------------------------------------------
// validate_min_length tests
// ---------------------------------------------------------------------------

#[test]
fn validate_min_length_passes_when_length_equals_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_length("hello", 5, "word", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_min_length_passes_when_length_exceeds_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_length("hello world", 5, "word", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_min_length_fails_when_length_below_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_length("hi", 5, "word", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("word"));
    assert!(errors[0].contains("5"));
}

#[test]
fn validate_min_length_fails_with_exact_message() {
    let errors = collect_errors(|errors| {
        validate_min_length("ab", 3, "username", errors);
    });

    let msg = &errors[0];
    assert!(msg.contains("username"));
    assert!(msg.contains("at least"));
    assert!(msg.contains("3"));
}

// ---------------------------------------------------------------------------
// validate_max_length tests
// ---------------------------------------------------------------------------

#[test]
fn validate_max_length_passes_when_length_equals_maximum() {
    let errors = collect_errors(|errors| {
        validate_max_length("hello", 5, "word", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_max_length_passes_when_length_below_maximum() {
    let errors = collect_errors(|errors| {
        validate_max_length("hi", 5, "word", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_max_length_fails_when_length_exceeds_maximum() {
    let errors = collect_errors(|errors| {
        validate_max_length("hello world", 5, "word", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("word"));
    assert!(errors[0].contains("5"));
}

#[test]
fn validate_max_length_fails_with_exact_message() {
    let errors = collect_errors(|errors| {
        validate_max_length("abcdef", 5, "code", errors);
    });

    let msg = &errors[0];
    assert!(msg.contains("code"));
    assert!(msg.contains("at most"));
    assert!(msg.contains("5"));
}

// ---------------------------------------------------------------------------
// validate_positive tests
// ---------------------------------------------------------------------------

#[test]
fn validate_positive_passes_for_positive_values() {
    let errors = collect_errors(|errors| {
        validate_positive(1u32, "count", errors);
        validate_positive(42i32, "value", errors);
        validate_positive(0.1f64, "rate", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_positive_fails_for_zero() {
    let errors = collect_errors(|errors| {
        validate_positive(0u32, "count", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("count"));
    assert!(errors[0].contains("positive"));
}

#[test]
fn validate_positive_fails_for_negative_values() {
    let errors = collect_errors(|errors| {
        validate_positive(-1i32, "balance", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("balance"));
    assert!(errors[0].contains("positive"));
}

#[test]
fn validate_positive_fails_for_zero_float() {
    let errors = collect_errors(|errors| {
        validate_positive(0.0f64, "rate", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("rate"));
}

// ---------------------------------------------------------------------------
// validate_range tests
// ---------------------------------------------------------------------------

#[test]
fn validate_range_passes_when_value_is_within_range() {
    let errors = collect_errors(|errors| {
        validate_range(5u32, 0, 10, "score", errors);
        validate_range(0u32, 0, 10, "score", errors);
        validate_range(10u32, 0, 10, "score", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_range_fails_when_value_is_below_minimum() {
    let errors = collect_errors(|errors| {
        validate_range(-1i32, 0, 10, "score", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("score"));
    assert!(errors[0].contains("0"));
    assert!(errors[0].contains("10"));
}

#[test]
fn validate_range_fails_when_value_is_above_maximum() {
    let errors = collect_errors(|errors| {
        validate_range(100u32, 0, 10, "score", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("score"));
    assert!(errors[0].contains("0"));
    assert!(errors[0].contains("10"));
}

#[test]
fn validate_range_includes_both_bounds_in_message() {
    let errors = collect_errors(|errors| {
        validate_range(50u32, 0, 10, "percentage", errors);
    });

    let msg = &errors[0];
    assert!(msg.contains("percentage"));
    assert!(msg.contains("0"));
    assert!(msg.contains("10"));
    assert!(msg.contains("between"));
}

// ---------------------------------------------------------------------------
// validate_min_count tests
// ---------------------------------------------------------------------------

#[test]
fn validate_min_count_passes_when_count_equals_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_count(vec![1, 2, 3], 3, "items", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_min_count_passes_when_count_exceeds_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_count(vec![1, 2, 3, 4, 5], 3, "items", errors);
    });

    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn validate_min_count_fails_when_count_below_minimum() {
    let errors = collect_errors(|errors| {
        validate_min_count(vec![1, 2], 3, "items", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("items"));
    assert!(errors[0].contains("at least"));
    assert!(errors[0].contains("3"));
}

#[test]
fn validate_min_count_fails_for_empty_when_min_is_one() {
    let errors = collect_errors(|errors| {
        validate_min_count(Vec::<u32>::new(), 1, "tags", errors);
    });

    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("tags"));
}

// ---------------------------------------------------------------------------
// EndpointValidation combinator tests
// ---------------------------------------------------------------------------

#[test]
fn endpoint_validation_collects_multiple_errors() {
    let mut validation = EndpointValidation::new();

    validation.add(validate_required(None::<&str>, "name"));
    validation.add(validate_min_length("", 1, "description"));
    validation.add(validate_positive(0u32, "quantity"));

    let errors = validation.into_result().unwrap_err();

    assert_eq!(errors.len(), 3);
    assert_eq!(errors.code(), ErrorCode::Validation);
    assert!(errors.message().contains("name"));
    assert!(errors.message().contains("description"));
    assert!(errors.message().contains("quantity"));
}

#[test]
fn endpoint_validation_succeeds_when_all_validations_pass() {
    let mut validation = EndpointValidation::new();

    validation.add(validate_required(Some("value"), "field"));
    validation.add(validate_min_length("hello", 3, "field"));
    validation.add(validate_positive(5u32, "count"));

    let result = validation.into_result();
    assert!(result.is_ok());
}

#[test]
fn endpoint_validation_len_returns_correct_count() {
    let mut validation = EndpointValidation::new();
    assert_eq!(validation.len(), 0);

    validation.add(validate_required(None::<&str>, "a"));
    assert_eq!(validation.len(), 1);

    validation.add(validate_required(None::<&str>, "b"));
    assert_eq!(validation.len(), 2);

    validation.add(validate_required(None::<&str>, "c"));
    assert_eq!(validation.len(), 3);
}

#[test]
fn endpoint_validation_combines_error_messages() {
    let mut validation = EndpointValidation::new();

    validation.add(validate_required(None::<&str>, "email"));
    validation.add(validate_required(None::<&str>, "password"));

    let error = validation.into_result().unwrap_err();

    assert!(error.message().contains("email"));
    assert!(error.message().contains("password"));
    assert!(error.message().contains("; ")); // Separator between errors
}

#[test]
fn endpoint_validation_from_validators() {
    let validators = vec![
        validate_required(Some("test"), "field1"),
        validate_positive(10i32, "field2"),
    ];

    let mut validation = EndpointValidation::from_validators(validators);

    assert_eq!(validation.len(), 2);
    assert!(validation.into_result().is_ok());
}

// ---------------------------------------------------------------------------
// Error message format tests
// ---------------------------------------------------------------------------

#[test]
fn error_messages_include_field_name() {
    let errors = collect_errors(|errors| {
        validate_required(None::<&str>, "user_email", errors);
        validate_min_length("ab", 3, "user_name", errors);
        validate_max_length("abcdefgh", 5, "bio", errors);
    });

    assert!(errors.iter().all(|e| e.contains("user_email") || e.contains("user_name") || e.contains("bio")));
}

#[test]
fn multiple_validators_on_same_field_accumulate_errors() {
    let mut validation = EndpointValidation::new();

    // Adding multiple validators for same field
    validation.add(validate_required(None::<&str>, "field"));
    validation.add(validate_min_length("", 1, "field"));

    let error = validation.into_result().unwrap_err();

    // Both errors should be present
    assert!(error.message().contains("field"));
    // The exact format depends on implementation but should have multiple mentions
    let count = error.message().matches("field").count();
    assert!(count >= 2, "expected at least 2 mentions of 'field', got {}", count);
}
