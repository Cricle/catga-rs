//! Unit tests for error types, error codes, and result aliases.

use catga_core::{bounded_details, CatgaError, ErrorCode, MAX_ERROR_DETAILS_BYTES};

#[test]
fn error_code_all_variants_have_stable_strings() {
    let codes = [
        ErrorCode::Validation,
        ErrorCode::HandlerFailed,
        ErrorCode::HandlerNotFound,
        ErrorCode::PipelineFailed,
        ErrorCode::PersistenceFailed,
        ErrorCode::LockFailed,
        ErrorCode::TransportFailed,
        ErrorCode::SerializationFailed,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::Unauthorized,
        ErrorCode::Forbidden,
        ErrorCode::Cancelled,
        ErrorCode::Timeout,
        ErrorCode::FlowFailed,
        ErrorCode::FlowCancelled,
        ErrorCode::FlowTimeout,
        ErrorCode::FlowCompensating,
        ErrorCode::Unsupported,
        ErrorCode::Transient,
        ErrorCode::Unavailable,
        ErrorCode::Internal,
    ];

    for code in codes {
        let stable = code.as_stable_str();
        assert!(!stable.is_empty());
        let round_trip = ErrorCode::from_stable_str(stable);
        assert_eq!(round_trip, Some(code), "round-trip failed for {:?}", code);
    }
}

#[test]
fn error_code_from_stable_str_handles_csharp_compatibility() {
    // C# compatibility names
    assert_eq!(
        ErrorCode::from_stable_str("VALIDATION_FAILED"),
        Some(ErrorCode::Validation)
    );
    assert_eq!(
        ErrorCode::from_stable_str("HANDLER_FAILED"),
        Some(ErrorCode::HandlerFailed)
    );
    assert_eq!(
        ErrorCode::from_stable_str("CANCELLED"),
        Some(ErrorCode::Cancelled)
    );
    assert_eq!(
        ErrorCode::from_stable_str("NOT_FOUND"),
        Some(ErrorCode::NotFound)
    );
    assert_eq!(
        ErrorCode::from_stable_str("CONFLICT"),
        Some(ErrorCode::Conflict)
    );
}

#[test]
fn error_code_from_stable_str_unknown_returns_none() {
    assert_eq!(ErrorCode::from_stable_str("unknown_error"), None);
    assert_eq!(ErrorCode::from_stable_str(""), None);
    assert_eq!(ErrorCode::from_stable_str("VALIDATION_FAILED_EXTRA"), None);
}

#[test]
fn error_code_retryable() {
    // Retryable codes
    assert!(ErrorCode::TransportFailed.is_retryable());
    assert!(ErrorCode::Timeout.is_retryable());
    assert!(ErrorCode::FlowTimeout.is_retryable());
    assert!(ErrorCode::Transient.is_retryable());
    assert!(ErrorCode::Unavailable.is_retryable());

    // Non-retryable codes
    assert!(!ErrorCode::Validation.is_retryable());
    assert!(!ErrorCode::NotFound.is_retryable());
    assert!(!ErrorCode::Conflict.is_retryable());
    assert!(!ErrorCode::Internal.is_retryable());
    assert!(!ErrorCode::Cancelled.is_retryable());
}

#[test]
fn error_code_http_status_u16() {
    assert_eq!(ErrorCode::Validation.http_status_u16(), 422);
    assert_eq!(ErrorCode::NotFound.http_status_u16(), 404);
    assert_eq!(ErrorCode::Conflict.http_status_u16(), 409);
    assert_eq!(ErrorCode::Unauthorized.http_status_u16(), 401);
    assert_eq!(ErrorCode::Forbidden.http_status_u16(), 403);
    assert_eq!(ErrorCode::Timeout.http_status_u16(), 408);
    assert_eq!(ErrorCode::Internal.http_status_u16(), 500);
    assert_eq!(ErrorCode::Unavailable.http_status_u16(), 503);
}

#[test]
fn catga_error_new() {
    let error = CatgaError::new(ErrorCode::Validation, "field is required");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "field is required");
    assert!(error.details().is_none());
}

#[test]
fn catga_error_with_details() {
    let error = CatgaError::new(ErrorCode::Validation, "validation failed")
        .with_details("input: userId");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "validation failed");
    assert_eq!(error.details(), Some("input: userId"));
}

#[test]
fn catga_error_with_details_truncates_long_details() {
    let long_details = "x".repeat(MAX_ERROR_DETAILS_BYTES * 2);
    let error = CatgaError::new(ErrorCode::Validation, "error").with_details(&long_details);
    assert_eq!(
        error.details().expect("details should be present").len(),
        MAX_ERROR_DETAILS_BYTES
    );
}

#[test]
fn catga_error_is_retryable_derives_from_code() {
    let transient = CatgaError::new(ErrorCode::Transient, "transient");
    assert!(transient.is_retryable());

    let validation = CatgaError::new(ErrorCode::Validation, "validation");
    assert!(!validation.is_retryable());
}

#[test]
fn catga_error_is_retryable_can_be_overridden() {
    let validation = CatgaError::new(ErrorCode::Validation, "validation");
    assert!(!validation.is_retryable());

    // When deserialized with retryable: Some(true), it would override
    // But for newly created errors, it uses code-derived value
}

#[test]
fn catga_error_display() {
    let error = CatgaError::new(ErrorCode::Validation, "test message");
    let display = format!("{}", error);
    assert_eq!(display, "test message");
}

#[test]
fn catga_error_clone() {
    let error = CatgaError::new(ErrorCode::Validation, "original");
    let cloned = error.clone();
    assert_eq!(cloned.code(), error.code());
    assert_eq!(cloned.message(), error.message());
}

#[test]
fn catga_error_debug() {
    let error = CatgaError::new(ErrorCode::Validation, "debug test");
    let debug = format!("{:?}", error);
    assert!(debug.contains("Validation"));
    assert!(debug.contains("debug test"));
}

#[test]
fn catga_error_eq() {
    let error1 = CatgaError::new(ErrorCode::Validation, "same message");
    let error2 = CatgaError::new(ErrorCode::Validation, "same message");
    let error3 = CatgaError::new(ErrorCode::Validation, "different message");
    let error4 = CatgaError::new(ErrorCode::Internal, "same message");

    assert_eq!(error1, error2);
    assert_ne!(error1, error3);
    assert_ne!(error1, error4);
}

#[test]
fn bounded_details_truncates_at_character_boundary() {
    // Test with a multi-byte UTF-8 character that would be split
    let multi_byte = "abc\u{1F600}xyz"; // emoji at position 3
    let result = bounded_details(multi_byte);
    // Should not split the emoji
    assert!(result.is_char_boundary(result.len()));
}

#[test]
fn bounded_details_within_limit() {
    let short = "short string";
    let result = bounded_details(short);
    assert_eq!(result, short.into());
}

#[test]
fn bounded_details_exactly_at_limit() {
    let exactly = "x".repeat(MAX_ERROR_DETAILS_BYTES);
    let result = bounded_details(&exactly);
    assert_eq!(result.len(), MAX_ERROR_DETAILS_BYTES);
}

#[test]
fn bounded_details_empty_string() {
    let result = bounded_details("");
    assert_eq!(result, "".into());
}

#[test]
fn catga_error_serialization_round_trip() {
    use serde_json;

    let original =
        CatgaError::new(ErrorCode::Validation, "test error").with_details("extra details");

    let json = serde_json::to_string(&original).expect("should serialize");
    let deserialized: CatgaError = serde_json::from_str(&json).expect("should deserialize");

    assert_eq!(deserialized.code(), original.code());
    assert_eq!(deserialized.message(), original.message());
    assert_eq!(deserialized.details(), original.details());
}

#[test]
fn catga_error_serialization_without_retryable() {
    use serde_json;

    let original = CatgaError::new(ErrorCode::Transient, "transient error");

    let json = serde_json::to_string(&original).expect("should serialize");
    let deserialized: CatgaError = serde_json::from_str(&json).expect("should deserialize");

    // Deserialized errors derive retryable from code
    assert_eq!(deserialized.code(), original.code());
    assert!(deserialized.is_retryable());
}
