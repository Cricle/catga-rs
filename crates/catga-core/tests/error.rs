//! Tests for error module

use catga_core::{CatgaError, ErrorCode, MAX_ERROR_DETAILS_BYTES};

const CODES: [ErrorCode; 22] = [
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

#[test]
fn stable_error_names_round_trip_and_map_http_and_retry_policy() {
    for code in CODES {
        assert_eq!(ErrorCode::from_stable_str(code.as_stable_str()), Some(code));
        assert_eq!(
            CatgaError::new(code, "failure").is_retryable(),
            code.is_retryable()
        );
        assert!(code.http_status_u16() >= 400);
    }
    for (name, code) in [
        ("VALIDATION_FAILED", ErrorCode::Validation),
        ("HANDLER_FAILED", ErrorCode::HandlerFailed),
        ("HANDLER_NOT_FOUND", ErrorCode::HandlerNotFound),
        ("PIPELINE_FAILED", ErrorCode::PipelineFailed),
        ("PERSISTENCE_FAILED", ErrorCode::PersistenceFailed),
        ("LOCK_FAILED", ErrorCode::LockFailed),
        ("TRANSPORT_FAILED", ErrorCode::TransportFailed),
        ("SERIALIZATION_FAILED", ErrorCode::SerializationFailed),
        ("TIMEOUT", ErrorCode::Timeout),
        ("CANCELLED", ErrorCode::Cancelled),
        ("INTERNAL_ERROR", ErrorCode::Internal),
        ("NOT_FOUND", ErrorCode::NotFound),
        ("CONFLICT", ErrorCode::Conflict),
        ("UNAUTHORIZED", ErrorCode::Unauthorized),
        ("FORBIDDEN", ErrorCode::Forbidden),
        ("FLOW_FAILED", ErrorCode::FlowFailed),
        ("FLOW_CANCELLED", ErrorCode::FlowCancelled),
        ("FLOW_TIMEOUT", ErrorCode::FlowTimeout),
        ("FLOW_COMPENSATING", ErrorCode::FlowCompensating),
    ] {
        assert_eq!(ErrorCode::from_stable_str(name), Some(code));
    }
    assert_eq!(ErrorCode::from_stable_str("unknown"), None);
}

#[test]
fn errors_bound_utf8_details_and_restore_legacy_wire_defaults() {
    let error = CatgaError::new(ErrorCode::Validation, "missing field")
        .with_details("é".repeat(MAX_ERROR_DETAILS_BYTES));
    assert!(error.details().is_some_and(|details| {
        details.len() <= MAX_ERROR_DETAILS_BYTES && details.is_char_boundary(details.len())
    }));
    assert_eq!(error.to_string(), "missing field");
    assert!(!error.is_retryable());

    let legacy: CatgaError =
        serde_json::from_str(r#"{"code":"Transient","message":"retry","details":null}"#)
            .expect("deserialize legacy error");
    assert_eq!(legacy.code(), ErrorCode::Transient);
    assert!(legacy.is_retryable());
    assert_eq!(legacy.details(), None);

    let explicit: CatgaError = serde_json::from_str(
        r#"{"code":"Validation","message":"invalid","details":"detail","retryable":true}"#,
    )
    .expect("deserialize explicit retry override");
    assert!(explicit.is_retryable());
    assert_eq!(explicit.details(), Some("detail"));
    assert!(
        serde_json::from_str::<CatgaError>(r#"{"code":"not-a-code","message":"invalid"}"#)
            .is_err()
    );
}
