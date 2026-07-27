//! Result contract tests.

use catga_core::{CatgaError, CatgaResult, ErrorCode, MAX_ERROR_DETAILS_BYTES};

#[test]
fn successful_result_maps_without_allocating_an_error() {
    let value: CatgaResult<u64> = Ok(7);

    assert_eq!(value.map(|value| value + 1), Ok(8));
    assert_eq!(
        CatgaError::new(ErrorCode::Validation, "bad").code(),
        ErrorCode::Validation
    );
}

#[test]
fn catga_error_bounds_unicode_details_and_derives_retryability() {
    let details = "é".repeat(513);
    let transient =
        CatgaError::new(ErrorCode::Transient, "temporary failure").with_details(details);

    assert!(transient.is_retryable());
    assert!(transient.details().is_some());
    assert!(
        transient
            .details()
            .unwrap()
            .is_char_boundary(transient.details().unwrap().len())
    );
    assert!(transient.details().unwrap().len() <= MAX_ERROR_DETAILS_BYTES);
    assert!(!CatgaError::new(ErrorCode::Validation, "invalid request").is_retryable());
}

#[test]
fn error_code_parses_source_failure_names_to_typed_categories() {
    assert_eq!(
        ErrorCode::from_stable_str("TRANSPORT_FAILED"),
        Some(ErrorCode::TransportFailed)
    );
    assert_eq!(
        ErrorCode::from_stable_str("SERIALIZATION_FAILED"),
        Some(ErrorCode::SerializationFailed)
    );
    assert_eq!(ErrorCode::from_stable_str("unknown_failure"), None);
}

#[test]
fn error_codes_accept_csharp_names_and_preserve_typed_retryability() {
    let cases = [
        ("HANDLER_FAILED", ErrorCode::HandlerFailed, false),
        ("HANDLER_NOT_FOUND", ErrorCode::HandlerNotFound, false),
        ("PIPELINE_FAILED", ErrorCode::PipelineFailed, false),
        ("PERSISTENCE_FAILED", ErrorCode::PersistenceFailed, false),
        ("LOCK_FAILED", ErrorCode::LockFailed, false),
        ("TRANSPORT_FAILED", ErrorCode::TransportFailed, true),
        (
            "SERIALIZATION_FAILED",
            ErrorCode::SerializationFailed,
            false,
        ),
        ("FLOW_FAILED", ErrorCode::FlowFailed, false),
        ("FLOW_CANCELLED", ErrorCode::FlowCancelled, false),
        ("FLOW_TIMEOUT", ErrorCode::FlowTimeout, true),
        ("FLOW_COMPENSATING", ErrorCode::FlowCompensating, false),
    ];

    for (source_name, code, retryable) in cases {
        assert_eq!(ErrorCode::from_stable_str(source_name), Some(code));
        assert_eq!(ErrorCode::from_stable_str(code.as_stable_str()), Some(code));
        assert_eq!(
            CatgaError::new(code, "source compatibility").is_retryable(),
            retryable
        );
    }
}
