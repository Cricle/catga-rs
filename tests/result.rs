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
fn error_code_parses_source_failure_aliases_to_typed_categories() {
    assert_eq!(
        ErrorCode::from_stable_str("TRANSPORT_FAILED"),
        Some(ErrorCode::Unavailable)
    );
    assert_eq!(
        ErrorCode::from_stable_str("SERIALIZATION_FAILED"),
        Some(ErrorCode::Internal)
    );
    assert_eq!(ErrorCode::from_stable_str("unknown_failure"), None);
}
