//! Result contract tests.

use catga_core::{CatgaError, CatgaResult, ErrorCode};

#[test]
fn successful_result_maps_without_allocating_an_error() {
    let value: CatgaResult<u64> = Ok(7);

    assert_eq!(value.map(|value| value + 1), Ok(8));
    assert_eq!(
        CatgaError::new(ErrorCode::Validation, "bad").code(),
        ErrorCode::Validation
    );
}
