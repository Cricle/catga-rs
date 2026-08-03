//! Test assertion helpers.

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Returns the successful value or panics with the Catga error details.
pub fn assert_success<T>(result: CatgaResult<T>) -> T {
    result.unwrap_or_else(|error| {
        panic!(
            "expected success, got {:?}: {}",
            error.code(),
            error.message()
        )
    })
}

/// Returns the structured error after asserting that a Catga operation failed.
///
/// This is intended for tests. It panics with the unexpected successful value's
/// type when the operation succeeds, keeping production APIs panic-free while
/// making failed-test diagnostics concise.
pub fn assert_failure<T>(result: CatgaResult<T>) -> CatgaError {
    match result {
        Ok(_) => panic!(
            "expected Catga operation returning {} to fail",
            std::any::type_name::<T>()
        ),
        Err(error) => error,
    }
}

/// Returns the successful value after asserting it equals `expected`.
///
/// The value is moved out of the result rather than cloned. This is useful for
/// asserting non-`Clone` response types in tests.
pub fn assert_value<T>(result: CatgaResult<T>, expected: T) -> T
where
    T: std::fmt::Debug + PartialEq,
{
    match result {
        Ok(value) if value == expected => value,
        Ok(value) => panic!("expected successful value {expected:?}, got {value:?}"),
        Err(error) => panic!(
            "expected successful value {expected:?}, got {:?}: {}",
            error.code(),
            error.message()
        ),
    }
}

/// Returns every value matching `predicate`, panicking when no value matches.
///
/// The supplied iterator is consumed exactly once. Matching values are moved
/// into the returned vector, so the helper does not require `Clone`.
pub fn assert_contains<T, I, Predicate>(values: I, mut predicate: Predicate) -> Vec<T>
where
    I: IntoIterator<Item = T>,
    Predicate: FnMut(&T) -> bool,
{
    let matches: Vec<_> = values
        .into_iter()
        .filter(|value| predicate(value))
        .collect();
    assert!(
        !matches.is_empty(),
        "expected at least one matching {}",
        std::any::type_name::<T>()
    );
    matches
}

/// Returns the error after asserting its stable Catga error code.
pub fn assert_error_code<T>(
    result: CatgaResult<T>,
    expected: ErrorCode,
) -> CatgaError {
    let error = match result {
        Ok(_) => panic!("expected Catga operation to fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), expected, "unexpected Catga error code");
    error
}
