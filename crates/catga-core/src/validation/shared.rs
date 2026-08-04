//! Shared validation utilities.

use crate::{CatgaError, ErrorCode};

/// Formats a slice of validation errors into a single CatgaError.
///
/// # Arguments
/// * `errors` - The validation error messages
/// * `prefix` - Error message prefix (e.g., "validation failed: ")
///
/// # Example
/// ```
/// use catga_core::validation::shared::format_validation_errors;
///
/// let errors = vec!["field1 is required".into(), "field2 must be positive".into()];
/// let err = format_validation_errors(&errors, "validation failed: ");
/// ```
pub fn format_validation_errors(errors: &[Box<str>], prefix: &str) -> CatgaError {
    if errors.is_empty() {
        return CatgaError::new(ErrorCode::Validation, "validation failed");
    }

    let capacity = prefix.len()
        + errors.iter().map(|error| error.len()).sum::<usize>()
        + errors.len().saturating_sub(1) * 2;

    let mut message = String::with_capacity(capacity);
    message.push_str(prefix);
    for (index, error) in errors.iter().enumerate() {
        if index != 0 {
            message.push_str("; ");
        }
        message.push_str(error);
    }

    CatgaError::new(ErrorCode::Validation, message)
}
