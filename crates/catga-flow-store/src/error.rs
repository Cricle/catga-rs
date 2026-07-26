//! Error conversion at database boundaries.

use catga_core::{CatgaError, ErrorCode};

/// Converts a database failure into a retryable Catga availability failure.
pub(crate) fn database_error(operation: &str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(
        ErrorCode::Unavailable,
        format!("SQL FlowStore {operation} failed: {error}"),
    )
}
