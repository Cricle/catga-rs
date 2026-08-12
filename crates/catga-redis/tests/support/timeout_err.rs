//! Shared timeout-error fixture for bounded receive waits.

use catga_core::{CatgaError, ErrorCode};

/// Builds a timeout error for bounded receive waits.
pub fn timeout_error(context: &'static str) -> CatgaError {
    CatgaError::new(ErrorCode::Timeout, context)
}
