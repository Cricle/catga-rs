//! Explicit endpoint-input validation helpers.
//!
//! Re-exported from [`catga_core`] where the implementation lives. This module exists
//! only for backward-compatible `catga_axum::validate_*` import paths.

pub use catga_core::{
    EndpointValidation, validate_max_length, validate_min_count, validate_min_length,
    validate_not_empty, validate_positive, validate_range, validate_required,
};
