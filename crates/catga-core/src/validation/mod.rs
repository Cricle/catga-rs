//! Input validation helpers for endpoints and behavior pipelines.
//!
//! # Endpoint Validation
//! Use [`EndpointValidation`] for HTTP request validation:
//!
//! ```
//! use catga_core::{EndpointValidation, validate_required};
//!
//! let mut validation = EndpointValidation::new();
//! validation.add(validate_required(None, "name"));
//! assert!(validation.into_result().is_err());
//! ```
//!
//! # Behavior Validation
//! Use [`ValidationBehavior`] for mediator pipeline validation:
//!
//! ```
//! use catga_core::validation::{ValidationBehavior, Validator};
//! ```

/// Validation behavior for mediator pipelines.
pub mod behavior;
/// Endpoint validation helpers.
pub mod endpoint;
/// Shared validation utilities.
pub mod shared;

pub use behavior::{ValidationBehavior, Validator};
pub use endpoint::{
    validate_max_length, validate_min_count, validate_min_length, validate_not_empty,
    validate_positive, validate_range, validate_required, EndpointValidation,
};
pub use shared::format_validation_errors;
