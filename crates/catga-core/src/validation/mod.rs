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

pub mod behavior;
pub mod endpoint;

pub use behavior::{ValidationBehavior, Validator};
pub use endpoint::{
    validate_max_length, validate_min_count, validate_min_length, validate_not_empty,
    validate_positive, validate_range, validate_required, EndpointValidation,
};
