//! Explicit endpoint-input validation helpers.
//!
//! These are framework-agnostic: any HTTP adapter can use them to collect validation
//! failures before converting them into a framework-specific error response.

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Collects validation messages before converting them into one Catga error.
///
/// The collector preserves insertion order and allocates only for reported
/// failures.
///
/// ```
/// use catga_core::{EndpointValidation, validate_required};
///
/// let mut validation = EndpointValidation::new();
/// validation.add(validate_required(None, "name"));
/// validation.add(validate_required(Some("user@example.com"), "email"));
/// assert!(!validation.is_valid());
/// assert_eq!(validation.len(), 1);
/// assert!(validation.into_result().is_err());
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EndpointValidation {
    errors: Vec<Box<str>>,
}

impl EndpointValidation {
    /// Creates an empty, valid endpoint validation result.
    pub const fn new() -> Self {
        Self { errors: Vec::new() }
    }

    /// Adds an optional error returned by one of the `validate_*` helpers.
    pub fn add(&mut self, error: Option<Box<str>>) -> &mut Self {
        if let Some(error) = error
            && !error.is_empty()
        {
            self.errors.push(error);
        }
        self
    }

    /// Adds an error message unless it is empty.
    pub fn add_error(&mut self, error: impl Into<Box<str>>) -> &mut Self {
        let error = error.into();
        if !error.is_empty() {
            self.errors.push(error);
        }
        self
    }

    /// Adds `error` when `condition` is true.
    pub fn add_if(&mut self, condition: bool, error: impl Into<Box<str>>) -> &mut Self {
        if condition {
            self.add_error(error);
        }
        self
    }

    /// Returns whether no validation error has been recorded.
    pub const fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the number of recorded validation errors.
    pub const fn len(&self) -> usize {
        self.errors.len()
    }

    /// Returns whether no validation error has been recorded.
    pub const fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the first error in insertion order.
    pub fn first(&self) -> Option<&str> {
        self.errors.first().map(AsRef::as_ref)
    }

    /// Iterates over validation errors in insertion order without cloning them.
    pub fn errors(&self) -> impl ExactSizeIterator<Item = &str> {
        self.errors.iter().map(AsRef::as_ref)
    }

    /// Converts this collector into a framework-standard validation result.
    pub fn into_result(self) -> CatgaResult<()> {
        if self.errors.is_empty() {
            return Ok(());
        }
        let capacity = self
            .errors
            .iter()
            .map(|error| error.len())
            .sum::<usize>()
            .saturating_add(self.errors.len().saturating_sub(1).saturating_mul(2));
        let mut message = String::with_capacity(capacity);
        for (index, error) in self.errors.into_iter().enumerate() {
            if index != 0 {
                message.push_str("; ");
            }
            message.push_str(&error);
        }
        Err(CatgaError::new(ErrorCode::Validation, message))
    }
}

/// Returns an error when `value` is missing or only whitespace.
pub fn validate_required(value: Option<&str>, field: &str) -> Option<Box<str>> {
    value
        .is_none_or(|value| value.trim().is_empty())
        .then(|| format!("{field} is required").into())
}

/// Returns an error when `value` is missing or shorter than `minimum` Unicode scalar values.
pub fn validate_min_length(value: Option<&str>, minimum: usize, field: &str) -> Option<Box<str>> {
    value
        .is_none_or(|value| value.chars().count() < minimum)
        .then(|| format!("{field} must be at least {minimum} characters").into())
}

/// Returns an error when a present string is longer than `maximum` Unicode scalar values.
pub fn validate_max_length(value: Option<&str>, maximum: usize, field: &str) -> Option<Box<str>> {
    value
        .is_some_and(|value| value.chars().count() > maximum)
        .then(|| format!("{field} must not exceed {maximum} characters").into())
}

/// Returns an error when a numeric value is not greater than its type's zero value.
pub fn validate_positive<T>(value: T, field: &str) -> Option<Box<str>>
where
    T: Default + PartialOrd,
{
    (value <= T::default()).then(|| format!("{field} must be positive").into())
}

/// Returns an error when `value` is outside the inclusive `[minimum, maximum]` range.
pub fn validate_range<T>(value: T, minimum: T, maximum: T, field: &str) -> Option<Box<str>>
where
    T: PartialOrd,
{
    (value < minimum || value > maximum)
        .then(|| format!("{field} must be between the supplied bounds").into())
}

/// Returns an error when an optional slice is absent or empty.
pub fn validate_not_empty<T>(value: Option<&[T]>, field: &str) -> Option<Box<str>> {
    value
        .is_none_or(<[T]>::is_empty)
        .then(|| format!("{field} must not be empty").into())
}

/// Returns an error when an optional slice has fewer than `minimum` entries.
pub fn validate_min_count<T>(value: Option<&[T]>, minimum: usize, field: &str) -> Option<Box<str>> {
    value
        .is_none_or(|value| value.len() < minimum)
        .then(|| format!("{field} must have at least {minimum} items").into())
}
