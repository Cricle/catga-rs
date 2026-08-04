use std::sync::Arc;

use async_trait::async_trait;

use super::shared::format_validation_errors;
use crate::{Behavior, CatgaError, CatgaResult, Next, Request};

/// Validates one request and appends every user-facing error to `errors`.
#[async_trait]
pub trait Validator<M>: Send + Sync
where
    M: Request,
{
    /// Validates `request`, returning an operational error only when validation itself cannot run.
    async fn validate(&self, request: &M, errors: &mut Vec<Box<str>>) -> CatgaResult<()>;
}

/// Runs immutable validators before dispatching a request to its handler.
pub struct ValidationBehavior<M>
where
    M: Request,
{
    validators: Box<[Arc<dyn Validator<M>>]>,
}

impl<M> ValidationBehavior<M>
where
    M: Request,
{
    /// Creates a behavior from validators assembled during application startup.
    pub fn new(validators: impl IntoIterator<Item = Arc<dyn Validator<M>>>) -> Self {
        Self {
            validators: validators.into_iter().collect(),
        }
    }

    /// Creates a behavior with no validators, preserving the handler fast path.
    pub fn empty() -> Self {
        Self {
            validators: Box::new([]),
        }
    }

    /// Creates a behavior with one typed validator.
    pub fn single<V>(validator: V) -> Self
    where
        V: Validator<M> + 'static,
    {
        Self::new([Arc::new(validator) as Arc<dyn Validator<M>>])
    }
}

impl<M> Default for ValidationBehavior<M>
where
    M: Request,
{
    fn default() -> Self {
        Self::empty()
    }
}

#[async_trait]
impl<M> Behavior<M> for ValidationBehavior<M>
where
    M: Request,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        if self.validators.is_empty() {
            return next.run(message).await;
        }

        let mut errors = Vec::new();
        for validator in self.validators.iter() {
            validator.validate(&message, &mut errors).await?;
        }
        if errors.is_empty() {
            next.run(message).await
        } else {
            Err(validation_error(&errors))
        }
    }
}

pub(crate) fn validation_error(errors: &[Box<str>]) -> CatgaError {
    format_validation_errors(errors, "validation failed: ")
}
