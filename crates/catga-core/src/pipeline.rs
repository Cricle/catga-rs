use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{CatgaError, CatgaResult, ErrorCode, Request};

/// Maximum number of behaviors that can wrap one request dispatch.
///
/// This preserves the upstream's fixed recursion bound and prevents an accidentally generated
/// behavior list from consuming unbounded startup memory or overflowing the dispatch chain.
pub const MAX_PIPELINE_DEPTH: usize = 100;

type Continuation<M> =
    dyn Fn(M) -> BoxFuture<'static, CatgaResult<<M as Request>::Response>> + Send + Sync;

/// Invokes the next behavior or the registered request handler in a pipeline.
pub struct Next<M: Request> {
    continuation: Arc<Continuation<M>>,
}

impl<M: Request> Clone for Next<M> {
    fn clone(&self) -> Self {
        Self {
            continuation: Arc::clone(&self.continuation),
        }
    }
}

impl<M: Request> Next<M> {
    pub(crate) fn new(
        continuation: impl Fn(M) -> BoxFuture<'static, CatgaResult<M::Response>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            continuation: Arc::new(continuation),
        }
    }

    /// Continues request processing with the supplied message.
    pub fn run(&self, message: M) -> BoxFuture<'static, CatgaResult<M::Response>> {
        (self.continuation)(message)
    }
}

/// Wraps typed request processing before and after the next pipeline stage.
#[async_trait]
pub trait Behavior<M: Request>: Send + Sync {
    /// Handles a request and optionally invokes the next behavior or request handler.
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response>;
}

/// An immutable, typed sequence of request behaviors built during application startup.
pub struct Pipeline<M: Request> {
    behaviors: Vec<Arc<dyn Behavior<M>>>,
}

impl<M: Request> Default for Pipeline<M> {
    fn default() -> Self {
        Self {
            behaviors: Vec::new(),
        }
    }
}

impl<M: Request> Pipeline<M> {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a behavior after the existing stages.
    pub fn with<B>(mut self, behavior: B) -> Self
    where
        B: Behavior<M> + 'static,
    {
        self.behaviors.push(Arc::new(behavior));
        self
    }

    /// Adds a shared behavior after the existing stages.
    pub fn with_shared(mut self, behavior: Arc<dyn Behavior<M>>) -> Self {
        self.behaviors.push(behavior);
        self
    }

    /// Adds a behavior while rejecting a pipeline deeper than [`MAX_PIPELINE_DEPTH`].
    ///
    /// Prefer this fallible builder for generated or configuration-driven pipelines. The legacy
    /// [`Self::with`] builder remains available for source compatibility; dispatch still rejects
    /// any oversized legacy pipeline before invoking a behavior or handler.
    pub fn try_with<B>(self, behavior: B) -> CatgaResult<Self>
    where
        B: Behavior<M> + 'static,
    {
        self.try_with_shared(Arc::new(behavior))
    }

    /// Adds a shared behavior while enforcing [`MAX_PIPELINE_DEPTH`].
    pub fn try_with_shared(mut self, behavior: Arc<dyn Behavior<M>>) -> CatgaResult<Self> {
        if self.behaviors.len() >= MAX_PIPELINE_DEPTH {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "pipeline depth exceeds the supported maximum",
            ));
        }
        self.behaviors.push(behavior);
        Ok(self)
    }

    /// Returns the number of configured behaviors.
    pub const fn len(&self) -> usize {
        self.behaviors.len()
    }

    /// Returns whether this pipeline has no configured behaviors.
    pub const fn is_empty(&self) -> bool {
        self.behaviors.is_empty()
    }

    pub(crate) fn wrap(&self, terminal: Next<M>) -> Next<M> {
        self.behaviors
            .iter()
            .rev()
            .fold(terminal, |next, behavior| {
                let behavior = Arc::clone(behavior);
                Next::new(move |message| {
                    let behavior = Arc::clone(&behavior);
                    let next = next.clone();
                    Box::pin(async move { behavior.handle(message, next).await })
                })
            })
    }
}
