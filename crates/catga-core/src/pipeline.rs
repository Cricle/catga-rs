use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{CatgaResult, Request};

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
