use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use futures::{Stream, StreamExt, stream};

use crate::{CatgaError, CatgaResult, ErrorCode, Event, Registry, Request};

/// Dispatches typed requests and events through an immutable handler registry.
pub struct Mediator {
    registry: Arc<Registry>,
}

impl Mediator {
    /// Creates a mediator from an explicit registry built during application startup.
    pub fn new(registry: Registry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    /// Routes a request to its sole registered handler.
    pub async fn send<M: Request>(&self, message: M) -> CatgaResult<M::Response> {
        let handler = self
            .registry
            .requests
            .get(&TypeId::of::<M>())
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::NotFound, "request handler is not registered")
            })?;
        let response = handler.handle(Box::new(message)).await?;
        response
            .downcast::<M::Response>()
            .map(|response| *response)
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "request handler returned an invalid response type",
                )
            })
    }

    /// Routes requests concurrently while preserving their input order.
    pub async fn send_batch<M>(
        &self,
        messages: impl IntoIterator<Item = M>,
        concurrency_limit: usize,
    ) -> CatgaResult<Vec<CatgaResult<M::Response>>>
    where
        M: Request,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch concurrency limit must be greater than zero",
            ));
        }

        Ok(stream::iter(messages)
            .map(|message| self.send(message))
            .buffered(concurrency_limit)
            .collect()
            .await)
    }

    /// Lazily routes every request produced by a stream.
    pub fn send_stream<'a, M, S>(
        &'a self,
        messages: S,
    ) -> impl Stream<Item = CatgaResult<M::Response>> + 'a
    where
        M: Request,
        S: Stream<Item = M> + Send + 'a,
    {
        messages.then(move |message| self.send(message))
    }

    /// Delivers an event to every registered handler in registration order.
    pub async fn publish<E: Event>(&self, event: E) -> CatgaResult<()> {
        if let Some(handlers) = self.registry.events.get(&TypeId::of::<E>()) {
            for handler in handlers {
                handler
                    .handle(Box::new(event.clone()) as Box<dyn Any + Send>)
                    .await?;
            }
        }
        Ok(())
    }
}
