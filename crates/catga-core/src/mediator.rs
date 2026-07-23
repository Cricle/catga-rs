use std::{
    any::{Any, TypeId},
    sync::Arc,
};

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
