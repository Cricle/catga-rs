use async_trait::async_trait;

use crate::{CatgaResult, Command, Event, Request};

/// Handles one request and returns its typed response.
#[async_trait]
pub trait Handler<M: Request>: Send + Sync {
    /// Handles the request.
    async fn handle(&self, message: M) -> CatgaResult<M::Response>;
}

/// Handles one command without returning a response value.
#[async_trait]
pub trait CommandHandler<C: Command>: Send + Sync {
    /// Handles the command.
    async fn handle(&self, command: C) -> CatgaResult<()>;
}

/// Handles one event delivery.
#[async_trait]
pub trait EventHandler<E: Event>: Send + Sync {
    /// Handles the event.
    async fn handle(&self, event: E) -> CatgaResult<()>;
}
