use std::{future::Future, marker::PhantomData};

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

/// A typed request handler backed by one explicit async closure.
///
/// Construct this with [`request_handler`]. It is useful when a handler's dependencies are
/// already captured by an application-owned closure and defining a separate struct would add no
/// behavior. The closure is still registered through [`crate::Registry`] during startup; it is
/// not a global callback or a runtime service lookup.
pub struct RequestHandlerFn<M, F> {
    handler: F,
    marker: PhantomData<fn(M)>,
}

/// Builds a request handler from an async closure.
///
/// The message type is normally inferred by [`crate::Registry::register_request`] or
/// [`crate::catga_handlers!`]:
///
/// ```
/// use catga_core::{CatgaResult, Mediator, Message, Registry, Request, request_handler};
///
/// struct Double(u64);
/// impl Message for Double {}
/// impl Request for Double { type Response = u64; }
///
/// # async fn run() -> CatgaResult<()> {
/// let mut registry = Registry::new();
/// registry.register_request::<Double, _>(request_handler(|value: Double| async move {
///     Ok(value.0.saturating_mul(2))
/// }))?;
/// let mediator = Mediator::new(registry);
/// assert_eq!(mediator.send(Double(21)).await?, 42);
/// # Ok(())
/// # }
/// ```
pub fn request_handler<M, F, Fut>(handler: F) -> RequestHandlerFn<M, F>
where
    M: Request,
    F: Fn(M) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<M::Response>> + Send,
{
    RequestHandlerFn {
        handler,
        marker: PhantomData,
    }
}

#[async_trait]
impl<M, F, Fut> Handler<M> for RequestHandlerFn<M, F>
where
    M: Request,
    F: Fn(M) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<M::Response>> + Send,
{
    async fn handle(&self, message: M) -> CatgaResult<M::Response> {
        (self.handler)(message).await
    }
}

/// A typed command handler backed by one explicit async closure.
///
/// Construct this with [`command_handler`].
pub struct CommandHandlerFn<C, F> {
    handler: F,
    marker: PhantomData<fn(C)>,
}

/// Builds a command handler from an async closure.
///
/// The command type is normally inferred by [`crate::Registry::register_command`] or
/// [`crate::catga_handlers!`].
pub fn command_handler<C, F, Fut>(handler: F) -> CommandHandlerFn<C, F>
where
    C: Command,
    F: Fn(C) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    CommandHandlerFn {
        handler,
        marker: PhantomData,
    }
}

#[async_trait]
impl<C, F, Fut> CommandHandler<C> for CommandHandlerFn<C, F>
where
    C: Command,
    F: Fn(C) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    async fn handle(&self, command: C) -> CatgaResult<()> {
        (self.handler)(command).await
    }
}

/// A typed event handler backed by one explicit async closure.
///
/// Construct this with [`event_handler`].
pub struct EventHandlerFn<E, F> {
    handler: F,
    marker: PhantomData<fn(E)>,
}

/// Builds an event handler from an async closure.
///
/// The event type is normally inferred by [`crate::Registry::register_event`] or
/// [`crate::catga_handlers!`].
pub fn event_handler<E, F, Fut>(handler: F) -> EventHandlerFn<E, F>
where
    E: Event,
    F: Fn(E) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    EventHandlerFn {
        handler,
        marker: PhantomData,
    }
}

#[async_trait]
impl<E, F, Fut> EventHandler<E> for EventHandlerFn<E, F>
where
    E: Event,
    F: Fn(E) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    async fn handle(&self, event: E) -> CatgaResult<()> {
        (self.handler)(event).await
    }
}
