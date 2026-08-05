//! ## Handler Trait Conformance
//!
//! Handlers must implement the appropriate trait explicitly:
//!
//!
use std::{future::Future, marker::PhantomData};

use async_trait::async_trait;

use crate::{CatgaResult, Command, Event, Request};

/// Handles one request and returns its typed response.
///
/// # Example
///
/// ```
/// use async_trait::async_trait;
/// use catga_core::{CatgaResult, Handler, Message, MessageTypeId, Request};
///
/// struct PingTypeId;
/// impl MessageTypeId for PingTypeId { const NAME: &'static str = "Ping"; }
///
/// struct Ping;
/// impl Message for Ping {}
/// impl Request for Ping { type Response = u64; type TypeId = PingTypeId; }
///
/// struct PingHandler;
/// #[async_trait]
/// impl Handler<Ping> for PingHandler {
///     async fn handle(&self, _: Ping) -> CatgaResult<u64> { Ok(42) }
/// }
/// ```
#[async_trait]
pub trait Handler<M: Request>: Send + Sync {
    /// Handles the request.
    async fn handle(&self, message: M) -> CatgaResult<M::Response>;
}

/// Handles one command without returning a response value.
///
/// # Example
///
/// ```
/// use async_trait::async_trait;
/// use catga_core::{CatgaResult, Command, CommandHandler, Message, MessageTypeId};
///
/// struct ArchiveTypeId;
/// impl MessageTypeId for ArchiveTypeId { const NAME: &'static str = "Archive"; }
///
/// struct Archive;
/// impl Message for Archive {}
/// impl Command for Archive { type TypeId = ArchiveTypeId; }
///
/// struct ArchiveHandler;
/// #[async_trait]
/// impl CommandHandler<Archive> for ArchiveHandler {
///     async fn handle(&self, _: Archive) -> CatgaResult<()> { Ok(()) }
/// }
/// ```
#[async_trait]
pub trait CommandHandler<C: Command>: Send + Sync {
    /// Handles the command.
    async fn handle(&self, command: C) -> CatgaResult<()>;
}

/// Handles one event delivery.
///
/// # Example
///
/// ```
/// use async_trait::async_trait;
/// use catga_core::{CatgaResult, Event, EventHandler, Message, MessageTypeId};
///
/// struct UserCreatedTypeId;
/// impl MessageTypeId for UserCreatedTypeId { const NAME: &'static str = "UserCreated"; }
///
/// #[derive(Clone)]
/// struct UserCreated { pub user_id: u64 }
/// impl Message for UserCreated {}
/// impl Event for UserCreated { type TypeId = UserCreatedTypeId; }
///
/// struct UserProjection;
/// #[async_trait]
/// impl EventHandler<UserCreated> for UserProjection {
///     async fn handle(&self, event: UserCreated) -> CatgaResult<()> {
///         println!("Projecting user: {}", event.user_id);
///         Ok(())
///     }
/// }
/// ```
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
/// use catga_core::{CatgaResult, Mediator, Message, MessageTypeId, Registry, Request, request_handler};
///
/// struct DoubleTypeId;
/// impl MessageTypeId for DoubleTypeId { const NAME: &'static str = "Double"; }
///
/// struct Double(u64);
/// impl Message for Double {}
/// impl Request for Double { type Response = u64; type TypeId = DoubleTypeId; }
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

/// Builds a request handler from explicit cloneable context and an async function.
///
/// Use this when several handlers share an application-owned dependency such as an `Arc` to a
/// store or gateway. The dependency remains visible at composition time, while each dispatch
/// receives its own clone without handwritten capture closures:
///
/// ```
/// use std::sync::Arc;
/// use catga_core::{CatgaResult, Mediator, Message, MessageTypeId, Registry, Request, request_handler_with};
///
/// struct DoubleTypeId;
/// impl MessageTypeId for DoubleTypeId { const NAME: &'static str = "Double"; }
///
/// struct Double(u64);
/// impl Message for Double {}
/// impl Request for Double { type Response = u64; type TypeId = DoubleTypeId; }
///
/// async fn double(factor: Arc<u64>, value: Double) -> CatgaResult<u64> {
///     Ok(value.0.saturating_mul(*factor))
/// }
///
/// # async fn run() -> CatgaResult<()> {
/// let mut registry = Registry::new();
/// registry.register_request::<Double, _>(request_handler_with(Arc::new(2), double))?;
/// assert_eq!(Mediator::new(registry).send(Double(21)).await?, 42);
/// # Ok(())
/// # }
/// ```
pub fn request_handler_with<Context, M, F, Fut>(context: Context, handler: F) -> impl Handler<M>
where
    Context: Clone + Send + Sync + 'static,
    M: Request,
    F: Fn(Context, M) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = CatgaResult<M::Response>> + Send,
{
    request_handler(move |message| handler(context.clone(), message))
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

/// Builds a command handler from explicit cloneable context and an async function.
///
/// This has the same explicit dependency model as [`request_handler_with`], for commands whose
/// handler returns no response.
pub fn command_handler_with<Context, C, F, Fut>(
    context: Context,
    handler: F,
) -> impl CommandHandler<C>
where
    Context: Clone + Send + Sync + 'static,
    C: Command,
    F: Fn(Context, C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    command_handler(move |command| handler(context.clone(), command))
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

/// Builds an event handler from explicit cloneable context and an async function.
///
/// This has the same explicit dependency model as [`request_handler_with`], while allowing a
/// registry to attach the resulting handler alongside other handlers for the same event.
pub fn event_handler_with<Context, E, F, Fut>(context: Context, handler: F) -> impl EventHandler<E>
where
    Context: Clone + Send + Sync + 'static,
    E: Event,
    F: Fn(Context, E) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    event_handler(move |event| handler(context.clone(), event))
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
