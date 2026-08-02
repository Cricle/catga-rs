use std::{
    any::{Any, TypeId},
    marker::PhantomData,
    sync::Arc,
};

use async_trait::async_trait;

use crate::{
    CatgaError, CatgaResult, Command, CommandHandler, ErrorCode, Event, EventHandler, Handler,
    Request,
};

type ErasedMessage = Box<dyn Any + Send>;

#[async_trait]
pub(crate) trait ErasedRequestHandler: Send + Sync {
    async fn handle(&self, message: ErasedMessage) -> CatgaResult<ErasedMessage>;
}

#[async_trait]
pub(crate) trait ErasedCommandHandler: Send + Sync {
    async fn handle(&self, command: ErasedMessage) -> CatgaResult<()>;
}

#[async_trait]
pub(crate) trait ErasedEventHandler: Send + Sync {
    async fn handle(&self, event: ErasedMessage) -> CatgaResult<()>;
}

struct RequestHandlerAdapter<M, H> {
    handler: H,
    marker: PhantomData<fn(M)>,
}

#[async_trait]
impl<M, H> ErasedRequestHandler for RequestHandlerAdapter<M, H>
where
    M: Request,
    H: Handler<M> + 'static,
{
    async fn handle(&self, message: ErasedMessage) -> CatgaResult<ErasedMessage> {
        let message = message.downcast::<M>().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "request handler received an invalid message type",
            )
        })?;
        self.handler
            .handle(*message)
            .await
            .map(|response| Box::new(response) as ErasedMessage)
    }
}

struct EventHandlerAdapter<E, H> {
    handler: H,
    marker: PhantomData<fn(E)>,
}

struct CommandHandlerAdapter<C, H> {
    handler: H,
    marker: PhantomData<fn(C)>,
}

#[async_trait]
impl<E, H> ErasedEventHandler for EventHandlerAdapter<E, H>
where
    E: Event,
    H: EventHandler<E> + 'static,
{
    async fn handle(&self, event: ErasedMessage) -> CatgaResult<()> {
        let event = event.downcast::<E>().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "event handler received an invalid event type",
            )
        })?;
        self.handler.handle(*event).await
    }
}

#[async_trait]
impl<C, H> ErasedCommandHandler for CommandHandlerAdapter<C, H>
where
    C: Command,
    H: CommandHandler<C> + 'static,
{
    async fn handle(&self, command: ErasedMessage) -> CatgaResult<()> {
        let command = command.downcast::<C>().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "command handler received an invalid message type",
            )
        })?;
        self.handler.handle(*command).await
    }
}

/// One registered request handler slot in the dispatch table.
pub(crate) struct RequestSlot {
    pub(crate) type_id: TypeId,
    pub(crate) handler: Arc<dyn ErasedRequestHandler>,
}

/// One registered command handler slot in the dispatch table.
pub(crate) struct CommandSlot {
    pub(crate) type_id: TypeId,
    pub(crate) handler: Arc<dyn ErasedCommandHandler>,
}

/// One registered event handler slot in the dispatch table.
pub(crate) struct EventSlot {
    pub(crate) type_id: TypeId,
    pub(crate) handlers: Vec<Arc<dyn ErasedEventHandler>>,
}

/// The explicit startup-time map of request, command, and event handlers.
///
/// Internally uses contiguous `Vec` slots instead of `HashMap` for cache-friendly
/// linear-scan dispatch. For typical applications with 5–30 registered message types,
/// this outperforms hashing due to contiguous memory layout, zero hash computation,
/// and predictable branch behavior.
///
/// ```no_run
/// use async_trait::async_trait;
/// use catga_core::{CatgaResult, Handler, Mediator, Message, MessageTypeId, Registry, Request};
///
/// struct PingTypeId;
/// impl MessageTypeId for PingTypeId { const NAME: &'static str = "Ping"; }
///
/// struct Ping;
/// impl Message for Ping {}
/// impl Request for Ping { type Response = &'static str; type TypeId = PingTypeId; }
///
/// struct PingHandler;
/// #[async_trait]
/// impl Handler<Ping> for PingHandler {
///     async fn handle(&self, _: Ping) -> CatgaResult<&'static str> { Ok("pong") }
/// }
///
/// # async fn run() -> CatgaResult<()> {
/// let mut registry = Registry::new();
/// registry.register_request::<Ping, _>(PingHandler)?;
/// let mediator = Mediator::new(registry);
/// assert_eq!(mediator.send(Ping).await?, "pong");
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct Registry {
    pub(crate) requests: Vec<RequestSlot>,
    pub(crate) commands: Vec<CommandSlot>,
    pub(crate) events: Vec<EventSlot>,
}

impl Registry {
    /// Creates an empty handler registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the sole handler for a request type.
    ///
    /// Registering another handler for the same request returns
    /// [`ErrorCode::Conflict`] and preserves the original handler.
    pub fn register_request<M, H>(&mut self, handler: H) -> CatgaResult<()>
    where
        M: Request,
        H: Handler<M> + 'static,
    {
        let type_id = TypeId::of::<M>();
        if self.requests.iter().any(|slot| slot.type_id == type_id) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "request handler is already registered",
            ));
        }
        self.requests.push(RequestSlot {
            type_id,
            handler: Arc::new(RequestHandlerAdapter::<M, H> {
                handler,
                marker: PhantomData,
            }),
        });
        Ok(())
    }

    /// Registers the sole handler for a command type.
    ///
    /// Registering another handler for the same command returns
    /// [`ErrorCode::Conflict`] and preserves the original handler.
    pub fn register_command<C, H>(&mut self, handler: H) -> CatgaResult<()>
    where
        C: Command,
        H: CommandHandler<C> + 'static,
    {
        let type_id = TypeId::of::<C>();
        if self.commands.iter().any(|slot| slot.type_id == type_id) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "command handler is already registered",
            ));
        }
        self.commands.push(CommandSlot {
            type_id,
            handler: Arc::new(CommandHandlerAdapter::<C, H> {
                handler,
                marker: PhantomData,
            }),
        });
        Ok(())
    }

    /// Registers an additional handler for an event type.
    pub fn register_event<E, H>(&mut self, handler: H)
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        let type_id = TypeId::of::<E>();
        if let Some(slot) = self.events.iter_mut().find(|slot| slot.type_id == type_id) {
            slot.handlers.push(Arc::new(EventHandlerAdapter::<E, H> {
                handler,
                marker: PhantomData,
            }));
        } else {
            self.events.push(EventSlot {
                type_id,
                handlers: vec![Arc::new(EventHandlerAdapter::<E, H> {
                    handler,
                    marker: PhantomData,
                })],
            });
        }
    }
}
