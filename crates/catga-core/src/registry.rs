use std::{
    any::{Any, TypeId},
    collections::HashMap,
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

/// The explicit startup-time map of request, command, and event handlers.
#[derive(Default)]
pub struct Registry {
    pub(crate) requests: HashMap<TypeId, Arc<dyn ErasedRequestHandler>>,
    pub(crate) commands: HashMap<TypeId, Arc<dyn ErasedCommandHandler>>,
    pub(crate) events: HashMap<TypeId, Vec<Arc<dyn ErasedEventHandler>>>,
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
        if self.requests.contains_key(&TypeId::of::<M>()) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "request handler is already registered",
            ));
        }
        self.requests.insert(
            TypeId::of::<M>(),
            Arc::new(RequestHandlerAdapter::<M, H> {
                handler,
                marker: PhantomData,
            }),
        );
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
        if self.commands.contains_key(&TypeId::of::<C>()) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "command handler is already registered",
            ));
        }
        self.commands.insert(
            TypeId::of::<C>(),
            Arc::new(CommandHandlerAdapter::<C, H> {
                handler,
                marker: PhantomData,
            }),
        );
        Ok(())
    }

    /// Registers an additional handler for an event type.
    pub fn register_event<E, H>(&mut self, handler: H)
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        self.events
            .entry(TypeId::of::<E>())
            .or_default()
            .push(Arc::new(EventHandlerAdapter::<E, H> {
                handler,
                marker: PhantomData,
            }));
    }
}
