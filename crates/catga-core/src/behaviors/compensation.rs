use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;

use crate::{Behavior, CatgaError, CatgaResult, Event, Mediator, Next, Request};

/// Publishes compensating work after a typed request fails.
#[async_trait]
pub trait CompensationPublisher<M>: Send + Sync
where
    M: Request,
{
    /// Publishes compensating work for `request` and its original handler error.
    async fn publish(&self, request: &M, error: &CatgaError) -> CatgaResult<()>;
}

/// Adapts a synchronous compensation-event factory to the existing mediator event fan-out.
pub struct EventCompensationPublisher<M, E, F>
where
    M: Request,
    E: Event,
{
    mediator: Arc<Mediator>,
    factory: F,
    marker: PhantomData<fn(M) -> E>,
}

impl<M, E, F> EventCompensationPublisher<M, E, F>
where
    M: Request,
    E: Event,
    F: Fn(&M, &CatgaError) -> Option<E>,
{
    /// Creates a publisher from a mediator and a factory that may opt out of compensation.
    pub fn new(mediator: Arc<Mediator>, factory: F) -> Self {
        Self {
            mediator,
            factory,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<M, E, F> CompensationPublisher<M> for EventCompensationPublisher<M, E, F>
where
    M: Request,
    E: Event,
    F: Fn(&M, &CatgaError) -> Option<E> + Send + Sync,
{
    async fn publish(&self, request: &M, error: &CatgaError) -> CatgaResult<()> {
        match (self.factory)(request, error) {
            Some(event) => self.mediator.publish(event).await,
            None => Ok(()),
        }
    }
}

/// Executes best-effort compensation after the next handler returns an error.
pub struct CompensationBehavior<P: ?Sized> {
    publisher: Arc<P>,
}

impl<P: ?Sized> CompensationBehavior<P> {
    /// Creates a compensation behavior backed by `publisher`.
    pub fn new(publisher: Arc<P>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl<M, P> Behavior<M> for CompensationBehavior<P>
where
    M: Request + Clone,
    P: CompensationPublisher<M> + ?Sized,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let original = message.clone();
        match next.run(message).await {
            Err(error) => {
                let _ = self.publisher.publish(&original, &error).await;
                Err(error)
            }
            result => result,
        }
    }
}
