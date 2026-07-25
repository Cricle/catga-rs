use std::sync::Arc;

use async_trait::async_trait;

use crate::{Behavior, CatgaResult, Fault, Mediator, Next, Request};

/// Publishes faults without affecting the request result when publication fails.
#[async_trait]
pub trait FaultPublisher<M>: Send + Sync
where
    M: Request + Clone,
{
    /// Publishes one request-processing fault.
    async fn publish(&self, fault: Fault<M>) -> CatgaResult<()>;
}

#[async_trait]
impl<M> FaultPublisher<M> for Mediator
where
    M: Request + Clone,
{
    async fn publish(&self, fault: Fault<M>) -> CatgaResult<()> {
        Mediator::publish(self, fault).await
    }
}

/// Converts failed request results into best-effort [`Fault`] events.
pub struct FaultPublishingBehavior<P: ?Sized> {
    publisher: Arc<P>,
}

impl<P: ?Sized> FaultPublishingBehavior<P> {
    /// Creates a behavior using the supplied fault publisher.
    pub fn new(publisher: Arc<P>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl<M, P> Behavior<M> for FaultPublishingBehavior<P>
where
    M: Request + Clone,
    P: FaultPublisher<M> + ?Sized,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let original = message.clone();
        match next.run(message).await {
            Err(error) => {
                let _ = self
                    .publisher
                    .publish(Fault::new(original, error.clone()))
                    .await;
                Err(error)
            }
            result => result,
        }
    }
}
