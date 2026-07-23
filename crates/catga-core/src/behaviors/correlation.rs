use async_trait::async_trait;

use crate::{Behavior, CatgaResult, Correlated, Next, Request, correlation::scope};

/// Propagates one request's correlation identity through its asynchronous handler chain.
pub struct CorrelationBehavior;

#[async_trait]
impl<M> Behavior<M> for CorrelationBehavior
where
    M: Request + Correlated,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let metadata = message.metadata();
        let correlation_id = metadata.correlation_id().unwrap_or(metadata.message_id());
        scope(correlation_id, next.run(message)).await
    }
}
