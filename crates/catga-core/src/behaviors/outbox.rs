//! Durable outbox pipeline composition.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{Behavior, CatgaResult, Envelope, Next, OutboxMessage, OutboxStore, Request};

/// Converts a successful request into the envelope retained for asynchronous delivery.
///
/// Implementations should use a stable identifier and type name so a later
/// [`crate::OutboxProcessor`] can publish the message through any transport.
pub trait OutboxEnvelope {
    /// Builds the durable envelope retained after successful request processing.
    fn outbox_envelope(&self) -> Envelope;
}

/// Persists successful request envelopes for a separate [`crate::OutboxProcessor`].
///
/// This behavior deliberately owns no transport or worker state. It therefore
/// has no hidden background task and composes with every [`OutboxStore`]. The
/// store interface has no transaction boundary, so atomic coordination with a
/// handler's own persistence must be supplied by that store or application.
pub struct OutboxBehavior {
    store: Arc<dyn OutboxStore>,
}

impl OutboxBehavior {
    /// Creates a behavior backed by `store`.
    pub fn new<S>(store: Arc<S>) -> Self
    where
        S: OutboxStore + 'static,
    {
        Self { store }
    }
}

#[async_trait]
impl<M> Behavior<M> for OutboxBehavior
where
    M: Request + OutboxEnvelope,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let envelope = message.outbox_envelope();
        let response = next.run(message).await?;
        self.store.enqueue(OutboxMessage::new(envelope)).await?;
        Ok(response)
    }
}
