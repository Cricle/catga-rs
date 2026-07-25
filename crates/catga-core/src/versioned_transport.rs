//! Transport receive-side event schema upgrades.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{CatgaResult, Delivery, Envelope, EventVersionRegistry, MessageTransport};

/// Applies registered event schema upgrades as messages are received.
///
/// Publishing remains a direct pass-through: callers set the current schema on
/// [`Envelope`] when they construct it. Receive-side upgrades preserve the
/// backend acknowledgement token, so the wrapped transport keeps its delivery
/// semantics unchanged.
pub struct VersionedMessageTransport<T> {
    inner: Arc<T>,
    versions: Arc<EventVersionRegistry>,
}

impl<T> VersionedMessageTransport<T> {
    /// Wraps `inner` with the immutable-read version registry.
    pub fn new(inner: Arc<T>, versions: Arc<EventVersionRegistry>) -> Self {
        Self { inner, versions }
    }
}

#[async_trait]
impl<T> MessageTransport for VersionedMessageTransport<T>
where
    T: MessageTransport,
{
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.inner.publish(envelope).await
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let delivery = self.inner.receive().await?;
        delivery.map_envelope(|envelope| self.versions.upgrade_to_latest(envelope))
    }
}
