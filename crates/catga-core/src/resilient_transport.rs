//! Composable bounded-resilience wrapper for transport implementations.

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaResult, Delivery, Destination, DestinationTransport, Envelope, MessageTransport,
    ResilienceExecutor,
};

/// Applies caller-owned resilience policies around a transport without coupling adapters to one
/// backend-specific retry implementation.
///
/// `publish` and `send_to` use `write_executor`; configure it with zero retries unless the
/// backend operation is explicitly idempotent. `receive` uses `read_executor`, which may safely
/// use retries for transient broker failures.
pub struct ResilientTransport<T: ?Sized> {
    inner: Arc<T>,
    write_executor: Arc<ResilienceExecutor>,
    read_executor: Arc<ResilienceExecutor>,
}

impl<T: ?Sized> ResilientTransport<T> {
    /// Wraps `inner` with independently shareable read and write resilience budgets.
    #[must_use]
    pub fn new(
        inner: Arc<T>,
        write_executor: Arc<ResilienceExecutor>,
        read_executor: Arc<ResilienceExecutor>,
    ) -> Self {
        Self {
            inner,
            write_executor,
            read_executor,
        }
    }

    /// Returns the wrapped transport.
    #[must_use]
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }
}

#[async_trait]
impl<T> MessageTransport for ResilientTransport<T>
where
    T: MessageTransport + ?Sized,
{
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.write_executor
            .execute(CancellationToken::new(), |_| {
                let inner = Arc::clone(&self.inner);
                let envelope = envelope.clone();
                async move { inner.publish(envelope).await }
            })
            .await
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.read_executor
            .execute(CancellationToken::new(), |_| {
                let inner = Arc::clone(&self.inner);
                async move { inner.receive().await }
            })
            .await
    }
}

#[async_trait]
impl<T> DestinationTransport for ResilientTransport<T>
where
    T: DestinationTransport + ?Sized,
{
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        let destination = destination.clone();
        self.write_executor
            .execute(CancellationToken::new(), |_| {
                let inner = Arc::clone(&self.inner);
                let destination = destination.clone();
                let envelope = envelope.clone();
                async move { inner.send_to(&destination, envelope).await }
            })
            .await
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        let destination = destination.clone();
        self.read_executor
            .execute(CancellationToken::new(), |_| {
                let inner = Arc::clone(&self.inner);
                let destination = destination.clone();
                async move { inner.receive_from(&destination).await }
            })
            .await
    }
}
