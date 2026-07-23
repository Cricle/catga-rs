use async_trait::async_trait;

use crate::{CatgaResult, Envelope};

/// A message received from a transport and awaiting acknowledgement.
pub struct Delivery {
    envelope: Envelope,
    acknowledger: Option<Box<dyn Acknowledger>>,
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("envelope", &self.envelope)
            .field("requires_ack", &self.acknowledger.is_some())
            .finish()
    }
}

/// Performs the backend-specific acknowledgement for one delivery.
#[async_trait]
pub trait Acknowledger: Send {
    /// Commits successful processing exactly once.
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()>;
}

impl Delivery {
    /// Creates a delivery around a received envelope.
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            acknowledger: None,
        }
    }

    /// Creates a delivery that owns its backend-specific acknowledgement token.
    pub fn with_acknowledger(envelope: Envelope, acknowledger: Box<dyn Acknowledger>) -> Self {
        Self {
            envelope,
            acknowledger: Some(acknowledger),
        }
    }

    /// Returns the delivered envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Consumes the delivery and commits its backend acknowledgement when required.
    pub async fn acknowledge(mut self) -> CatgaResult<()> {
        match self.acknowledger.take() {
            Some(acknowledger) => acknowledger.acknowledge().await,
            None => Ok(()),
        }
    }
}

/// Sends envelopes and receives acknowledged deliveries.
#[async_trait]
pub trait MessageTransport: Send + Sync {
    /// Publishes an envelope, applying the transport's configured backpressure.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;

    /// Receives the next delivery for the configured consumer.
    async fn receive(&self) -> CatgaResult<Delivery>;

    /// Acknowledges successful processing of a delivery.
    async fn ack(&self, delivery: Delivery) -> CatgaResult<()> {
        delivery.acknowledge().await
    }
}
