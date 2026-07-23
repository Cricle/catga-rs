use async_trait::async_trait;

use crate::{CatgaResult, Envelope};

/// A message received from a transport and awaiting acknowledgement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    envelope: Envelope,
}

impl Delivery {
    /// Creates a delivery around a received envelope.
    pub fn new(envelope: Envelope) -> Self {
        Self { envelope }
    }

    /// Returns the delivered envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
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
    async fn ack(&self, delivery: Delivery) -> CatgaResult<()>;
}
