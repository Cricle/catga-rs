use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    CatgaResult, DistributedIdGenerator, Envelope, Message, MessageTransport, PayloadEncoder,
    TransportContext, build_publish_metadata, current_transport_context,
};

/// Publishes caller-owned envelopes without requiring a receive capability.
#[async_trait]
pub trait EnvelopePublisher: Send + Sync {
    /// Publishes one fully constructed envelope.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()>;
}

#[async_trait]
impl<T: MessageTransport + ?Sized> EnvelopePublisher for T {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        MessageTransport::publish(self, envelope).await
    }
}

/// Builds durable envelopes from typed messages for a publish-only backend.
pub struct TypedPublisher<T: ?Sized, C> {
    publisher: Arc<T>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: Arc<C>,
}

impl<T: ?Sized, C> Clone for TypedPublisher<T, C> {
    fn clone(&self) -> Self {
        Self {
            publisher: Arc::clone(&self.publisher),
            id_generator: Arc::clone(&self.id_generator),
            codec: Arc::clone(&self.codec),
        }
    }
}

impl<T: ?Sized, C> TypedPublisher<T, C> {
    /// Creates a typed publisher from caller-owned dependencies.
    pub fn new_with_codec(
        publisher: Arc<T>,
        id_generator: Arc<dyn DistributedIdGenerator>,
        codec: C,
    ) -> Self {
        Self::new_with_shared_codec(publisher, id_generator, Arc::new(codec))
    }

    /// Creates a typed publisher using an already shared codec.
    pub fn new_with_shared_codec(
        publisher: Arc<T>,
        id_generator: Arc<dyn DistributedIdGenerator>,
        codec: Arc<C>,
    ) -> Self {
        Self {
            publisher,
            id_generator,
            codec,
        }
    }
}

impl<T, C> TypedPublisher<T, C>
where
    T: EnvelopePublisher + ?Sized,
{
    /// Serializes and publishes one typed message with at-least-once metadata.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        let (id, metadata) = build_publish_metadata(&*self.id_generator, message)?;
        let context = current_transport_context();
        let envelope = Envelope::versioned(
            id,
            message.message_type(),
            self.codec.encode_payload(message)?,
            metadata,
            message.schema_version(),
        );
        let envelope = match context.as_ref().and_then(TransportContext::headers) {
            Some(headers) => envelope.with_headers(headers.clone()),
            None => envelope,
        };
        self.publisher.publish(envelope).await
    }
}
