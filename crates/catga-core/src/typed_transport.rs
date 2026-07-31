//! Generic, statically typed transport facade.
//!
//! This module deliberately separates payload encoding from payload decoding. A producer only
//! needs [`PayloadEncoder`], while a consumer only needs [`PayloadDecoder`]. The envelope itself
//! stays backend-neutral and contains Catga metadata, correlation, priority, and immutable
//! headers independently of the configured payload format.

use std::{future::Future, sync::Arc};

use futures::{StreamExt, future::BoxFuture, stream};

use crate::{
    CatgaError, CatgaResult, DEFAULT_TRANSPORT_BATCH_CONCURRENCY, Delivery, Destination,
    DestinationTransport, DistributedIdGenerator, Envelope, EnvelopeHeaders, ErrorCode, Event,
    Message, MessageDestinationRouter, MessageMetadata, MessagePriority, MessageTransport,
    PayloadDecoder, PayloadEncoder, QualityOfService, TransportContext, current_correlation_id,
    current_transport_context, scope_transport_context,
};

/// Serializes statically typed values and delegates their delivery to an envelope transport.
///
/// `TypedTransport` owns shared references to a transport, distributed ID generator, and payload
/// codec. It creates one owned [`Envelope`] per publication and never starts background tasks or
/// keeps a message registry. Consequently the backend remains responsible for backpressure and
/// the caller remains responsible for polling, cancellation, and subscription lifetime.
///
/// Payload codecs are intentionally explicit. Catga envelopes do not attach a dynamic codec
/// name, so communicating endpoints must select compatible codecs and schemas themselves.
pub struct TypedTransport<T: ?Sized, C> {
    transport: Arc<T>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: Arc<C>,
}

/// A decoded message that still owns its backend acknowledgement token.
///
/// The typed value is decoded directly from the received envelope payload. Acknowledgement is
/// explicit and consumes this wrapper, preventing the value and acknowledgement token from being
/// resolved more than once.
pub struct TypedDelivery<M> {
    delivery: Delivery,
    message: M,
}

/// The acknowledgement result of one [`TypedTransport::process_next`] call.
///
/// A rejected outcome retains the original application error after a successful negative
/// acknowledgement. If acknowledgement itself fails, that backend error remains the outer
/// [`CatgaResult`] because delivery ownership was not conclusively resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypedProcessOutcome {
    /// The handler succeeded and the delivery was acknowledged.
    Acknowledged,
    /// The handler failed and the delivery was negatively acknowledged for redelivery.
    Rejected(CatgaError),
}

fn outgoing_transport_context(
    message_id: u64,
) -> (u64, Option<MessagePriority>, Option<EnvelopeHeaders>) {
    let transport_context = current_transport_context();
    let correlation_id = transport_context.as_ref().map_or_else(
        || current_correlation_id().unwrap_or(message_id),
        |context| context.correlation_id().unwrap_or(message_id),
    );
    let headers = transport_context
        .as_ref()
        .and_then(TransportContext::headers)
        .cloned();
    let priority = transport_context.as_ref().map(TransportContext::priority);
    (correlation_id, priority, headers)
}

fn resolve_outgoing_headers(
    explicit: Option<&EnvelopeHeaders>,
    inherited: Option<&EnvelopeHeaders>,
) -> CatgaResult<Option<EnvelopeHeaders>> {
    match (inherited, explicit) {
        (Some(inherited), Some(explicit)) => inherited.merge_overrides(explicit).map(Some),
        (Some(inherited), None) => Ok(Some(inherited.clone())),
        (None, Some(explicit)) => Ok(Some(explicit.clone())),
        (None, None) => Ok(None),
    }
}

impl<M> TypedDelivery<M> {
    /// Returns the decoded application message.
    pub const fn message(&self) -> &M {
        &self.message
    }

    /// Returns the serialized envelope associated with this delivery.
    pub const fn envelope(&self) -> &Envelope {
        self.delivery.envelope()
    }

    /// Returns the backend delivery attempt count.
    pub const fn attempts(&self) -> u32 {
        self.delivery.attempts()
    }

    /// Consumes this value and acknowledges the underlying delivery.
    pub async fn acknowledge(self) -> CatgaResult<()> {
        self.delivery.acknowledge().await
    }

    /// Consumes this value and requests backend redelivery.
    pub async fn negative_acknowledge(self) -> CatgaResult<()> {
        self.delivery.negative_acknowledge().await
    }

    /// Shorthand for [`Self::negative_acknowledge`].
    pub async fn nack(self) -> CatgaResult<()> {
        self.negative_acknowledge().await
    }

    /// Runs `future` with this delivery's immutable transport context available.
    ///
    /// Nested typed publication inherits the delivery's correlation ID, priority, and shared
    /// headers. The delivery remains borrowed, retaining the caller's explicit acknowledgement
    /// choice after the future completes.
    pub async fn with_transport_context<T>(&self, future: impl Future<Output = T>) -> T {
        scope_transport_context(self.envelope(), future).await
    }
}

async fn process_typed_delivery<M, H>(
    delivery: TypedDelivery<M>,
    handler: H,
) -> CatgaResult<TypedProcessOutcome>
where
    H: for<'a> FnOnce(&'a M) -> BoxFuture<'a, CatgaResult<()>>,
{
    let result = {
        let message = delivery.message();
        delivery.with_transport_context(handler(message)).await
    };
    match result {
        Ok(()) => {
            delivery.acknowledge().await?;
            Ok(TypedProcessOutcome::Acknowledged)
        }
        Err(error) => {
            delivery.negative_acknowledge().await?;
            Ok(TypedProcessOutcome::Rejected(error))
        }
    }
}

impl<T: ?Sized, C> Clone for TypedTransport<T, C> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            id_generator: Arc::clone(&self.id_generator),
            codec: Arc::clone(&self.codec),
        }
    }
}

impl<T: ?Sized, C> TypedTransport<T, C> {
    /// Creates a typed facade over `transport` using `id_generator` and an owned payload codec.
    ///
    /// The codec is placed in an [`Arc`] once, so cloning the facade is cheap and does not clone
    /// codec state. Use [`Self::new_with_shared_codec`] when the application already shares it.
    pub fn new_with_codec(
        transport: Arc<T>,
        id_generator: Arc<dyn DistributedIdGenerator>,
        codec: C,
    ) -> Self {
        Self::new_with_shared_codec(transport, id_generator, Arc::new(codec))
    }

    /// Creates a typed facade over already shared transport, ID-generator, and codec handles.
    pub fn new_with_shared_codec(
        transport: Arc<T>,
        id_generator: Arc<dyn DistributedIdGenerator>,
        codec: Arc<C>,
    ) -> Self {
        Self {
            transport,
            id_generator,
            codec,
        }
    }
}

impl<T: ?Sized, C: Default> TypedTransport<T, C> {
    /// Creates a typed facade with `C`'s default payload codec.
    ///
    /// Prefer [`Self::new_with_codec`] when codec configuration is part of the wire contract.
    pub fn new(transport: Arc<T>, id_generator: Arc<dyn DistributedIdGenerator>) -> Self {
        Self::new_with_codec(transport, id_generator, C::default())
    }
}

impl<T, C> TypedTransport<T, C>
where
    T: MessageTransport + ?Sized,
{
    /// Publishes one ordinary typed message with at-least-once delivery metadata.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.transport
            .publish(self.encode_envelope(message, QualityOfService::AtLeastOnce)?)
            .await
    }

    /// Publishes one ordinary typed message with immutable transport headers.
    ///
    /// The supplied [`EnvelopeHeaders`] are shared with the outgoing envelope, avoiding header
    /// string copies before backend serialization.
    pub async fn publish_with_headers<M>(
        &self,
        message: &M,
        headers: &EnvelopeHeaders,
    ) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.transport
            .publish(self.encode_envelope_with_headers(
                message,
                QualityOfService::AtLeastOnce,
                Some(headers),
            )?)
            .await
    }

    /// Publishes one event with the source contract's at-most-once default.
    pub async fn publish_event<E>(&self, event: &E) -> CatgaResult<()>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.transport
            .publish(self.encode_envelope(event, QualityOfService::AtMostOnce)?)
            .await
    }

    /// Publishes one event with explicit at-least-once delivery metadata.
    pub async fn publish_reliable_event<E>(&self, event: &E) -> CatgaResult<()>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.transport
            .publish(self.encode_envelope(event, QualityOfService::AtLeastOnce)?)
            .await
    }

    /// Publishes ordinary messages using Catga's default bounded batch concurrency.
    pub async fn publish_batch<M, I>(&self, messages: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.publish_batch_with_concurrency(messages, DEFAULT_TRANSPORT_BATCH_CONCURRENCY)
            .await
    }

    /// Publishes events using Catga's default bounded batch concurrency.
    pub async fn publish_event_batch<E, I>(&self, events: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.publish_event_batch_with_concurrency(events, DEFAULT_TRANSPORT_BATCH_CONCURRENCY)
            .await
    }

    /// Publishes reliable events using Catga's default bounded batch concurrency.
    pub async fn publish_reliable_event_batch<E, I>(&self, events: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.publish_reliable_event_batch_with_concurrency(
            events,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Publishes ordinary messages with a bounded number of active serialization and publish
    /// futures.
    ///
    /// The input is consumed lazily. At most `concurrency_limit` messages are encoded or await
    /// publication at once; zero returns [`ErrorCode::Validation`] before input consumption.
    /// Every item is attempted before the first observed failure is returned.
    pub async fn publish_batch_with_concurrency<M, I>(
        &self,
        messages: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.publish_batch_with_quality_of_service(
            messages,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Publishes events with bounded concurrency and at-most-once metadata.
    pub async fn publish_event_batch_with_concurrency<E, I>(
        &self,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.publish_batch_with_quality_of_service(
            events,
            concurrency_limit,
            QualityOfService::AtMostOnce,
        )
        .await
    }

    /// Publishes reliable events with bounded concurrency and at-least-once metadata.
    pub async fn publish_reliable_event_batch_with_concurrency<E, I>(
        &self,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.publish_batch_with_quality_of_service(
            events,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Receives one envelope, decodes its payload, and retains acknowledgement ownership.
    ///
    /// Decode failures request redelivery before their decoding error is returned. A failed
    /// negative acknowledgement is returned instead because delivery ownership is unresolved.
    pub async fn receive<M>(&self) -> CatgaResult<TypedDelivery<M>>
    where
        C: PayloadDecoder<M>,
    {
        let delivery = self.transport.receive().await?;
        self.decode_delivery(delivery).await
    }

    /// Receives, handles, and resolves one typed delivery.
    ///
    /// The handler runs with the immutable delivery transport context scoped to its future.
    /// Success acknowledges the delivery; handler failure requests redelivery and returns a
    /// [`TypedProcessOutcome::Rejected`] containing the original handler error. This facade never
    /// starts a background task, keeping retry loops and cancellation caller-owned. It is intended
    /// for local composition and tests; production receive loops should use
    /// [`CompetingConsumer`](crate::CompetingConsumer) so concurrency, cancellation, and
    /// acknowledgement draining remain bounded.
    pub async fn process_next<M, H>(&self, handler: H) -> CatgaResult<TypedProcessOutcome>
    where
        C: PayloadDecoder<M>,
        H: for<'a> FnOnce(&'a M) -> BoxFuture<'a, CatgaResult<()>>,
    {
        let delivery = self.receive::<M>().await?;
        process_typed_delivery(delivery, handler).await
    }

    async fn decode_delivery<M>(&self, delivery: Delivery) -> CatgaResult<TypedDelivery<M>>
    where
        C: PayloadDecoder<M>,
    {
        let message = match self.codec.decode_payload(delivery.envelope().payload()) {
            Ok(message) => message,
            Err(error) => {
                delivery.negative_acknowledge().await?;
                return Err(error);
            }
        };
        Ok(TypedDelivery { delivery, message })
    }

    fn encode_envelope<M>(
        &self,
        message: &M,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<Envelope>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.encode_envelope_with_headers(message, quality_of_service, None)
    }

    fn encode_envelope_with_headers<M>(
        &self,
        message: &M,
        quality_of_service: QualityOfService,
        headers: Option<&EnvelopeHeaders>,
    ) -> CatgaResult<Envelope>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        let message_id = self.id_generator.next_id()?;
        let (correlation_id, inherited_priority, inherited_headers) =
            outgoing_transport_context(message_id);
        let envelope = Envelope::versioned(
            message_id,
            message.message_type(),
            self.codec.encode_payload(message)?,
            MessageMetadata::new(message_id, Some(correlation_id))
                .with_quality_of_service(quality_of_service)
                .with_priority(inherited_priority.unwrap_or_else(|| message.priority())),
            message.schema_version(),
        );
        match resolve_outgoing_headers(headers, inherited_headers.as_ref())? {
            Some(headers) => Ok(envelope.with_headers(headers)),
            None => Ok(envelope),
        }
    }

    async fn publish_batch_with_quality_of_service<M, I>(
        &self,
        messages: I,
        concurrency_limit: usize,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "typed transport batch concurrency must be greater than zero",
            ));
        }
        let mut publications = stream::iter(messages)
            .map(|message| async move {
                let envelope = self.encode_envelope(&message, quality_of_service)?;
                self.transport.publish(envelope).await
            })
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = publications.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl<T, C> TypedTransport<T, C>
where
    T: DestinationTransport + ?Sized,
{
    /// Sends one ordinary typed message to the destination configured for its stable type name.
    ///
    /// The router is startup-owned and borrowed for this call; resolving it does not allocate or
    /// lock. A missing route returns [`ErrorCode::NotFound`] before the envelope is encoded, so a
    /// misconfigured deployment cannot publish an undeliverable message.
    pub async fn send_routed<M>(
        &self,
        router: &MessageDestinationRouter,
        message: &M,
    ) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        let destination = router.resolve(message.message_type()).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "no durable destination is configured for this message type",
            )
        })?;
        let envelope = self.encode_envelope(message, QualityOfService::AtLeastOnce)?;
        self.transport.send_to(destination, envelope).await
    }

    /// Sends one ordinary typed message to a validated durable destination.
    pub async fn send_to<M>(&self, destination: impl Into<Box<str>>, message: &M) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.send_with_quality_of_service(destination, message, QualityOfService::AtLeastOnce)
            .await
    }

    /// Sends one ordinary typed message with immutable transport headers.
    pub async fn send_to_with_headers<M>(
        &self,
        destination: impl Into<Box<str>>,
        message: &M,
        headers: &EnvelopeHeaders,
    ) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        let destination = Destination::parse(destination)?;
        let envelope = self.encode_envelope_with_headers(
            message,
            QualityOfService::AtLeastOnce,
            Some(headers),
        )?;
        self.transport.send_to(&destination, envelope).await
    }

    /// Sends one event to a validated durable destination with at-most-once metadata.
    pub async fn send_event_to<E>(
        &self,
        destination: impl Into<Box<str>>,
        event: &E,
    ) -> CatgaResult<()>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_with_quality_of_service(destination, event, QualityOfService::AtMostOnce)
            .await
    }

    /// Sends one reliable event to a destination with at-least-once metadata.
    pub async fn send_reliable_event_to<E>(
        &self,
        destination: impl Into<Box<str>>,
        event: &E,
    ) -> CatgaResult<()>
    where
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_with_quality_of_service(destination, event, QualityOfService::AtLeastOnce)
            .await
    }

    /// Sends ordinary messages using Catga's default bounded batch concurrency.
    pub async fn send_batch_to<M, I>(
        &self,
        destination: impl Into<Box<str>>,
        messages: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.send_batch_to_with_concurrency(
            destination,
            messages,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends events using Catga's default bounded batch concurrency.
    pub async fn send_event_batch_to<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_event_batch_to_with_concurrency(
            destination,
            events,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends reliable events using Catga's default bounded batch concurrency.
    pub async fn send_reliable_event_batch_to<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_reliable_event_batch_to_with_concurrency(
            destination,
            events,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends ordinary messages with bounded concurrent serialization and sending.
    pub async fn send_batch_to_with_concurrency<M, I>(
        &self,
        destination: impl Into<Box<str>>,
        messages: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.send_batch_with_quality_of_service(
            destination,
            messages,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Sends events with bounded concurrency and at-most-once metadata.
    pub async fn send_event_batch_to_with_concurrency<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_batch_with_quality_of_service(
            destination,
            events,
            concurrency_limit,
            QualityOfService::AtMostOnce,
        )
        .await
    }

    /// Sends reliable events with bounded concurrency and at-least-once metadata.
    pub async fn send_reliable_event_batch_to_with_concurrency<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event,
        C: PayloadEncoder<E>,
    {
        self.send_batch_with_quality_of_service(
            destination,
            events,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Receives and decodes one delivery from a validated named destination.
    ///
    /// Decode failures follow the negative-acknowledgement policy of [`Self::receive`].
    pub async fn receive_from<M>(
        &self,
        destination: impl Into<Box<str>>,
    ) -> CatgaResult<TypedDelivery<M>>
    where
        C: PayloadDecoder<M>,
    {
        let destination = Destination::parse(destination)?;
        let delivery = self.transport.receive_from(&destination).await?;
        self.decode_delivery(delivery).await
    }

    /// Receives, handles, and resolves one typed delivery from `destination`.
    ///
    /// The handler executes with the delivery's immutable transport context scoped to its future.
    /// A successful handler acknowledges the delivery. A handler error requests redelivery and
    /// returns [`TypedProcessOutcome::Rejected`] with the original application error; a failed
    /// negative acknowledgement remains the outer result because ownership is unresolved. This
    /// facade starts no background work, so callers retain control of polling and cancellation.
    /// It is a single-delivery convenience API; use [`CompetingConsumer`](crate::CompetingConsumer)
    /// for bounded production receive loops.
    pub async fn process_next_from<M, H>(
        &self,
        destination: impl Into<Box<str>>,
        handler: H,
    ) -> CatgaResult<TypedProcessOutcome>
    where
        C: PayloadDecoder<M>,
        H: for<'a> FnOnce(&'a M) -> BoxFuture<'a, CatgaResult<()>>,
    {
        let delivery = self.receive_from::<M>(destination).await?;
        process_typed_delivery(delivery, handler).await
    }

    async fn send_with_quality_of_service<M>(
        &self,
        destination: impl Into<Box<str>>,
        message: &M,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        let destination = Destination::parse(destination)?;
        let envelope = self.encode_envelope(message, quality_of_service)?;
        self.transport.send_to(&destination, envelope).await
    }

    async fn send_batch_with_quality_of_service<M, I>(
        &self,
        destination: impl Into<Box<str>>,
        messages: I,
        concurrency_limit: usize,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message,
        C: PayloadEncoder<M>,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "typed destination transport batch concurrency must be greater than zero",
            ));
        }
        let destination = Destination::parse(destination)?;
        let mut sends = stream::iter(messages)
            .map(|message| {
                let destination = destination.clone();
                async move {
                    let envelope = self.encode_envelope(&message, quality_of_service)?;
                    self.transport.send_to(&destination, envelope).await
                }
            })
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = sends.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
