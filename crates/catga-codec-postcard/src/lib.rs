#![forbid(unsafe_code)]
//! Postcard envelope codec for Catga transports.

mod wire;

use std::{
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{
    CatgaError, CatgaResult, DEFAULT_TRANSPORT_BATCH_CONCURRENCY, Delivery, Destination,
    DestinationTransport, DistributedIdGenerator, Envelope, EnvelopeCodec, EnvelopeHeaders,
    EnvelopeRequestClient, ErrorCode, Event, Message, MessageMetadata, MessagePriority,
    MessageTransport, OutboxMessage, OutboxStore, QualityOfService, RemoteRequest, Request,
    RequestClient, RequestTransport, SnapshotCodec, TransportContext, current_correlation_id,
    current_transport_context, scope_transport_context,
};
use futures::{StreamExt, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use wire::{EnvelopeWire, HeadersEnvelopeWire, LegacyEnvelopeWire};

/// A compact binary envelope codec backed by Postcard.
#[derive(Clone, Copy, Debug, Default)]
pub struct PostcardCodec;

impl EnvelopeCodec for PostcardCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        postcard::to_allocvec(&EnvelopeWire::from(envelope)).map_err(map_error)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        match postcard::from_bytes::<EnvelopeWire>(bytes) {
            Ok(wire) => Envelope::try_from(wire),
            Err(error) if error == postcard::Error::DeserializeUnexpectedEnd => {
                decode_historical_envelope(bytes, error)
            }
            Err(error) => Err(map_error(error)),
        }
    }
}

fn decode_historical_envelope(
    bytes: &[u8],
    current_error: postcard::Error,
) -> CatgaResult<Envelope> {
    match postcard::take_from_bytes::<HeadersEnvelopeWire>(bytes) {
        Ok((headers, [])) => Envelope::try_from(headers),
        Ok(_) => Err(map_error(current_error)),
        Err(postcard::Error::DeserializeUnexpectedEnd) => {
            let (legacy, remaining) =
                postcard::take_from_bytes::<LegacyEnvelopeWire>(bytes).map_err(map_error)?;
            if !remaining.is_empty() {
                return Err(map_error(current_error));
            }
            Ok(Envelope::from(legacy))
        }
        Err(_) => Err(map_error(current_error)),
    }
}

impl PostcardCodec {
    /// Serializes an envelope into a caller-owned buffer, retaining its capacity for reuse.
    pub fn encode_into(&self, envelope: &Envelope, output: &mut Vec<u8>) -> CatgaResult<()> {
        encode_reusing(&EnvelopeWire::from(envelope), output).map_err(map_error)
    }

    /// Serializes an application value using the same compact codec as envelope payloads.
    pub fn encode_value<T: Serialize>(&self, value: &T) -> CatgaResult<Vec<u8>> {
        postcard::to_allocvec(value).map_err(map_value_error)
    }

    /// Serializes an application value into a caller-owned buffer without replacing its capacity.
    pub fn encode_value_into<T: Serialize>(
        &self,
        value: &T,
        output: &mut Vec<u8>,
    ) -> CatgaResult<()> {
        encode_reusing(value, output).map_err(map_value_error)
    }

    /// Deserializes an application value from an envelope payload without copying it first.
    pub fn decode_value<T: DeserializeOwned>(&self, bytes: &[u8]) -> CatgaResult<T> {
        postcard::from_bytes(bytes).map_err(map_value_error)
    }

    /// Builds a typed successful response with request correlation and priority propagated.
    pub fn typed_success<T: Serialize>(
        &self,
        request: &Envelope,
        response: &T,
    ) -> CatgaResult<Envelope> {
        Ok(Envelope::new(
            request.id(),
            std::any::type_name::<T>(),
            self.encode_value(&PostcardRpcResponse::Success(response))?,
            response_metadata(request),
        ))
    }

    /// Builds a typed remote failure response with request correlation and priority propagated.
    pub fn typed_failure(&self, request: &Envelope, error: CatgaError) -> CatgaResult<Envelope> {
        Ok(Envelope::new(
            request.id(),
            "catga.rpc.error",
            self.encode_value(&PostcardRpcResponse::<()>::Failure(error))?,
            response_metadata(request),
        ))
    }
}

/// The envelope payload returned by a typed Postcard request server.
#[derive(Deserialize, Serialize)]
pub enum PostcardRpcResponse<T> {
    /// A successful typed response.
    Success(T),
    /// A structured remote Catga failure.
    Failure(CatgaError),
}

/// Serializes typed values and delegates their delivery to one envelope transport.
///
/// This facade owns only shared transport and ID-generator handles. It creates an owned
/// [`Envelope`] for each call, so the backend retains its existing backpressure, lifecycle, and
/// acknowledgement behavior. It does not create worker tasks or retain an in-memory message
/// registry.
pub struct PostcardTransport<T: ?Sized> {
    transport: Arc<T>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: PostcardCodec,
}

/// A decoded message that still owns its backend acknowledgement token.
///
/// The typed value is deserialized directly from the received envelope payload. Acknowledgement
/// remains explicit and consumes this wrapper, so neither the typed value nor the backend token
/// can be acknowledged twice.
pub struct PostcardDelivery<M> {
    delivery: Delivery,
    message: M,
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

impl<M> PostcardDelivery<M> {
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
    /// Nested [`PostcardTransport`] publication inherits the delivery's
    /// correlation ID and shared headers. The delivery remains borrowed, so the
    /// caller retains explicit control over acknowledgement after the future
    /// finishes.
    pub async fn with_transport_context<T>(&self, future: impl Future<Output = T>) -> T {
        scope_transport_context(self.envelope(), future).await
    }
}

impl<T: ?Sized> Clone for PostcardTransport<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            id_generator: Arc::clone(&self.id_generator),
            codec: self.codec,
        }
    }
}

impl<T> PostcardTransport<T>
where
    T: MessageTransport + ?Sized,
{
    /// Creates a typed facade over `transport` using `id_generator` for outgoing envelopes.
    pub fn new(transport: Arc<T>, id_generator: Arc<dyn DistributedIdGenerator>) -> Self {
        Self {
            transport,
            id_generator,
            codec: PostcardCodec,
        }
    }

    /// Publishes one ordinary typed message with at-least-once delivery metadata.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message + Serialize,
    {
        self.transport
            .publish(self.encode_envelope(message, QualityOfService::AtLeastOnce)?)
            .await
    }

    /// Publishes one ordinary typed message with immutable transport headers.
    ///
    /// The supplied [`EnvelopeHeaders`] are shared with the outgoing envelope,
    /// so this call does not copy header strings before backend serialization.
    pub async fn publish_with_headers<M>(
        &self,
        message: &M,
        headers: &EnvelopeHeaders,
    ) -> CatgaResult<()>
    where
        M: Message + Serialize,
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
        E: Event + Serialize,
    {
        self.transport
            .publish(self.encode_envelope(event, QualityOfService::AtMostOnce)?)
            .await
    }

    /// Publishes one event with explicit at-least-once delivery metadata.
    ///
    /// Use this method for domain events that require durable broker delivery. The explicit name
    /// keeps that stronger contract visible at the call site without runtime type inspection.
    pub async fn publish_reliable_event<E>(&self, event: &E) -> CatgaResult<()>
    where
        E: Event + Serialize,
    {
        self.transport
            .publish(self.encode_envelope(event, QualityOfService::AtLeastOnce)?)
            .await
    }

    /// Publishes ordinary messages using Catga's default bounded batch concurrency.
    pub async fn publish_batch<M, I>(&self, messages: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message + Serialize,
    {
        self.publish_batch_with_concurrency(messages, DEFAULT_TRANSPORT_BATCH_CONCURRENCY)
            .await
    }

    /// Publishes events using Catga's default bounded batch concurrency.
    pub async fn publish_event_batch<E, I>(&self, events: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.publish_event_batch_with_concurrency(events, DEFAULT_TRANSPORT_BATCH_CONCURRENCY)
            .await
    }

    /// Publishes reliable events using Catga's default bounded batch concurrency.
    pub async fn publish_reliable_event_batch<E, I>(&self, events: I) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
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
    /// backend publication at once; a zero limit returns [`ErrorCode::Validation`] before the
    /// iterator is consumed. Every item is attempted before the first observed failure is
    /// returned.
    pub async fn publish_batch_with_concurrency<M, I>(
        &self,
        messages: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message + Serialize,
    {
        self.publish_batch_with_quality_of_service(
            messages,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Publishes events with a bounded number of active futures and at-most-once metadata.
    pub async fn publish_event_batch_with_concurrency<E, I>(
        &self,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.publish_batch_with_quality_of_service(
            events,
            concurrency_limit,
            QualityOfService::AtMostOnce,
        )
        .await
    }

    /// Publishes reliable events with a bounded number of active futures and at-least-once
    /// metadata.
    pub async fn publish_reliable_event_batch_with_concurrency<E, I>(
        &self,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.publish_batch_with_quality_of_service(
            events,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Receives one envelope, decodes its payload, and retains its acknowledgement ownership.
    ///
    /// When decoding fails, this method requests redelivery before returning the decoding error.
    /// If the backend cannot negative-acknowledge the delivery, that backend error is returned so
    /// the caller can distinguish an unresolved delivery from a completed negative acknowledgement.
    pub async fn receive<M>(&self) -> CatgaResult<PostcardDelivery<M>>
    where
        M: DeserializeOwned,
    {
        let delivery = self.transport.receive().await?;
        self.decode_delivery(delivery).await
    }

    async fn decode_delivery<M>(&self, delivery: Delivery) -> CatgaResult<PostcardDelivery<M>>
    where
        M: DeserializeOwned,
    {
        let message = match self.codec.decode_value(delivery.envelope().payload()) {
            Ok(message) => message,
            Err(error) => {
                delivery.negative_acknowledge().await?;
                return Err(error);
            }
        };
        Ok(PostcardDelivery { delivery, message })
    }

    fn encode_envelope<M>(
        &self,
        message: &M,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<Envelope>
    where
        M: Message + Serialize,
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
        M: Message + Serialize,
    {
        let message_id = self.id_generator.next_id()?;
        let (correlation_id, inherited_priority, inherited_headers) =
            outgoing_transport_context(message_id);
        let envelope = Envelope::versioned(
            message_id,
            message.message_type(),
            self.codec.encode_value(message)?,
            MessageMetadata::new(message_id, Some(correlation_id))
                .with_quality_of_service(quality_of_service)
                .with_priority(inherited_priority.unwrap_or_else(|| message.priority())),
            message.schema_version(),
        );
        Ok(
            match resolve_outgoing_headers(headers, inherited_headers.as_ref())? {
                Some(headers) => envelope.with_headers(headers),
                None => envelope,
            },
        )
    }

    async fn publish_batch_with_quality_of_service<M, I>(
        &self,
        messages: I,
        concurrency_limit: usize,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message + Serialize,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "typed Postcard transport batch concurrency must be greater than zero",
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

impl<T> PostcardTransport<T>
where
    T: DestinationTransport + ?Sized,
{
    /// Sends one ordinary typed message to a validated durable destination.
    pub async fn send_to<M>(&self, destination: impl Into<Box<str>>, message: &M) -> CatgaResult<()>
    where
        M: Message + Serialize,
    {
        self.send_with_quality_of_service(destination, message, QualityOfService::AtLeastOnce)
            .await
    }

    /// Sends one ordinary typed message with immutable transport headers.
    ///
    /// Header strings are retained through [`EnvelopeHeaders`] sharing until
    /// the backend encodes or consumes the envelope.
    pub async fn send_to_with_headers<M>(
        &self,
        destination: impl Into<Box<str>>,
        message: &M,
        headers: &EnvelopeHeaders,
    ) -> CatgaResult<()>
    where
        M: Message + Serialize,
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
        E: Event + Serialize,
    {
        self.send_with_quality_of_service(destination, event, QualityOfService::AtMostOnce)
            .await
    }

    /// Sends one reliable event to a validated durable destination with at-least-once metadata.
    pub async fn send_reliable_event_to<E>(
        &self,
        destination: impl Into<Box<str>>,
        event: &E,
    ) -> CatgaResult<()>
    where
        E: Event + Serialize,
    {
        self.send_with_quality_of_service(destination, event, QualityOfService::AtLeastOnce)
            .await
    }

    /// Sends ordinary messages to a destination using Catga's default bounded batch concurrency.
    pub async fn send_batch_to<M, I>(
        &self,
        destination: impl Into<Box<str>>,
        messages: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message + Serialize,
    {
        self.send_batch_to_with_concurrency(
            destination,
            messages,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends events to a destination using Catga's default bounded batch concurrency.
    pub async fn send_event_batch_to<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.send_event_batch_to_with_concurrency(
            destination,
            events,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends reliable events to a destination using Catga's default bounded batch concurrency.
    pub async fn send_reliable_event_batch_to<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.send_reliable_event_batch_to_with_concurrency(
            destination,
            events,
            DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        )
        .await
    }

    /// Sends ordinary messages to one destination with bounded concurrent serialization and
    /// sending.
    pub async fn send_batch_to_with_concurrency<M, I>(
        &self,
        destination: impl Into<Box<str>>,
        messages: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = M>,
        M: Message + Serialize,
    {
        self.send_batch_with_quality_of_service(
            destination,
            messages,
            concurrency_limit,
            QualityOfService::AtLeastOnce,
        )
        .await
    }

    /// Sends events to one destination with bounded concurrency and at-most-once metadata.
    pub async fn send_event_batch_to_with_concurrency<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
    {
        self.send_batch_with_quality_of_service(
            destination,
            events,
            concurrency_limit,
            QualityOfService::AtMostOnce,
        )
        .await
    }

    /// Sends reliable events to one destination with bounded concurrency and at-least-once
    /// metadata.
    pub async fn send_reliable_event_batch_to_with_concurrency<E, I>(
        &self,
        destination: impl Into<Box<str>>,
        events: I,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        I: IntoIterator<Item = E>,
        E: Event + Serialize,
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
    /// A decode failure follows the same negative-acknowledgement policy as [`PostcardTransport::receive`].
    pub async fn receive_from<M>(
        &self,
        destination: impl Into<Box<str>>,
    ) -> CatgaResult<PostcardDelivery<M>>
    where
        M: DeserializeOwned,
    {
        let destination = Destination::parse(destination)?;
        let delivery = self.transport.receive_from(&destination).await?;
        self.decode_delivery(delivery).await
    }

    async fn send_with_quality_of_service<M>(
        &self,
        destination: impl Into<Box<str>>,
        message: &M,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<()>
    where
        M: Message + Serialize,
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
        M: Message + Serialize,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "typed Postcard destination batch concurrency must be greater than zero",
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

/// Schedules typed Postcard messages through a durable Catga outbox.
///
/// This is the Rust counterpart to mediator-level delayed send and publish APIs. It keeps
/// serialization explicit and does not couple `catga-core` to Postcard or a particular outbox
/// backend. Each scheduled envelope retains the message's declared schema version, so delayed
/// versioned messages follow the same evolution path as immediate typed publication. The returned
/// ID can be passed to [`Self::cancel`] while the message remains pending.
pub struct PostcardScheduledOutbox<S: ?Sized> {
    store: Arc<S>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: PostcardCodec,
}

impl<S: ?Sized> Clone for PostcardScheduledOutbox<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            id_generator: Arc::clone(&self.id_generator),
            codec: self.codec,
        }
    }
}

impl<S> PostcardScheduledOutbox<S>
where
    S: OutboxStore + ?Sized,
{
    /// Creates a typed scheduler backed by one durable outbox and ID generator.
    pub fn new(store: Arc<S>, id_generator: Arc<dyn DistributedIdGenerator>) -> Self {
        Self {
            store,
            id_generator,
            codec: PostcardCodec,
        }
    }

    /// Serializes and persists a message that cannot be delivered before `not_before`.
    pub async fn schedule_at<M>(&self, message: &M, not_before: SystemTime) -> CatgaResult<u64>
    where
        M: Message + Serialize,
    {
        self.schedule_with_quality_of_service(message, not_before, QualityOfService::AtLeastOnce)
            .await
    }

    /// Serializes and persists an event with the source contract's at-most-once metadata.
    pub async fn schedule_event_at<E>(&self, event: &E, not_before: SystemTime) -> CatgaResult<u64>
    where
        E: Event + Serialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtMostOnce)
            .await
    }

    /// Serializes and persists a reliable event with at-least-once delivery metadata.
    pub async fn schedule_reliable_event_at<E>(
        &self,
        event: &E,
        not_before: SystemTime,
    ) -> CatgaResult<u64>
    where
        E: Event + Serialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtLeastOnce)
            .await
    }

    /// Serializes and persists a message after `delay` from the current wall clock.
    pub async fn schedule_after<M>(&self, message: &M, delay: Duration) -> CatgaResult<u64>
    where
        M: Message + Serialize,
    {
        let not_before = self.not_before_after(delay)?;
        self.schedule_at(message, not_before).await
    }

    /// Serializes and persists an event after `delay` with at-most-once metadata.
    pub async fn schedule_event_after<E>(&self, event: &E, delay: Duration) -> CatgaResult<u64>
    where
        E: Event + Serialize,
    {
        let not_before = self.not_before_after(delay)?;
        self.schedule_event_at(event, not_before).await
    }

    /// Serializes and persists a reliable event after `delay` with at-least-once metadata.
    pub async fn schedule_reliable_event_after<E>(
        &self,
        event: &E,
        delay: Duration,
    ) -> CatgaResult<u64>
    where
        E: Event + Serialize,
    {
        let not_before = self.not_before_after(delay)?;
        self.schedule_reliable_event_at(event, not_before).await
    }

    /// Cancels a pending scheduled message by its returned ID.
    pub async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        self.store.cancel(id).await
    }

    async fn schedule_with_quality_of_service<M>(
        &self,
        message: &M,
        not_before: SystemTime,
        quality_of_service: QualityOfService,
    ) -> CatgaResult<u64>
    where
        M: Message + Serialize,
    {
        let id = self.id_generator.next_id()?;
        let (correlation_id, inherited_priority, inherited_headers) =
            outgoing_transport_context(id);
        let envelope = Envelope::versioned(
            id,
            message.message_type(),
            self.codec.encode_value(message)?,
            MessageMetadata::new(id, Some(correlation_id))
                .with_quality_of_service(quality_of_service)
                .with_priority(inherited_priority.unwrap_or_else(|| message.priority())),
            message.schema_version(),
        );
        let envelope = match inherited_headers {
            Some(headers) => envelope.with_headers(headers),
            None => envelope,
        };
        self.store
            .enqueue(OutboxMessage::scheduled(envelope, not_before)?)
            .await?;
        Ok(id)
    }

    fn not_before_after(&self, delay: Duration) -> CatgaResult<SystemTime> {
        SystemTime::now().checked_add(delay).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "scheduled delay exceeds the system time range",
            )
        })
    }
}

/// A destination-bound typed request client over any Catga envelope request transport.
pub struct PostcardRequestClient<T: ?Sized> {
    client: EnvelopeRequestClient<T>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: PostcardCodec,
}

/// Creates typed, destination-bound Postcard request clients from shared transport resources.
///
/// The factory is immutable after construction.  Each created client receives cloned [`Arc`]
/// handles and an owned compact destination, so concurrent construction and use need no global
/// reply map, mutex, or task.  It is the Rust counterpart to Catga's request-client factory while
/// keeping the chosen wire codec explicit.
pub struct PostcardRequestClientFactory<T: ?Sized> {
    transport: Arc<T>,
    default_timeout: Duration,
    id_generator: Arc<dyn DistributedIdGenerator>,
}

impl<T: ?Sized> Clone for PostcardRequestClientFactory<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            default_timeout: self.default_timeout,
            id_generator: Arc::clone(&self.id_generator),
        }
    }
}

impl<T> PostcardRequestClientFactory<T>
where
    T: RequestTransport + ?Sized,
{
    /// Creates a factory with one validated timeout policy for subsequent clients.
    pub fn new(
        transport: Arc<T>,
        default_timeout: Duration,
        id_generator: Arc<dyn DistributedIdGenerator>,
    ) -> CatgaResult<Self> {
        if default_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "request client factory timeout must be greater than zero",
            ));
        }
        Ok(Self {
            transport,
            default_timeout,
            id_generator,
        })
    }

    /// Creates a client whose destination is the stable Rust type name of `M`.
    pub fn create<M>(&self) -> CatgaResult<PostcardRequestClient<T>>
    where
        M: RemoteRequest,
        M::Response: DeserializeOwned,
    {
        self.create_to::<M>(std::any::type_name::<M>())
    }

    /// Creates a client with an explicit destination and the factory timeout.
    pub fn create_to<M>(
        &self,
        destination: impl Into<Box<str>>,
    ) -> CatgaResult<PostcardRequestClient<T>>
    where
        M: RemoteRequest,
        M::Response: DeserializeOwned,
    {
        self.create_to_with_timeout::<M>(destination, self.default_timeout)
    }

    /// Creates a client with an explicit destination and validated per-client timeout.
    pub fn create_to_with_timeout<M>(
        &self,
        destination: impl Into<Box<str>>,
        default_timeout: Duration,
    ) -> CatgaResult<PostcardRequestClient<T>>
    where
        M: RemoteRequest,
        M::Response: DeserializeOwned,
    {
        PostcardRequestClient::new(
            Arc::clone(&self.transport),
            destination,
            default_timeout,
            Arc::clone(&self.id_generator),
        )
    }

    /// Returns the validated timeout supplied to clients created without an override.
    pub const fn default_timeout(&self) -> Duration {
        self.default_timeout
    }
}

impl<T: ?Sized> Clone for PostcardRequestClient<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            id_generator: Arc::clone(&self.id_generator),
            codec: self.codec,
        }
    }
}

impl<T> PostcardRequestClient<T>
where
    T: RequestTransport + ?Sized,
{
    /// Binds an envelope request transport, destination, default timeout, and ID generator.
    pub fn new(
        transport: Arc<T>,
        destination: impl Into<Box<str>>,
        default_timeout: Duration,
        id_generator: Arc<dyn DistributedIdGenerator>,
    ) -> CatgaResult<Self> {
        Ok(Self {
            client: EnvelopeRequestClient::new(transport, destination, default_timeout)?,
            id_generator,
            codec: PostcardCodec,
        })
    }

    /// Serializes, sends, validates, and deserializes one typed request/response pair.
    pub async fn request_default<M>(&self, request: &M) -> CatgaResult<M::Response>
    where
        M: Message + Request + Serialize,
        M::Response: DeserializeOwned,
    {
        self.request(request, self.client.default_timeout()).await
    }

    /// Sends a typed request using an explicit timeout.
    ///
    /// The outgoing envelope retains the request's declared schema version and
    /// priority, and inherits any scoped transport correlation and immutable
    /// headers. An inbound scoped priority takes precedence over the request's
    /// declared priority, matching nested source transport contexts. This
    /// keeps request/reply calls consistent with typed
    /// publication without a client-side reply registry or header map
    /// allocation.
    pub async fn request<M>(&self, request: &M, timeout: Duration) -> CatgaResult<M::Response>
    where
        M: Message + Request + Serialize,
        M::Response: DeserializeOwned,
    {
        let message_id = self.id_generator.next_id()?;
        let (correlation_id, inherited_priority, inherited_headers) =
            outgoing_transport_context(message_id);
        let request_envelope = Envelope::versioned(
            message_id,
            request.message_type(),
            self.codec.encode_value(request)?,
            MessageMetadata::new(message_id, Some(correlation_id))
                .with_priority(inherited_priority.unwrap_or_else(|| request.priority())),
            request.schema_version(),
        );
        let request_envelope = match inherited_headers {
            Some(headers) => request_envelope.with_headers(headers),
            None => request_envelope,
        };
        let response = self
            .client
            .request_with_timeout(request_envelope, timeout)
            .await?;
        if response.metadata().correlation_id() != Some(correlation_id) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "response correlation does not match the request",
            ));
        }
        match self.codec.decode_value(response.payload())? {
            PostcardRpcResponse::Success(response) => Ok(response),
            PostcardRpcResponse::Failure(error) => Err(error),
        }
    }

    /// Returns the configured request destination.
    pub fn destination(&self) -> &str {
        self.client.destination()
    }
}

#[async_trait::async_trait]
impl<T, M> RequestClient<M> for PostcardRequestClient<T>
where
    T: RequestTransport + ?Sized,
    M: RemoteRequest,
    M::Response: DeserializeOwned,
{
    async fn request(&self, request: &M) -> CatgaResult<M::Response> {
        self.request_default(request).await
    }
}

/// Compact Postcard codec for one explicit persistent snapshot state type.
#[derive(Clone, Copy, Debug)]
pub struct PostcardSnapshotCodec<S> {
    state: PhantomData<fn() -> S>,
}

impl<S> Default for PostcardSnapshotCodec<S> {
    fn default() -> Self {
        Self { state: PhantomData }
    }
}

impl<S> SnapshotCodec<S> for PostcardSnapshotCodec<S>
where
    S: DeserializeOwned + Serialize + Send + Sync,
{
    fn encode_state(&self, state: &S) -> CatgaResult<Vec<u8>> {
        postcard::to_allocvec(state).map_err(map_error)
    }

    fn decode_state(&self, bytes: &[u8]) -> CatgaResult<S> {
        postcard::from_bytes(bytes).map_err(map_error)
    }
}

fn map_error(error: postcard::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}

fn map_value_error(error: postcard::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}

fn response_metadata(request: &Envelope) -> MessageMetadata {
    let correlation_id = request
        .metadata()
        .correlation_id()
        .unwrap_or(request.metadata().message_id());
    MessageMetadata::new(request.metadata().message_id(), Some(correlation_id))
        .with_priority(request.metadata().priority())
}

fn encode_reusing<T: Serialize>(value: &T, output: &mut Vec<u8>) -> postcard::Result<()> {
    output.clear();
    match postcard::to_extend(value, ReusableBuffer(output)) {
        Ok(_) => Ok(()),
        Err(error) => {
            output.clear();
            Err(error)
        }
    }
}

struct ReusableBuffer<'a>(&'a mut Vec<u8>);

impl Extend<u8> for ReusableBuffer<'_> {
    fn extend<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = u8>,
    {
        self.0.extend(items);
    }
}
