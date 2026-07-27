//! Catga-facing typed helpers built exclusively on bounded MemoryPack frames.

use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{
    CatgaError, CatgaResult, DelayedMessage, DistributedIdGenerator, Envelope,
    EnvelopeRequestClient, ErrorCode, Message, MessageMetadata, MessagePriority, OutboxMessage,
    OutboxStore, QualityOfService, RemoteRequest, Request, RequestClient, RequestTransport,
    SnapshotCodec, TransportContext, current_correlation_id, current_transport_context,
};

use crate::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

/// A typed RPC payload encoded by [`MemoryPackCodec`].
///
/// Failure values use a bounded MemoryPack-only wire DTO because `CatgaError` intentionally does
/// not expose its Serde deserializer as the MemoryPack application trait contract.
#[derive(Debug, Eq, PartialEq)]
pub enum MemoryPackRpcResponse<T> {
    /// A successful typed response.
    Success(T),
    /// A structured remote Catga failure.
    Failure(CatgaError),
}

#[derive(crate::MemoryPackable)]
struct RpcErrorWire {
    code: String,
    message: String,
    details: Option<String>,
}

impl From<&CatgaError> for RpcErrorWire {
    fn from(error: &CatgaError) -> Self {
        Self {
            code: error.code().as_stable_str().to_owned(),
            message: error.message().to_owned(),
            details: error.details().map(str::to_owned),
        }
    }
}

impl TryFrom<RpcErrorWire> for CatgaError {
    type Error = MemoryPackError;

    fn try_from(wire: RpcErrorWire) -> Result<Self, Self::Error> {
        let code = ErrorCode::from_stable_str(&wire.code).ok_or_else(|| {
            MemoryPackError::DeserializationError(format!(
                "invalid Catga error code: {}",
                wire.code
            ))
        })?;
        let error = CatgaError::new(code, wire.message);
        Ok(match wire.details {
            Some(details) => error.with_details(details),
            None => error,
        })
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for MemoryPackRpcResponse<T> {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        match self {
            Self::Success(value) => {
                writer.write_u8(0)?;
                value.serialize(writer)
            }
            Self::Failure(error) => {
                writer.write_u8(1)?;
                RpcErrorWire::from(error).serialize(writer)
            }
        }
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for MemoryPackRpcResponse<T> {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::Success(T::deserialize(reader)?)),
            1 => Ok(Self::Failure(CatgaError::try_from(
                RpcErrorWire::deserialize(reader)?,
            )?)),
            tag => Err(MemoryPackError::DeserializationError(format!(
                "invalid MemoryPack RPC response tag: {tag}"
            ))),
        }
    }
}

/// Schedules typed MemoryPack messages through a durable Catga outbox.
pub struct MemoryPackScheduledOutbox<S: ?Sized> {
    store: Arc<S>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: MemoryPackCodec,
}

impl<S: ?Sized> Clone for MemoryPackScheduledOutbox<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            id_generator: Arc::clone(&self.id_generator),
            codec: self.codec,
        }
    }
}

impl<S> MemoryPackScheduledOutbox<S>
where
    S: OutboxStore + ?Sized,
{
    /// Creates a typed scheduler backed by `store` and `id_generator`.
    pub fn new(store: Arc<S>, id_generator: Arc<dyn DistributedIdGenerator>) -> Self {
        Self {
            store,
            id_generator,
            codec: MemoryPackCodec::default(),
        }
    }

    /// Serializes and persists a message that cannot be delivered before `not_before`.
    pub async fn schedule_at<M>(&self, message: &M, not_before: SystemTime) -> CatgaResult<u64>
    where
        M: Message + MemoryPackSerialize,
    {
        self.schedule_with_quality_of_service(message, not_before, QualityOfService::AtLeastOnce)
            .await
    }

    /// Resolves and persists a message-declared durable delivery boundary.
    ///
    /// [`DelayedMessage::scheduled_at`] takes precedence over its relative delay. The resolution
    /// happens exactly once immediately before persistence; this method creates no timer or
    /// background task. Use [`Self::schedule_at`] when scheduling policy belongs at the call site.
    pub async fn schedule_delayed<M>(&self, message: &M) -> CatgaResult<u64>
    where
        M: DelayedMessage + MemoryPackSerialize,
    {
        self.schedule_at(message, message.deliver_at(SystemTime::now())?)
            .await
    }

    /// Serializes and persists an event with at-most-once delivery metadata.
    pub async fn schedule_event_at<E>(&self, event: &E, not_before: SystemTime) -> CatgaResult<u64>
    where
        E: catga_core::Event + MemoryPackSerialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtMostOnce)
            .await
    }

    /// Resolves and persists a message-declared event delivery boundary with at-most-once QoS.
    pub async fn schedule_delayed_event<E>(&self, event: &E) -> CatgaResult<u64>
    where
        E: catga_core::Event + DelayedMessage + MemoryPackSerialize,
    {
        self.schedule_event_at(event, event.deliver_at(SystemTime::now())?)
            .await
    }

    /// Serializes and persists a reliable event with at-least-once delivery metadata.
    pub async fn schedule_reliable_event_at<E>(
        &self,
        event: &E,
        not_before: SystemTime,
    ) -> CatgaResult<u64>
    where
        E: catga_core::Event + MemoryPackSerialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtLeastOnce)
            .await
    }

    /// Resolves and persists a message-declared event delivery boundary with at-least-once QoS.
    pub async fn schedule_delayed_reliable_event<E>(&self, event: &E) -> CatgaResult<u64>
    where
        E: catga_core::Event + DelayedMessage + MemoryPackSerialize,
    {
        self.schedule_reliable_event_at(event, event.deliver_at(SystemTime::now())?)
            .await
    }

    /// Serializes and persists a message after `delay` from the current wall clock.
    pub async fn schedule_after<M>(&self, message: &M, delay: Duration) -> CatgaResult<u64>
    where
        M: Message + MemoryPackSerialize,
    {
        self.schedule_at(message, self.not_before_after(delay)?)
            .await
    }

    /// Serializes and persists an event after `delay` with at-most-once metadata.
    pub async fn schedule_event_after<E>(&self, event: &E, delay: Duration) -> CatgaResult<u64>
    where
        E: catga_core::Event + MemoryPackSerialize,
    {
        self.schedule_event_at(event, self.not_before_after(delay)?)
            .await
    }

    /// Serializes and persists a reliable event after `delay` with at-least-once metadata.
    pub async fn schedule_reliable_event_after<E>(
        &self,
        event: &E,
        delay: Duration,
    ) -> CatgaResult<u64>
    where
        E: catga_core::Event + MemoryPackSerialize,
    {
        self.schedule_reliable_event_at(event, self.not_before_after(delay)?)
            .await
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
        M: Message + MemoryPackSerialize,
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

/// A destination-bound typed request client using exact bounded MemoryPack frames.
pub struct MemoryPackRequestClient<T: ?Sized> {
    client: EnvelopeRequestClient<T>,
    id_generator: Arc<dyn DistributedIdGenerator>,
    codec: MemoryPackCodec,
}

/// Creates typed MemoryPack request clients from shared transport resources.
///
/// Application request and response types remain bound only by MemoryPack traits; core's
/// [`RemoteRequest`] stays format-neutral.
pub struct MemoryPackRequestClientFactory<T: ?Sized> {
    transport: Arc<T>,
    default_timeout: Duration,
    id_generator: Arc<dyn DistributedIdGenerator>,
}

impl<T: ?Sized> Clone for MemoryPackRequestClientFactory<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            default_timeout: self.default_timeout,
            id_generator: Arc::clone(&self.id_generator),
        }
    }
}

impl<T> MemoryPackRequestClientFactory<T>
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
    pub fn create<M>(&self) -> CatgaResult<MemoryPackRequestClient<T>>
    where
        M: Message + Request,
        M::Response: MemoryPackDeserialize,
    {
        self.create_to::<M>(std::any::type_name::<M>())
    }

    /// Creates a client with an explicit destination and the factory timeout.
    pub fn create_to<M>(
        &self,
        destination: impl Into<Box<str>>,
    ) -> CatgaResult<MemoryPackRequestClient<T>>
    where
        M: Message + Request,
        M::Response: MemoryPackDeserialize,
    {
        self.create_to_with_timeout::<M>(destination, self.default_timeout)
    }

    /// Creates a client with an explicit destination and validated per-client timeout.
    pub fn create_to_with_timeout<M>(
        &self,
        destination: impl Into<Box<str>>,
        default_timeout: Duration,
    ) -> CatgaResult<MemoryPackRequestClient<T>>
    where
        M: Message + Request,
        M::Response: MemoryPackDeserialize,
    {
        MemoryPackRequestClient::new(
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

impl<T: ?Sized> Clone for MemoryPackRequestClient<T> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            id_generator: Arc::clone(&self.id_generator),
            codec: self.codec,
        }
    }
}

impl<T> MemoryPackRequestClient<T>
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
            codec: MemoryPackCodec::default(),
        })
    }

    /// Serializes, sends, validates, and deserializes one typed request/response pair.
    pub async fn request_default<M>(&self, request: &M) -> CatgaResult<M::Response>
    where
        M: Message + Request + MemoryPackSerialize,
        M::Response: MemoryPackDeserialize,
    {
        self.request(request, self.client.default_timeout()).await
    }

    /// Sends a typed request using an explicit timeout.
    ///
    /// The request and response application values are constrained only by MemoryPack traits,
    /// avoiding the Serde requirement of core's generic `RequestClient` abstraction.
    pub async fn request<M>(&self, request: &M, timeout: Duration) -> CatgaResult<M::Response>
    where
        M: Message + Request + MemoryPackSerialize,
        M::Response: MemoryPackDeserialize,
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
        match self.codec.decode_rpc_response(response.payload())? {
            MemoryPackRpcResponse::Success(response) => Ok(response),
            MemoryPackRpcResponse::Failure(error) => Err(error),
        }
    }

    /// Returns the configured request destination.
    pub fn destination(&self) -> &str {
        self.client.destination()
    }
}

#[async_trait::async_trait]
impl<T, M> RequestClient<M> for MemoryPackRequestClient<T>
where
    T: RequestTransport + ?Sized,
    M: RemoteRequest + MemoryPackSerialize,
    M::Response: MemoryPackDeserialize,
{
    async fn request(&self, request: &M) -> CatgaResult<M::Response> {
        self.request_default(request).await
    }
}

/// Compact MemoryPack codec for one explicit persistent snapshot state type.
#[derive(Clone, Copy, Debug)]
pub struct MemoryPackSnapshotCodec<S> {
    state: PhantomData<fn() -> S>,
}

impl<S> Default for MemoryPackSnapshotCodec<S> {
    fn default() -> Self {
        Self { state: PhantomData }
    }
}

impl<S> SnapshotCodec<S> for MemoryPackSnapshotCodec<S>
where
    S: MemoryPackDeserialize + MemoryPackSerialize + Send + Sync,
{
    fn encode_state(&self, state: &S) -> CatgaResult<Vec<u8>> {
        MemoryPackCodec::default().encode_value(state)
    }

    fn decode_state(&self, bytes: &[u8]) -> CatgaResult<S> {
        MemoryPackCodec::default().decode_value(bytes)
    }
}

fn outgoing_transport_context(
    message_id: u64,
) -> (
    u64,
    Option<MessagePriority>,
    Option<catga_core::EnvelopeHeaders>,
) {
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

impl MemoryPackCodec {
    /// Serializes an envelope into a caller-owned buffer while retaining its capacity.
    pub fn encode_into(&self, envelope: &Envelope, output: &mut Vec<u8>) -> CatgaResult<()> {
        self.encode_into_value(&crate::envelope::EnvelopeWire::from(envelope), output)
    }

    /// Serializes an application value using the same bounded codec as envelope payloads.
    pub fn encode_value<T: MemoryPackSerialize>(&self, value: &T) -> CatgaResult<Vec<u8>> {
        let bytes = MemoryPackSerializer::serialize(value).map_err(map_memorypack_error)?;
        self.check_outbound_frame(&bytes)?;
        Ok(bytes)
    }

    /// Serializes an application value into a caller-owned buffer without replacing its capacity.
    pub fn encode_value_into<T: MemoryPackSerialize>(
        &self,
        value: &T,
        output: &mut Vec<u8>,
    ) -> CatgaResult<()> {
        self.encode_into_value(value, output)
    }

    /// Deserializes one exact application payload frame under this codec's receive limits.
    pub fn decode_value<T: MemoryPackDeserialize>(&self, bytes: &[u8]) -> CatgaResult<T> {
        MemoryPackSerializer::deserialize_bounded(bytes, self.decode_limits())
            .map_err(map_memorypack_error)
    }

    /// Decodes a typed RPC response from one exact bounded MemoryPack frame.
    pub fn decode_rpc_response<T: MemoryPackDeserialize>(
        &self,
        bytes: &[u8],
    ) -> CatgaResult<MemoryPackRpcResponse<T>> {
        self.decode_value(bytes)
    }

    /// Builds a typed successful response with request correlation and priority propagated.
    pub fn typed_success<T: MemoryPackSerialize>(
        &self,
        request: &Envelope,
        response: &T,
    ) -> CatgaResult<Envelope> {
        let mut payload = Vec::new();
        payload.push(0);
        let response_bytes = self.encode_value(response)?;
        payload.extend_from_slice(&response_bytes);
        self.check_outbound_frame(&payload)?;
        Ok(Envelope::new(
            request.id(),
            std::any::type_name::<T>(),
            payload,
            response_metadata(request),
        ))
    }

    /// Builds a typed remote failure response with request correlation and priority propagated.
    pub fn typed_failure(&self, request: &Envelope, error: CatgaError) -> CatgaResult<Envelope> {
        Ok(Envelope::new(
            request.id(),
            "catga.rpc.error",
            self.encode_value(&MemoryPackRpcResponse::<()>::Failure(error))?,
            response_metadata(request),
        ))
    }

    fn encode_into_value<T: MemoryPackSerialize>(
        &self,
        value: &T,
        output: &mut Vec<u8>,
    ) -> CatgaResult<()> {
        MemoryPackSerializer::serialize_into(value, output).map_err(map_memorypack_error)?;
        if let Err(error) = self.check_outbound_frame(output) {
            output.clear();
            return Err(error);
        }
        Ok(())
    }

    fn check_outbound_frame(&self, bytes: &[u8]) -> CatgaResult<()> {
        if bytes.len() > self.decode_limits().max_frame_bytes() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "MemoryPack payload exceeds the configured frame limit",
            ));
        }
        Ok(())
    }
}

fn response_metadata(request: &Envelope) -> MessageMetadata {
    let correlation_id = request
        .metadata()
        .correlation_id()
        .unwrap_or(request.metadata().message_id());
    MessageMetadata::new(request.metadata().message_id(), Some(correlation_id))
        .with_priority(request.metadata().priority())
}

fn map_memorypack_error(error: MemoryPackError) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}
