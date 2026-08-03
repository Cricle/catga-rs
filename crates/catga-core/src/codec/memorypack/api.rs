//! Catga-facing typed helpers built exclusively on bounded MemoryPack frames.

use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime},
};

use crate::{
    CatgaError, CatgaResult, DelayedMessage, DistributedIdGenerator, Envelope, EnvelopeHeaders,
    EnvelopeRequestClient, ErrorCode, Event, Message, MessageMetadata, MessagePriority,
    OutboxMessage, OutboxStore, QualityOfService, RemoteRequest, Request, RequestClient,
    RequestTransport, SnapshotCodec, TransportContext, current_correlation_id,
    current_transport_context,
};

use super::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, envelope::EnvelopeWire,
};
use crate::codec::memorypack::MemoryPackable;

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

#[derive(MemoryPackable)]
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
        E: Event + MemoryPackSerialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtMostOnce)
            .await
    }

    /// Resolves and persists a message-declared event delivery boundary with at-most-once QoS.
    pub async fn schedule_delayed_event<E>(&self, event: &E) -> CatgaResult<u64>
    where
        E: Event + DelayedMessage + MemoryPackSerialize,
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
        E: Event + MemoryPackSerialize,
    {
        self.schedule_with_quality_of_service(event, not_before, QualityOfService::AtLeastOnce)
            .await
    }

    /// Resolves and persists a message-declared event delivery boundary with at-least-once QoS.
    pub async fn schedule_delayed_reliable_event<E>(&self, event: &E) -> CatgaResult<u64>
    where
        E: Event + DelayedMessage + MemoryPackSerialize,
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
        E: Event + MemoryPackSerialize,
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
        E: Event + MemoryPackSerialize,
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

impl MemoryPackCodec {
    /// Serializes an envelope into a caller-owned buffer while retaining its capacity.
    pub fn encode_into(&self, envelope: &Envelope, output: &mut Vec<u8>) -> CatgaResult<()> {
        self.encode_into_value(&EnvelopeWire::from(envelope), output)
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::codec::memorypack::MemoryPackDecodeLimits;
    use crate::{
        Command, DefaultMessageTypeId, EnvelopeHeaders, Event, MemoryPackDeserialize,
        MemoryPackReader, MemoryPackSerialize, MemoryPackWriter, Message, OutboxStore,
        SnowflakeIdGenerator, SnowflakeLayout, scope_transport_context,
    };

    #[derive(MemoryPackable, Debug, Eq, PartialEq)]
    struct TestValue {
        number: u32,
        text: String,
    }

    #[derive(MemoryPackable)]
    struct TestCommand(u32);

    impl Message for TestCommand {}
    impl Command for TestCommand {
        type TypeId = DefaultMessageTypeId;
    }

    impl DelayedMessage for TestCommand {
        fn delay(&self) -> Option<Duration> {
            Some(Duration::ZERO)
        }
    }

    #[derive(Clone, MemoryPackable)]
    struct TestEvent(u32);

    impl Message for TestEvent {}
    impl Event for TestEvent {
        type TypeId = DefaultMessageTypeId;
    }

    impl DelayedMessage for TestEvent {
        fn delay(&self) -> Option<Duration> {
            Some(Duration::ZERO)
        }
    }

    #[derive(MemoryPackable)]
    struct TestRequest(u32);

    impl Message for TestRequest {}
    impl Request for TestRequest {
        type Response = TestResponse;
        type TypeId = DefaultMessageTypeId;
    }

    #[derive(Clone, MemoryPackable, Debug, Eq, PartialEq)]
    struct TestResponse(u32);

    #[derive(Default)]
    struct TestOutbox {
        messages: Mutex<Vec<OutboxMessage>>,
    }

    #[async_trait::async_trait]
    impl OutboxStore for TestOutbox {
        async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
            self.messages.lock().expect("outbox lock").push(message);
            Ok(())
        }

        async fn claim(&self, _: &str, _: usize) -> CatgaResult<Vec<OutboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack(&self, _: &str, _: u64, _: &str) -> CatgaResult<()> {
            Ok(())
        }

        async fn release(&self, _: &str, _: u64, _: &str) -> CatgaResult<()> {
            Ok(())
        }

        async fn record_failure(&self, _: &str, _: u64, _: &str, _: &str) -> CatgaResult<()> {
            Ok(())
        }

        async fn cancel(&self, id: u64) -> CatgaResult<bool> {
            let mut messages = self.messages.lock().expect("outbox lock");
            let Some(index) = messages.iter().position(|message| message.id() == id) else {
                return Ok(false);
            };
            messages.remove(index);
            Ok(true)
        }
    }

    #[derive(Default)]
    struct EchoTransport {
        request: Mutex<Option<Envelope>>,
        failure: bool,
    }

    #[async_trait::async_trait]
    impl RequestTransport for EchoTransport {
        async fn request(&self, _: &str, request: Envelope, _: Duration) -> CatgaResult<Envelope> {
            *self.request.lock().expect("request lock") = Some(request.clone());
            let codec = MemoryPackCodec::default();
            if self.failure {
                codec.typed_failure(
                    &request,
                    CatgaError::new(ErrorCode::Conflict, "remote conflict"),
                )
            } else {
                codec.typed_success(&request, &TestResponse(42))
            }
        }
    }

    fn ids() -> Arc<dyn DistributedIdGenerator> {
        Arc::new(
            SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
                .expect("test snowflake layout is valid"),
        )
    }

    #[test]
    fn rpc_response_rejects_unknown_tags_and_error_codes() {
        let mut unknown = MemoryPackWriter::new();
        unknown.write_u8(9).expect("tag writes");
        let mut reader =
            MemoryPackReader::new_bounded(unknown.as_bytes(), MemoryPackDecodeLimits::default())
                .expect("test frame is bounded");
        assert!(matches!(
            MemoryPackRpcResponse::<u8>::deserialize(&mut reader),
            Err(MemoryPackError::DeserializationError(message)) if message.contains("tag")
        ));

        let wire = RpcErrorWire {
            code: "not-a-catga-code".into(),
            message: "bad".into(),
            details: None,
        };
        let bytes = MemoryPackSerializer::serialize(&wire).expect("error wire serializes");
        let mut reader = MemoryPackReader::new_bounded(&bytes, MemoryPackDecodeLimits::default())
            .expect("error frame is bounded");
        assert!(matches!(
            RpcErrorWire::deserialize(&mut reader).and_then(CatgaError::try_from),
            Err(MemoryPackError::DeserializationError(message)) if message.contains("invalid Catga")
        ));
    }

    #[test]
    fn snapshot_codec_round_trips_and_maps_malformed_frames() {
        let codec = MemoryPackSnapshotCodec::<TestValue>::default();
        let value = TestValue {
            number: 7,
            text: "snapshot".into(),
        };
        let bytes = codec.encode_state(&value).expect("snapshot encodes");
        assert_eq!(codec.decode_state(&bytes).expect("snapshot decodes"), value);
        let error = codec
            .decode_state(&[0xff])
            .expect_err("malformed snapshot is rejected");
        assert_eq!(error.code(), ErrorCode::Validation);
    }

    #[test]
    fn codec_frame_checks_clear_reusable_output_after_failure() {
        let limits = MemoryPackDecodeLimits::new(1, 16, 16, 4, 4).expect("limits are valid");
        let codec = MemoryPackCodec::new(limits);
        let mut output = vec![1, 2, 3];
        let error = codec
            .encode_value_into(
                &TestValue {
                    number: 1,
                    text: "too large".into(),
                },
                &mut output,
            )
            .expect_err("frame limit rejects the value");
        assert_eq!(error.code(), ErrorCode::Validation);
        assert!(output.is_empty());
        assert!(codec.decode_value::<TestValue>(&[1]).is_err());
    }

    #[test]
    fn response_metadata_falls_back_to_message_id_and_preserves_priority() {
        let request = Envelope::new(
            42,
            "request",
            vec![],
            MessageMetadata::new(9, None).with_priority(MessagePriority::High),
        );
        let metadata = response_metadata(&request);
        assert_eq!(metadata.message_id(), 9);
        assert_eq!(metadata.correlation_id(), Some(9));
        assert_eq!(metadata.priority(), MessagePriority::High);
        assert_eq!(outgoing_transport_context(42).0, 42);
    }

    #[test]
    fn scheduled_outbox_serializes_every_qos_and_delayed_entry_point() {
        let store = Arc::new(TestOutbox::default());
        let scheduled = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids());
        futures::executor::block_on(async {
            let command = TestCommand(1);
            let event = TestEvent(2);
            scheduled
                .schedule_at(&command, SystemTime::now())
                .await
                .expect("command schedules");
            scheduled
                .schedule_delayed(&command)
                .await
                .expect("delayed command schedules");
            scheduled
                .schedule_after(&command, Duration::ZERO)
                .await
                .expect("relative command schedules");
            scheduled
                .schedule_event_at(&event, SystemTime::now())
                .await
                .expect("at-most-once event schedules");
            scheduled
                .schedule_delayed_event(&event)
                .await
                .expect("delayed event schedules");
            scheduled
                .schedule_event_after(&event, Duration::ZERO)
                .await
                .expect("relative event schedules");
            scheduled
                .schedule_reliable_event_at(&event, SystemTime::now())
                .await
                .expect("reliable event schedules");
            scheduled
                .schedule_delayed_reliable_event(&event)
                .await
                .expect("delayed reliable event schedules");
            scheduled
                .schedule_reliable_event_after(&event, Duration::ZERO)
                .await
                .expect("relative reliable event schedules");
        });
        let messages = store.messages.lock().expect("outbox lock");
        assert_eq!(messages.len(), 9);
        assert!(
            messages
                .iter()
                .all(|message| message.not_before().is_some())
        );
        assert_eq!(
            messages[3].envelope().metadata().quality_of_service(),
            QualityOfService::AtMostOnce
        );
        assert_eq!(
            messages[6].envelope().metadata().quality_of_service(),
            QualityOfService::AtLeastOnce
        );
    }

    #[test]
    fn request_client_inherits_transport_context_and_maps_remote_failures() {
        let transport = Arc::new(EchoTransport::default());
        let client = MemoryPackRequestClient::new(
            Arc::clone(&transport),
            "orders",
            Duration::from_secs(1),
            ids(),
        )
        .expect("request client is valid");
        let incoming = Envelope::new(
            1,
            "incoming",
            vec![],
            MessageMetadata::new(1, Some(77)).with_priority(MessagePriority::High),
        )
        .with_headers(
            EnvelopeHeaders::try_new([("tenant", "acme")])
                .expect("test transport headers are valid"),
        );
        let response = futures::executor::block_on(scope_transport_context(
            &incoming,
            client.request_default(&TestRequest(7)),
        ))
        .expect("typed response decodes");
        assert_eq!(response, TestResponse(42));
        let request = transport
            .request
            .lock()
            .expect("request lock")
            .clone()
            .expect("transport received request");
        assert_eq!(request.metadata().correlation_id(), Some(77));
        assert_eq!(request.metadata().priority(), MessagePriority::High);
        assert_eq!(
            request
                .headers()
                .find(|(key, _)| *key == "tenant")
                .map(|(_, value)| value),
            Some("acme")
        );

        let failure = Arc::new(EchoTransport {
            request: Mutex::new(None),
            failure: true,
        });
        let client = MemoryPackRequestClient::new(failure, "orders", Duration::from_secs(1), ids())
            .expect("request client is valid");
        let error =
            futures::executor::block_on(client.request(&TestRequest(7), Duration::from_secs(1)))
                .expect_err("structured remote failure is returned");
        assert_eq!(error.code(), ErrorCode::Conflict);
    }
}
