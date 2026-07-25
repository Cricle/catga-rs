//! Envelope codec tests.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use catga_codec_postcard::{
    PostcardCodec, PostcardProcessOutcome, PostcardRpcResponse, PostcardTransport,
};
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, DeliveryMode, Destination,
    DestinationTransport, Envelope, EnvelopeCodec, EnvelopeHeaders, ErrorCode, Event, Message,
    MessageMetadata, MessagePriority, MessageTransport, QualityOfService, SnowflakeIdGenerator,
    SnowflakeLayout,
};
use catga_memory::MemoryTransport;
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct BestEffortEvent(u8);

impl Message for BestEffortEvent {}
impl Event for BestEffortEvent {}

#[derive(Clone, Serialize)]
struct ReliableEvent(u8);

impl Message for ReliableEvent {}
impl Event for ReliableEvent {}

#[derive(Deserialize, Serialize)]
struct OutboundCommand(u8);

impl Message for OutboundCommand {}

#[derive(catga_core::Message, Deserialize, Serialize)]
#[catga(version = 2, priority = high)]
struct VersionedOutboundCommand(u8);

struct SingleDeliveryTransport {
    delivery: Mutex<Option<Delivery>>,
}

impl SingleDeliveryTransport {
    fn new(delivery: Delivery) -> Self {
        Self {
            delivery: Mutex::new(Some(delivery)),
        }
    }
}

#[async_trait]
impl MessageTransport for SingleDeliveryTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "test transport does not publish",
        ))
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let mut delivery = self
            .delivery
            .lock()
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "test delivery lock is poisoned"))?;
        delivery.take().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "test transport has no remaining delivery",
            )
        })
    }
}

struct RecordingAcknowledger {
    acknowledged: Arc<AtomicBool>,
    negatively_acknowledged: Arc<AtomicBool>,
}

#[derive(Serialize)]
struct LegacyEnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: QualityOfService,
    delivery_mode: DeliveryMode,
    priority: MessagePriority,
    not_before_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<String>,
}

#[derive(Serialize)]
struct RawHeaderWire {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct LegacyCatgaErrorWire {
    code: ErrorCode,
    message: Box<str>,
}

#[derive(Serialize)]
enum LegacyPostcardRpcResponse<T> {
    Success(T),
    Failure(LegacyCatgaErrorWire),
}

#[derive(Serialize)]
struct UnboundedCatgaErrorWire {
    code: ErrorCode,
    message: Box<str>,
    details: Option<Box<str>>,
    retryable: Option<bool>,
}

#[derive(Serialize)]
struct HeaderEnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: QualityOfService,
    delivery_mode: DeliveryMode,
    priority: MessagePriority,
    not_before_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<String>,
    headers: Vec<RawHeaderWire>,
}

fn wire_envelope_with_headers(headers: Vec<RawHeaderWire>) -> HeaderEnvelopeWire {
    HeaderEnvelopeWire {
        id: 55,
        message_type: String::from("orders.created"),
        payload: vec![4, 5],
        message_id: 55,
        correlation_id: Some(12),
        quality_of_service: QualityOfService::AtLeastOnce,
        delivery_mode: DeliveryMode::WaitForResult,
        priority: MessagePriority::Normal,
        not_before_unix_ms: None,
        schema_version: 1,
        reply_to: None,
        headers,
    }
}

#[async_trait]
impl Acknowledger for RecordingAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.acknowledged.store(true, Ordering::Release);
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.negatively_acknowledged.store(true, Ordering::Release);
        Ok(())
    }
}

#[test]
fn postcard_codec_round_trips_envelope_metadata_and_payload() {
    let envelope = Envelope::versioned(
        42,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(42, Some(9))
            .with_quality_of_service(QualityOfService::ExactlyOnce)
            .with_delivery_mode(DeliveryMode::AsyncRetry)
            .with_priority(MessagePriority::High)
            .with_not_before_unix_ms(Some(700)),
        4,
    );
    let codec = PostcardCodec;

    let decoded = codec.decode(&codec.encode(&envelope).unwrap()).unwrap();

    assert_eq!(decoded.id(), 42);
    assert_eq!(decoded.message_type(), "order.created");
    assert_eq!(decoded.payload(), [1, 2, 3]);
    assert_eq!(
        decoded.metadata(),
        MessageMetadata::new(42, Some(9))
            .with_quality_of_service(QualityOfService::ExactlyOnce)
            .with_delivery_mode(DeliveryMode::AsyncRetry)
            .with_priority(MessagePriority::High)
            .with_not_before_unix_ms(Some(700))
    );
    assert_eq!(decoded.schema_version(), 4);
}

#[test]
fn postcard_codec_preserves_an_optional_reply_destination() {
    let codec = PostcardCodec;
    let envelope = Envelope::new(9, "rpc.request", vec![1], MessageMetadata::new(9, Some(9)))
        .with_reply_to("reply.inbox.9");

    let restored = codec
        .encode(&envelope)
        .and_then(|bytes| codec.decode(&bytes));

    assert_eq!(
        restored.expect("round trip succeeds").reply_to(),
        Some("reply.inbox.9")
    );
}

#[test]
fn postcard_codec_round_trips_validated_envelope_headers() {
    let codec = PostcardCodec;
    let headers = EnvelopeHeaders::try_new([("tenant", "blue"), ("route", "priority")])
        .expect("valid headers are accepted");
    let envelope = Envelope::new(
        53,
        "orders.created",
        vec![7],
        MessageMetadata::new(53, Some(12)),
    )
    .with_headers(headers);

    let restored = codec
        .encode(&envelope)
        .and_then(|bytes| codec.decode(&bytes))
        .expect("header envelope round trip succeeds");

    assert_eq!(
        restored.headers().collect::<Vec<_>>(),
        vec![("tenant", "blue"), ("route", "priority")]
    );
}

#[test]
fn postcard_codec_round_trips_exact_envelope_sent_at() {
    let codec = PostcardCodec;
    let envelope = Envelope::new(
        57,
        "orders.created",
        vec![1],
        MessageMetadata::new(57, Some(12)),
    )
    .with_sent_at(UNIX_EPOCH)
    .expect("epoch timestamp is valid");

    let restored = codec
        .encode(&envelope)
        .and_then(|bytes| codec.decode(&bytes))
        .expect("timestamp envelope round trip succeeds");

    assert_eq!(restored.sent_at_unix_ms(), Some(0));
    assert_eq!(restored.sent_at(), Some(UNIX_EPOCH));
}

#[test]
fn postcard_codec_decodes_header_wire_without_sent_at() {
    let prior = wire_envelope_with_headers(vec![RawHeaderWire {
        key: String::from("tenant"),
        value: String::from("blue"),
    }]);
    let bytes = postcard::to_allocvec(&prior).expect("prior header wire encodes");

    let restored = PostcardCodec
        .decode(&bytes)
        .expect("prior header wire remains decodable");

    assert_eq!(restored.sent_at_unix_ms(), None);
    assert_eq!(restored.header("tenant"), Some("blue"));
}

#[test]
fn postcard_codec_decodes_legacy_envelopes_without_headers() {
    let legacy = LegacyEnvelopeWire {
        id: 54,
        message_type: String::from("orders.created"),
        payload: vec![9],
        message_id: 54,
        correlation_id: Some(12),
        quality_of_service: QualityOfService::AtLeastOnce,
        delivery_mode: DeliveryMode::WaitForResult,
        priority: MessagePriority::Normal,
        not_before_unix_ms: None,
        schema_version: 1,
        reply_to: None,
    };
    let bytes = postcard::to_allocvec(&legacy).expect("legacy wire encodes");

    let restored = PostcardCodec
        .decode(&bytes)
        .expect("legacy wire remains decodable");

    assert_eq!(restored.id(), 54);
    assert!(restored.headers().next().is_none());
    assert_eq!(restored.sent_at_unix_ms(), None);
}

#[test]
fn postcard_codec_rejects_invalid_remote_headers() {
    let raw = wire_envelope_with_headers(vec![
        RawHeaderWire {
            key: String::from("tenant"),
            value: String::from("blue"),
        },
        RawHeaderWire {
            key: String::from("tenant"),
            value: String::from("green"),
        },
    ]);
    let bytes = postcard::to_allocvec(&raw).expect("raw wire encodes");

    let error = PostcardCodec
        .decode(&bytes)
        .expect_err("duplicate remote headers are rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn postcard_codec_encodes_into_a_reused_caller_buffer() {
    let codec = PostcardCodec;
    let envelope = Envelope::new(
        11,
        "inventory.updated",
        vec![1, 2, 3, 4],
        MessageMetadata::new(11, Some(8)),
    );
    let mut output = Vec::with_capacity(256);
    let allocation = output.as_ptr();

    codec.encode_into(&envelope, &mut output).unwrap();

    assert_eq!(output.as_ptr(), allocation);
    let restored = codec.decode(&output).unwrap();
    assert_eq!(restored.payload(), [1, 2, 3, 4]);

    codec.encode_value_into(&1234_u16, &mut output).unwrap();

    assert_eq!(output.as_ptr(), allocation);
    assert_eq!(codec.decode_value::<u16>(&output).unwrap(), 1234);
}

#[test]
fn postcard_codec_builds_correlated_typed_success_and_failure_envelopes() {
    let codec = PostcardCodec;
    let request = Envelope::new(
        5,
        "inventory.lookup",
        vec![1],
        MessageMetadata::new(17, Some(23)).with_priority(MessagePriority::Critical),
    );

    let success = codec.typed_success(&request, &42_u16).unwrap();
    let failure = codec
        .typed_failure(
            &request,
            CatgaError::new(ErrorCode::Conflict, "unavailable"),
        )
        .unwrap();

    assert_eq!(success.metadata().correlation_id(), Some(23));
    assert_eq!(failure.metadata().correlation_id(), Some(23));
    assert_eq!(success.metadata().priority(), MessagePriority::Critical);
    assert_eq!(failure.metadata().priority(), MessagePriority::Critical);
    assert!(matches!(
        codec.decode_value(success.payload()).unwrap(),
        PostcardRpcResponse::Success(42_u16)
    ));
    assert!(matches!(
        codec.decode_value::<PostcardRpcResponse<()>>(failure.payload()).unwrap(),
        PostcardRpcResponse::Failure(error) if error.code() == ErrorCode::Conflict
    ));
}

#[test]
fn postcard_rpc_failure_round_trip_retains_error_details_and_retryability() {
    let codec = PostcardCodec;
    let response = PostcardRpcResponse::<()>::Failure(
        CatgaError::new(ErrorCode::Timeout, "upstream timed out")
            .with_details("retry after 1 second"),
    );

    let bytes = codec.encode_value(&response).unwrap();
    let decoded = codec
        .decode_value::<PostcardRpcResponse<()>>(&bytes)
        .unwrap();

    match decoded {
        PostcardRpcResponse::Failure(error) => {
            assert_eq!(error.code(), ErrorCode::Timeout);
            assert_eq!(error.message(), "upstream timed out");
            assert_eq!(error.details(), Some("retry after 1 second"));
            assert!(error.is_retryable());
        }
        PostcardRpcResponse::Success(()) => panic!("expected a failure response"),
    }
}

#[test]
fn postcard_codec_decodes_legacy_rpc_failure_without_new_error_fields() {
    let codec = PostcardCodec;
    let bytes = postcard::to_allocvec(&LegacyPostcardRpcResponse::<()>::Failure(
        LegacyCatgaErrorWire {
            code: ErrorCode::Unavailable,
            message: "legacy transport failure".into(),
        },
    ))
    .unwrap();

    let decoded = codec.decode_rpc_response::<()>(&bytes).unwrap();

    assert!(matches!(
        decoded,
        PostcardRpcResponse::Failure(error)
            if error.code() == ErrorCode::Unavailable
                && error.message() == "legacy transport failure"
                && error.details().is_none()
                && error.is_retryable()
    ));
}

#[test]
fn postcard_codec_bounds_received_error_details() {
    let codec = PostcardCodec;
    let bytes = postcard::to_allocvec(&UnboundedCatgaErrorWire {
        code: ErrorCode::Internal,
        message: "invalid payload".into(),
        details: Some("é".repeat(513).into()),
        retryable: Some(false),
    })
    .unwrap();

    let decoded = codec.decode_value::<CatgaError>(&bytes).unwrap();

    assert!(decoded.details().unwrap().len() <= catga_core::MAX_ERROR_DETAILS_BYTES);
    assert!(
        decoded
            .details()
            .unwrap()
            .is_char_boundary(decoded.details().unwrap().len())
    );
}

#[test]
fn postcard_codec_rejects_a_truncated_current_error_frame() {
    let codec = PostcardCodec;
    let mut bytes = postcard::to_allocvec(&UnboundedCatgaErrorWire {
        code: ErrorCode::Internal,
        message: "truncated payload".into(),
        details: Some("new structured details".into()),
        retryable: Some(false),
    })
    .unwrap();
    bytes.pop();

    assert!(codec.decode_value::<CatgaError>(&bytes).is_err());
}

#[test]
fn postcard_codec_rejects_a_truncated_current_rpc_failure_frame() {
    let codec = PostcardCodec;
    let mut bytes = codec
        .encode_value(&PostcardRpcResponse::<()>::Failure(
            CatgaError::new(ErrorCode::Internal, "truncated response").with_details("details"),
        ))
        .unwrap();
    bytes.pop();

    assert!(codec.decode_rpc_response::<()>(&bytes).is_err());
}

#[tokio::test]
async fn typed_postcard_publish_propagates_derived_schema_version_to_envelope() {
    let backend = Arc::new(MemoryTransport::new(1).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .publish(&VersionedOutboundCommand(7))
        .await
        .expect("versioned command publishes");
    let delivery = backend.receive().await.expect("versioned command arrives");

    assert_eq!(delivery.envelope().schema_version(), 2);
    assert_eq!(
        delivery.envelope().metadata().priority(),
        MessagePriority::High
    );
    assert_eq!(
        PostcardCodec
            .decode_value::<VersionedOutboundCommand>(delivery.envelope().payload())
            .expect("versioned payload decodes")
            .0,
        7
    );
    delivery
        .acknowledge()
        .await
        .expect("versioned acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_delivery_context_propagates_correlation_headers_and_priority() {
    let backend = Arc::new(MemoryTransport::new(2).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);
    let headers = EnvelopeHeaders::try_new([("tenant", "blue"), ("route", "priority")])
        .expect("valid inbound headers");
    let inbound = Envelope::new(
        71,
        "orders.received",
        PostcardCodec
            .encode_value(&OutboundCommand(1))
            .expect("inbound payload encodes"),
        MessageMetadata::new(71, Some(41)).with_priority(MessagePriority::Critical),
    )
    .with_headers(headers);

    backend
        .publish(inbound)
        .await
        .expect("inbound message publishes");
    let delivery = transport
        .receive::<OutboundCommand>()
        .await
        .expect("inbound message decodes");

    delivery
        .with_transport_context(transport.publish(&OutboundCommand(2)))
        .await
        .expect("nested message publishes");
    let nested = backend.receive().await.expect("nested message arrives");

    assert_eq!(nested.envelope().metadata().correlation_id(), Some(41));
    assert_eq!(
        nested.envelope().metadata().priority(),
        MessagePriority::Critical
    );
    assert_eq!(nested.envelope().header("tenant"), Some("blue"));
    assert_eq!(nested.envelope().header("route"), Some("priority"));
    nested
        .acknowledge()
        .await
        .expect("nested acknowledgement succeeds");
    delivery
        .acknowledge()
        .await
        .expect("inbound acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_delivery_context_merges_explicit_headers() {
    let backend = Arc::new(MemoryTransport::new(2).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);
    let inbound = Envelope::new(
        72,
        "orders.received",
        PostcardCodec
            .encode_value(&OutboundCommand(1))
            .expect("inbound payload encodes"),
        MessageMetadata::new(72, Some(42)),
    )
    .with_headers(
        EnvelopeHeaders::try_new([("tenant", "blue"), ("route", "priority")])
            .expect("valid inbound headers"),
    );
    let explicit = EnvelopeHeaders::try_new([("tenant", "green"), ("role", "worker")])
        .expect("valid explicit headers");

    backend
        .publish(inbound)
        .await
        .expect("inbound message publishes");
    let delivery = transport
        .receive::<OutboundCommand>()
        .await
        .expect("inbound message decodes");

    delivery
        .with_transport_context(transport.publish_with_headers(&OutboundCommand(2), &explicit))
        .await
        .expect("nested message publishes");
    let nested = backend.receive().await.expect("nested message arrives");

    assert_eq!(nested.envelope().header("tenant"), Some("green"));
    assert_eq!(nested.envelope().header("route"), Some("priority"));
    assert_eq!(nested.envelope().header("role"), Some("worker"));
    nested
        .acknowledge()
        .await
        .expect("nested acknowledgement succeeds");
    delivery
        .acknowledge()
        .await
        .expect("inbound acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_events_apply_source_quality_of_service_defaults() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .publish_event(&BestEffortEvent(7))
        .await
        .expect("best-effort event publishes");
    let best_effort = backend.receive().await.expect("best-effort event arrives");
    assert_eq!(
        best_effort.envelope().metadata().quality_of_service(),
        QualityOfService::AtMostOnce
    );
    best_effort
        .acknowledge()
        .await
        .expect("best-effort acknowledgement succeeds");

    transport
        .publish_reliable_event(&ReliableEvent(8))
        .await
        .expect("reliable event publishes");
    let reliable = backend.receive().await.expect("reliable event arrives");
    assert_eq!(
        reliable.envelope().metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    reliable
        .acknowledge()
        .await
        .expect("reliable acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_send_event_preserves_the_event_delivery_contract() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let destination = Destination::parse("orders").expect("valid destination");
    backend
        .declare_destination(destination.clone())
        .expect("destination is declared");
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .send_event_to("orders", &BestEffortEvent(9))
        .await
        .expect("event is sent to its destination");

    let delivery = backend
        .receive_from(&destination)
        .await
        .expect("destination receives event");
    assert_eq!(
        delivery.envelope().metadata().quality_of_service(),
        QualityOfService::AtMostOnce
    );
    delivery
        .acknowledge()
        .await
        .expect("destination acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_event_batch_keeps_each_event_best_effort() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .publish_event_batch_with_concurrency(
            [BestEffortEvent(1), BestEffortEvent(2), BestEffortEvent(3)],
            2,
        )
        .await
        .expect("event batch publishes");

    for _ in 0..3 {
        let delivery = backend.receive().await.expect("event batch item arrives");
        assert_eq!(
            delivery.envelope().metadata().quality_of_service(),
            QualityOfService::AtMostOnce
        );
        delivery
            .acknowledge()
            .await
            .expect("event batch acknowledgement succeeds");
    }
}

#[tokio::test]
async fn typed_postcard_delivery_decodes_and_consumes_the_original_acknowledger() {
    let acknowledged = Arc::new(AtomicBool::new(false));
    let negatively_acknowledged = Arc::new(AtomicBool::new(false));
    let codec = PostcardCodec;
    let backend = Arc::new(SingleDeliveryTransport::new(Delivery::with_acknowledger(
        Envelope::new(
            44,
            "best-effort",
            codec
                .encode_value(&BestEffortEvent(12))
                .expect("serializable event"),
            MessageMetadata::new(44, None),
        ),
        Box::new(RecordingAcknowledger {
            acknowledged: Arc::clone(&acknowledged),
            negatively_acknowledged: Arc::clone(&negatively_acknowledged),
        }),
    )));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(backend, ids);

    let delivery = transport
        .receive::<BestEffortEvent>()
        .await
        .expect("typed delivery decodes");
    assert_eq!(delivery.message().0, 12);
    assert_eq!(delivery.envelope().id(), 44);
    delivery
        .acknowledge()
        .await
        .expect("typed acknowledgement succeeds");

    assert!(acknowledged.load(Ordering::Acquire));
    assert!(!negatively_acknowledged.load(Ordering::Acquire));
}

#[tokio::test]
async fn typed_postcard_decode_failure_negative_acknowledges_the_delivery() {
    let acknowledged = Arc::new(AtomicBool::new(false));
    let negatively_acknowledged = Arc::new(AtomicBool::new(false));
    let backend = Arc::new(SingleDeliveryTransport::new(Delivery::with_acknowledger(
        Envelope::new(
            45,
            "best-effort",
            Vec::new(),
            MessageMetadata::new(45, None),
        ),
        Box::new(RecordingAcknowledger {
            acknowledged: Arc::clone(&acknowledged),
            negatively_acknowledged: Arc::clone(&negatively_acknowledged),
        }),
    )));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(backend, ids);

    let error = match transport.receive::<BestEffortEvent>().await {
        Ok(_) => panic!("malformed payload must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(!acknowledged.load(Ordering::Acquire));
    assert!(negatively_acknowledged.load(Ordering::Acquire));
}

#[tokio::test]
async fn typed_postcard_process_next_acknowledges_a_successful_handler() {
    let acknowledged = Arc::new(AtomicBool::new(false));
    let negatively_acknowledged = Arc::new(AtomicBool::new(false));
    let codec = PostcardCodec;
    let backend = Arc::new(SingleDeliveryTransport::new(Delivery::with_acknowledger(
        Envelope::new(
            46,
            "best-effort",
            codec
                .encode_value(&BestEffortEvent(7))
                .expect("serializable event"),
            MessageMetadata::new(46, None),
        ),
        Box::new(RecordingAcknowledger {
            acknowledged: Arc::clone(&acknowledged),
            negatively_acknowledged: Arc::clone(&negatively_acknowledged),
        }),
    )));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(backend, ids);

    let outcome = transport
        .process_next::<BestEffortEvent, _>(|message| {
            Box::pin(async move {
                assert_eq!(message.0, 7);
                Ok(())
            })
        })
        .await
        .expect("processing succeeds");

    assert_eq!(outcome, PostcardProcessOutcome::Acknowledged);
    assert!(acknowledged.load(Ordering::Acquire));
    assert!(!negatively_acknowledged.load(Ordering::Acquire));
}

#[tokio::test]
async fn typed_postcard_process_next_returns_a_rejected_business_error_after_nack() {
    let acknowledged = Arc::new(AtomicBool::new(false));
    let negatively_acknowledged = Arc::new(AtomicBool::new(false));
    let codec = PostcardCodec;
    let backend = Arc::new(SingleDeliveryTransport::new(Delivery::with_acknowledger(
        Envelope::new(
            47,
            "best-effort",
            codec
                .encode_value(&BestEffortEvent(8))
                .expect("serializable event"),
            MessageMetadata::new(47, None),
        ),
        Box::new(RecordingAcknowledger {
            acknowledged: Arc::clone(&acknowledged),
            negatively_acknowledged: Arc::clone(&negatively_acknowledged),
        }),
    )));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(backend, ids);

    let outcome = transport
        .process_next::<BestEffortEvent, _>(|_| {
            Box::pin(async { Err(CatgaError::new(ErrorCode::Validation, "business rejection")) })
        })
        .await
        .expect("negative acknowledgement succeeds");

    assert_eq!(
        outcome,
        PostcardProcessOutcome::Rejected(CatgaError::new(
            ErrorCode::Validation,
            "business rejection",
        ))
    );
    assert!(!acknowledged.load(Ordering::Acquire));
    assert!(negatively_acknowledged.load(Ordering::Acquire));
}

#[tokio::test]
async fn typed_postcard_process_next_from_acknowledges_the_selected_destination() {
    let backend = Arc::new(MemoryTransport::new(2).expect("valid memory transport"));
    let destination = Destination::parse("orders").expect("valid destination");
    backend
        .declare_destination(destination)
        .expect("destination is declared");
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);
    transport
        .send_to("orders", &BestEffortEvent(9))
        .await
        .expect("destination message is sent");

    let outcome = transport
        .process_next_from::<BestEffortEvent, _>("orders", |message| {
            Box::pin(async move {
                assert_eq!(message.0, 9);
                Ok(())
            })
        })
        .await
        .expect("destination processing succeeds");

    assert_eq!(outcome, PostcardProcessOutcome::Acknowledged);
}

#[tokio::test]
async fn typed_postcard_destination_event_batch_decodes_from_the_selected_queue() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let destination = Destination::parse("orders").expect("valid destination");
    backend
        .declare_destination(destination.clone())
        .expect("destination is declared");
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .send_event_batch_to_with_concurrency("orders", [BestEffortEvent(3), BestEffortEvent(4)], 2)
        .await
        .expect("destination event batch sends");

    for expected in [3, 4] {
        let delivery = transport
            .receive_from::<BestEffortEvent>("orders")
            .await
            .expect("typed destination delivery decodes");
        assert_eq!(delivery.message().0, expected);
        assert_eq!(
            delivery.envelope().metadata().quality_of_service(),
            QualityOfService::AtMostOnce
        );
        delivery
            .acknowledge()
            .await
            .expect("typed destination acknowledgement succeeds");
    }
}

#[tokio::test]
async fn typed_postcard_messages_publish_and_send_with_at_least_once_metadata() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let destination = Destination::parse("commands").expect("valid destination");
    backend
        .declare_destination(destination.clone())
        .expect("destination is declared");
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .publish(&OutboundCommand(5))
        .await
        .expect("message publishes");
    let published = transport
        .receive::<OutboundCommand>()
        .await
        .expect("published message decodes");
    assert_eq!(published.message().0, 5);
    assert_eq!(
        published.envelope().metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    published
        .acknowledge()
        .await
        .expect("published message acknowledgement succeeds");

    transport
        .send_to("commands", &OutboundCommand(6))
        .await
        .expect("message is sent");
    let sent = transport
        .receive_from::<OutboundCommand>("commands")
        .await
        .expect("sent message decodes");
    assert_eq!(sent.message().0, 6);
    assert_eq!(
        sent.envelope().metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    sent.acknowledge()
        .await
        .expect("sent message acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_contextual_publish_and_send_preserve_headers() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let destination = Destination::parse("commands").expect("valid destination");
    backend
        .declare_destination(destination.clone())
        .expect("destination is declared");
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);
    let headers = EnvelopeHeaders::try_new([("tenant", "blue")]).expect("valid headers");

    transport
        .publish_with_headers(&OutboundCommand(5), &headers)
        .await
        .expect("contextual message publishes");
    let published = backend.receive().await.expect("contextual message arrives");
    assert_eq!(published.envelope().header("tenant"), Some("blue"));
    published
        .acknowledge()
        .await
        .expect("contextual acknowledgement succeeds");

    transport
        .send_to_with_headers("commands", &OutboundCommand(6), &headers)
        .await
        .expect("contextual message sends");
    let sent = backend
        .receive_from(&destination)
        .await
        .expect("contextual destination message arrives");
    assert_eq!(sent.envelope().header("tenant"), Some("blue"));
    sent.acknowledge()
        .await
        .expect("contextual destination acknowledgement succeeds");
}

#[tokio::test]
async fn typed_postcard_event_batch_uses_the_core_default_concurrency_bound() {
    let backend = Arc::new(MemoryTransport::new(4).expect("valid memory transport"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let transport = PostcardTransport::new(Arc::clone(&backend), ids);

    transport
        .publish_event_batch([BestEffortEvent(10), BestEffortEvent(11)])
        .await
        .expect("default event batch publishes");

    for _ in 0..2 {
        let delivery = backend.receive().await.expect("default batch item arrives");
        assert_eq!(
            delivery.envelope().metadata().quality_of_service(),
            QualityOfService::AtMostOnce
        );
        delivery
            .acknowledge()
            .await
            .expect("default batch acknowledgement succeeds");
    }
}
