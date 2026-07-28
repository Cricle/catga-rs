//! Behavioral tests for the generic statically typed transport facade.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, Destination, DestinationTransport,
    DistributedIdGenerator, Envelope, EnvelopeHeaders, ErrorCode, Event, Message,
    MessageDestinationRouter, MessageMetadata, MessagePriority, MessageTransport, PayloadDecoder,
    PayloadEncoder, QualityOfService, SnowflakeIdGenerator, SnowflakeLayout, TypedProcessOutcome,
    TypedTransport,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestMessage(u8);

impl Message for TestMessage {}

impl Event for TestMessage {}

#[derive(Default)]
struct AcknowledgementCounts {
    acknowledged: AtomicUsize,
    negatively_acknowledged: AtomicUsize,
}

struct RecordingAcknowledger {
    counts: Arc<AcknowledgementCounts>,
}

struct FailingAcknowledger;

#[async_trait]
impl Acknowledger for FailingAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "test acknowledgement backend is unavailable",
        ))
    }
}

#[async_trait]
impl Acknowledger for RecordingAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.counts.acknowledged.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.counts
            .negatively_acknowledged
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct DeliveryTransport {
    delivery: Mutex<Option<Delivery>>,
    published: Mutex<Vec<Envelope>>,
}

impl DeliveryTransport {
    fn with_delivery(delivery: Delivery) -> Self {
        Self {
            delivery: Mutex::new(Some(delivery)),
            published: Mutex::new(Vec::new()),
        }
    }

    fn without_delivery() -> Self {
        Self {
            delivery: Mutex::new(None),
            published: Mutex::new(Vec::new()),
        }
    }

    fn take_delivery(&self) -> CatgaResult<Delivery> {
        let mut delivery = self.delivery.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "typed transport test delivery lock is poisoned",
            )
        })?;
        delivery.take().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "typed transport test has no remaining delivery",
            )
        })
    }

    fn publication_count(&self) -> CatgaResult<usize> {
        self.published
            .lock()
            .map(|published| published.len())
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed transport test publication lock is poisoned",
                )
            })
    }

    fn first_payload(&self) -> CatgaResult<Vec<u8>> {
        let published = self.published.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "typed transport test publication lock is poisoned",
            )
        })?;
        published
            .first()
            .map(|envelope| envelope.payload().to_vec())
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::NotFound,
                    "typed transport test has no published envelope",
                )
            })
    }

    fn published_envelopes(&self) -> CatgaResult<Vec<Envelope>> {
        self.published
            .lock()
            .map(|published| published.clone())
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed transport test publication lock is poisoned",
                )
            })
    }
}

#[async_trait]
impl MessageTransport for DeliveryTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.published
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed transport test publication lock is poisoned",
                )
            })?
            .push(envelope);
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.take_delivery()
    }
}

struct DestinationDeliveryTransport {
    delivery: Mutex<Option<Delivery>>,
    sent: Mutex<Vec<Destination>>,
}

struct FailingPublishTransport {
    attempted_payloads: Mutex<Vec<u8>>,
}

impl FailingPublishTransport {
    fn attempted_payloads(&self) -> CatgaResult<Vec<u8>> {
        self.attempted_payloads
            .lock()
            .map(|payloads| payloads.clone())
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed transport test failure recorder lock is poisoned",
                )
            })
    }
}

#[async_trait]
impl MessageTransport for FailingPublishTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let payload = envelope.payload().first().copied().ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "test envelope payload is empty")
        })?;
        self.attempted_payloads
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed transport test failure recorder lock is poisoned",
                )
            })?
            .push(payload);
        if payload == 2 {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "test transport rejected payload",
            ));
        }
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::NotFound,
            "failing publish transport has no deliveries",
        ))
    }
}

impl DestinationDeliveryTransport {
    fn new(delivery: Delivery) -> Self {
        Self {
            delivery: Mutex::new(Some(delivery)),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn take_delivery(&self) -> CatgaResult<Delivery> {
        let mut delivery = self.delivery.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "typed destination transport test delivery lock is poisoned",
            )
        })?;
        delivery.take().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "typed destination transport test has no remaining delivery",
            )
        })
    }

    fn sent_destinations(&self) -> CatgaResult<Vec<Destination>> {
        self.sent
            .lock()
            .map(|destinations| destinations.clone())
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed destination send lock is poisoned",
                )
            })
    }
}

#[async_trait]
impl MessageTransport for DestinationDeliveryTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "destination-only typed transport test",
        ))
    }
}

#[async_trait]
impl DestinationTransport for DestinationDeliveryTransport {
    async fn send_to(&self, destination: &Destination, _: Envelope) -> CatgaResult<()> {
        self.sent
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "typed destination send lock is poisoned",
                )
            })?
            .push(destination.clone());
        Ok(())
    }

    async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
        self.take_delivery()
    }
}

struct DecodeFailureCodec;

impl PayloadDecoder<TestMessage> for DecodeFailureCodec {
    fn decode_payload(&self, _: &[u8]) -> CatgaResult<TestMessage> {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "test payload cannot be decoded",
        ))
    }
}

struct EncoderOnlyCodec {
    encoded: AtomicUsize,
}

impl PayloadEncoder<TestMessage> for EncoderOnlyCodec {
    fn encode_payload(&self, value: &TestMessage) -> CatgaResult<Vec<u8>> {
        self.encoded.fetch_add(1, Ordering::Relaxed);
        Ok(vec![value.0])
    }
}

struct TestCodec;

impl PayloadEncoder<TestMessage> for TestCodec {
    fn encode_payload(&self, value: &TestMessage) -> CatgaResult<Vec<u8>> {
        Ok(vec![value.0])
    }
}

impl PayloadDecoder<TestMessage> for TestCodec {
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<TestMessage> {
        match bytes {
            [value] => Ok(TestMessage(*value)),
            _ => Err(CatgaError::new(
                ErrorCode::Validation,
                "test payload must contain exactly one byte",
            )),
        }
    }
}

fn ids() -> CatgaResult<Arc<dyn DistributedIdGenerator>> {
    Ok(Arc::new(SnowflakeIdGenerator::new(
        1,
        SnowflakeLayout::default(),
    )?))
}

fn delivery(payload: Vec<u8>, counts: Arc<AcknowledgementCounts>) -> Delivery {
    Delivery::with_acknowledger(
        Envelope::new(
            1,
            "typed.transport.test",
            payload,
            MessageMetadata::new(1, None),
        ),
        Box::new(RecordingAcknowledger { counts }),
    )
}

#[tokio::test]
async fn send_routed_uses_the_message_type_destination_without_encoding_unknown_types()
-> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let backend = Arc::new(DestinationDeliveryTransport::new(delivery(vec![0], counts)));
    let codec = Arc::new(EncoderOnlyCodec {
        encoded: AtomicUsize::new(0),
    });
    let transport =
        TypedTransport::new_with_shared_codec(Arc::clone(&backend), ids()?, Arc::clone(&codec));
    let mut router = MessageDestinationRouter::new();
    router.add_route(
        std::any::type_name::<TestMessage>(),
        Destination::parse("orders")?,
    )?;

    transport.send_routed(&router, &TestMessage(9)).await?;
    assert_eq!(
        backend.sent_destinations()?,
        [Destination::parse("orders")?]
    );
    assert_eq!(codec.encoded.load(Ordering::Relaxed), 1);

    let missing = transport
        .send_routed(&MessageDestinationRouter::new(), &TestMessage(10))
        .await;
    assert!(matches!(missing, Err(error) if error.code() == ErrorCode::NotFound));
    assert_eq!(codec.encoded.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn receive_negative_acknowledges_a_delivery_when_decoding_fails() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let backend = Arc::new(DeliveryTransport::with_delivery(delivery(
        vec![9],
        Arc::clone(&counts),
    )));
    let transport = TypedTransport::new_with_codec(backend, ids()?, DecodeFailureCodec);

    let result = transport.receive::<TestMessage>().await;

    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 0);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn encoder_only_codec_publishes_without_a_payload_decoder() -> CatgaResult<()> {
    let backend = Arc::new(DeliveryTransport::without_delivery());
    let codec = Arc::new(EncoderOnlyCodec {
        encoded: AtomicUsize::new(0),
    });
    let transport =
        TypedTransport::new_with_shared_codec(Arc::clone(&backend), ids()?, Arc::clone(&codec));

    transport.publish(&TestMessage(42)).await?;

    assert_eq!(codec.encoded.load(Ordering::Relaxed), 1);
    assert_eq!(backend.publication_count()?, 1);
    assert_eq!(backend.first_payload()?, vec![42]);
    Ok(())
}

#[tokio::test]
async fn publish_batch_with_zero_concurrency_returns_validation_before_encoding_or_publishing()
-> CatgaResult<()> {
    let backend = Arc::new(DeliveryTransport::without_delivery());
    let codec = Arc::new(EncoderOnlyCodec {
        encoded: AtomicUsize::new(0),
    });
    let transport =
        TypedTransport::new_with_shared_codec(Arc::clone(&backend), ids()?, Arc::clone(&codec));

    let result = transport
        .publish_batch_with_concurrency(vec![TestMessage(3)], 0)
        .await;

    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    assert_eq!(codec.encoded.load(Ordering::Relaxed), 0);
    assert_eq!(backend.publication_count()?, 0);
    Ok(())
}

#[tokio::test]
async fn process_next_from_acknowledges_successful_handlers() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let backend = Arc::new(DestinationDeliveryTransport::new(delivery(
        vec![7],
        Arc::clone(&counts),
    )));
    let transport = TypedTransport::new_with_codec(backend, ids()?, TestCodec);
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_handler = Arc::clone(&handled);

    let outcome = transport
        .process_next_from::<TestMessage, _>("orders", move |message| {
            assert_eq!(message, &TestMessage(7));
            handled_by_handler.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        })
        .await?;

    assert_eq!(outcome, TypedProcessOutcome::Acknowledged);
    assert_eq!(handled.load(Ordering::Relaxed), 1);
    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 1);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn process_next_from_negative_acknowledges_failed_handlers() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let backend = Arc::new(DestinationDeliveryTransport::new(delivery(
        vec![8],
        Arc::clone(&counts),
    )));
    let transport = TypedTransport::new_with_codec(backend, ids()?, TestCodec);
    let handler_error = CatgaError::new(ErrorCode::Conflict, "test handler rejected delivery");
    let expected_error = handler_error.clone();

    let outcome = transport
        .process_next_from::<TestMessage, _>("orders", move |_| {
            Box::pin(async move { Err(handler_error) })
        })
        .await?;

    assert_eq!(outcome, TypedProcessOutcome::Rejected(expected_error));
    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 0);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn nested_publication_inherits_context_and_merges_explicit_headers() -> CatgaResult<()> {
    let inherited_headers =
        EnvelopeHeaders::try_new([("tenant", "north"), ("request", "incoming")])?;
    let inbound = Delivery::new(
        Envelope::new(
            7,
            "typed.transport.context",
            vec![0],
            MessageMetadata::new(7, Some(73)).with_priority(MessagePriority::Critical),
        )
        .with_headers(inherited_headers),
    );
    let backend = Arc::new(DeliveryTransport::with_delivery(inbound));
    let transport = TypedTransport::new_with_codec(Arc::clone(&backend), ids()?, TestCodec);
    let explicit_headers =
        EnvelopeHeaders::try_new([("request", "outgoing"), ("operation", "charge")])?;

    let delivery = transport.receive::<TestMessage>().await?;
    delivery
        .with_transport_context(async {
            transport
                .publish_with_headers(&TestMessage(9), &explicit_headers)
                .await
        })
        .await?;

    let published = backend.published_envelopes()?;
    assert_eq!(published.len(), 1);
    let envelope = &published[0];
    assert_eq!(envelope.metadata().correlation_id(), Some(73));
    assert_eq!(envelope.metadata().priority(), MessagePriority::Critical);
    assert_eq!(
        envelope.metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    assert_eq!(envelope.header("tenant"), Some("north"));
    assert_eq!(envelope.header("request"), Some("outgoing"));
    assert_eq!(envelope.header("operation"), Some("charge"));
    assert!(envelope.sent_at_unix_ms().is_some());
    Ok(())
}

#[tokio::test]
async fn event_publication_uses_at_most_once_and_reliable_event_uses_at_least_once()
-> CatgaResult<()> {
    let backend = Arc::new(DeliveryTransport::without_delivery());
    let transport = TypedTransport::new_with_codec(Arc::clone(&backend), ids()?, TestCodec);

    transport.publish_event(&TestMessage(1)).await?;
    transport.publish_reliable_event(&TestMessage(2)).await?;

    let published = backend.published_envelopes()?;
    assert_eq!(published.len(), 2);
    assert_eq!(
        published[0].metadata().quality_of_service(),
        QualityOfService::AtMostOnce
    );
    assert_eq!(
        published[1].metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    Ok(())
}

#[tokio::test]
async fn batch_publication_drains_all_inputs_before_returning_the_first_failure() -> CatgaResult<()>
{
    let backend = Arc::new(FailingPublishTransport {
        attempted_payloads: Mutex::new(Vec::new()),
    });
    let transport = TypedTransport::new_with_codec(Arc::clone(&backend), ids()?, TestCodec);

    let error = transport
        .publish_batch_with_concurrency([TestMessage(1), TestMessage(2), TestMessage(3)], 1)
        .await
        .expect_err("the rejected payload must fail the drained batch");

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(backend.attempted_payloads()?, [1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn handler_acknowledgement_failure_is_not_reclassified_as_handler_failure() -> CatgaResult<()>
{
    let delivery = Delivery::with_acknowledger(
        Envelope::new(
            1,
            "typed.transport.ack-failure",
            vec![4],
            MessageMetadata::new(1, None),
        ),
        Box::new(FailingAcknowledger),
    );
    let transport = TypedTransport::new_with_codec(
        Arc::new(DeliveryTransport::with_delivery(delivery)),
        ids()?,
        TestCodec,
    );

    let error = transport
        .process_next::<TestMessage, _>(|message| {
            assert_eq!(message, &TestMessage(4));
            Box::pin(async { Ok(()) })
        })
        .await
        .expect_err("a failed acknowledgement leaves delivery ownership unresolved");

    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}
