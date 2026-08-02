//! Direct contract tests for the Core typed transport facade.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, Destination, DestinationTransport,
    DistributedIdGenerator, Envelope, EnvelopeHeaders, ErrorCode, Event, Message,
    MessageDestinationRouter, MessageMetadata, MessagePriority, MessageTransport, PayloadDecoder,
    PayloadEncoder, QualityOfService, SnowflakeIdGenerator, SnowflakeLayout, TypedTransport,
};

mod __catga_types {
    pub struct TestMessageTypeId;
    impl catga_core::MessageTypeId for TestMessageTypeId {
        const NAME: &'static str = "TestMessage";
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestMessage(u8);

impl Message for TestMessage {}

impl Event for TestMessage {
    type TypeId = __catga_types::TestMessageTypeId;
}

#[derive(Default)]
struct AcknowledgementCounts {
    acknowledged: AtomicUsize,
    negatively_acknowledged: AtomicUsize,
}

struct RecordingAcknowledger(Arc<AcknowledgementCounts>);

#[async_trait]
impl Acknowledger for RecordingAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0.acknowledged.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0
            .negatively_acknowledged
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct TestTransport {
    delivery: Mutex<Option<Delivery>>,
    published: AtomicUsize,
}

impl TestTransport {
    fn new(delivery: Option<Delivery>) -> Self {
        Self {
            delivery: Mutex::new(delivery),
            published: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl MessageTransport for TestTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        self.published.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.delivery
            .lock()
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "typed transport test lock poisoned")
            })?
            .take()
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "test delivery is unavailable"))
    }
}

struct DestinationRecordingTransport {
    delivery: Mutex<Option<Delivery>>,
    published: Mutex<Vec<Envelope>>,
    sent: Mutex<Vec<(Destination, Envelope)>>,
}

impl DestinationRecordingTransport {
    fn without_delivery() -> Self {
        Self {
            delivery: Mutex::new(None),
            published: Mutex::new(Vec::new()),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn with_delivery(delivery: Delivery) -> Self {
        Self {
            delivery: Mutex::new(Some(delivery)),
            published: Mutex::new(Vec::new()),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn sent(&self) -> CatgaResult<Vec<(Destination, Envelope)>> {
        self.sent.lock().map(|sent| sent.clone()).map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "typed destination test lock poisoned")
        })
    }

    fn published(&self) -> CatgaResult<Vec<Envelope>> {
        self.published
            .lock()
            .map(|published| published.clone())
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "typed publication test lock poisoned")
            })
    }

    fn take_delivery(&self) -> CatgaResult<Delivery> {
        self.delivery
            .lock()
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "typed destination test lock poisoned")
            })?
            .take()
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::NotFound,
                    "typed destination test delivery is unavailable",
                )
            })
    }
}

#[async_trait]
impl MessageTransport for DestinationRecordingTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.published
            .lock()
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "typed publication test lock poisoned")
            })?
            .push(envelope);
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.take_delivery()
    }
}

#[async_trait]
impl DestinationTransport for DestinationRecordingTransport {
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        self.sent
            .lock()
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "typed destination test lock poisoned")
            })?
            .push((destination.clone(), envelope));
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

struct EncoderOnlyCodec(AtomicUsize);

impl PayloadEncoder<TestMessage> for EncoderOnlyCodec {
    fn encode_payload(&self, message: &TestMessage) -> CatgaResult<Vec<u8>> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(vec![message.0])
    }
}

#[derive(Default)]
struct TestCodec;

impl PayloadEncoder<TestMessage> for TestCodec {
    fn encode_payload(&self, message: &TestMessage) -> CatgaResult<Vec<u8>> {
        Ok(vec![message.0])
    }
}

impl PayloadDecoder<TestMessage> for TestCodec {
    fn decode_payload(&self, payload: &[u8]) -> CatgaResult<TestMessage> {
        match payload {
            [value] => Ok(TestMessage(*value)),
            _ => Err(CatgaError::new(
                ErrorCode::Validation,
                "typed transport test payload must contain exactly one byte",
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

#[tokio::test]
async fn decoding_failure_nacks_the_original_core_delivery() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let delivery = Delivery::with_acknowledger(
        Envelope::new(
            1,
            "core.transport.test",
            vec![7],
            MessageMetadata::new(1, None),
        ),
        Box::new(RecordingAcknowledger(Arc::clone(&counts))),
    );
    let transport = TypedTransport::new_with_codec(
        Arc::new(TestTransport::new(Some(delivery))),
        ids()?,
        DecodeFailureCodec,
    );

    let result = transport.receive::<TestMessage>().await;

    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 0);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn publishing_requires_only_an_encoder_and_rejects_zero_batch_concurrency() -> CatgaResult<()>
{
    let backend = Arc::new(TestTransport::new(None));
    let codec = Arc::new(EncoderOnlyCodec(AtomicUsize::new(0)));
    let transport =
        TypedTransport::new_with_shared_codec(Arc::clone(&backend), ids()?, Arc::clone(&codec));

    transport.publish(&TestMessage(3)).await?;
    let zero_limit = transport
        .publish_batch_with_concurrency([TestMessage(4)], 0)
        .await;

    assert!(matches!(zero_limit, Err(error) if error.code() == ErrorCode::Validation));
    assert_eq!(codec.0.load(Ordering::Relaxed), 1);
    assert_eq!(backend.published.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn typed_destination_sends_preserve_destination_headers_and_delivery_guarantees()
-> CatgaResult<()> {
    let backend = Arc::new(DestinationRecordingTransport::without_delivery());
    let transport = TypedTransport::<_, TestCodec>::new(Arc::clone(&backend), ids()?);
    let headers = EnvelopeHeaders::try_new([("tenant", "north")])?;

    transport.send_to("orders", &TestMessage(1)).await?;
    transport
        .send_to_with_headers("orders", &TestMessage(2), &headers)
        .await?;
    transport.send_event_to("orders", &TestMessage(3)).await?;
    transport
        .send_reliable_event_to("orders", &TestMessage(4))
        .await?;

    let mut router = MessageDestinationRouter::new();
    router.add_route(
        std::any::type_name::<TestMessage>(),
        Destination::parse("routed-orders")?,
    )?;
    transport.send_routed(&router, &TestMessage(5)).await?;

    let sent = backend.sent()?;
    assert_eq!(sent.len(), 5);
    assert_eq!(sent[0].0, Destination::parse("orders")?);
    assert_eq!(sent[1].1.header("tenant"), Some("north"));
    assert_eq!(
        sent[2].1.metadata().quality_of_service(),
        QualityOfService::AtMostOnce
    );
    assert_eq!(
        sent[3].1.metadata().quality_of_service(),
        QualityOfService::AtLeastOnce
    );
    assert_eq!(sent[4].0, Destination::parse("routed-orders")?);
    Ok(())
}

#[tokio::test]
async fn typed_destination_batch_sends_apply_each_public_quality_of_service_variant()
-> CatgaResult<()> {
    let backend = Arc::new(DestinationRecordingTransport::without_delivery());
    let transport = TypedTransport::new_with_codec(Arc::clone(&backend), ids()?, TestCodec);

    transport.send_batch_to("orders", [TestMessage(1)]).await?;
    transport
        .send_event_batch_to("orders", [TestMessage(2)])
        .await?;
    transport
        .send_reliable_event_batch_to("orders", [TestMessage(3)])
        .await?;
    transport
        .send_batch_to_with_concurrency("orders", [TestMessage(4)], 1)
        .await?;
    transport
        .send_event_batch_to_with_concurrency("orders", [TestMessage(5)], 1)
        .await?;
    transport
        .send_reliable_event_batch_to_with_concurrency("orders", [TestMessage(6)], 1)
        .await?;

    let zero_limit = transport
        .send_batch_to_with_concurrency("orders", [TestMessage(7)], 0)
        .await;
    assert!(matches!(zero_limit, Err(error) if error.code() == ErrorCode::Validation));

    let qualities: Vec<_> = backend
        .sent()?
        .into_iter()
        .map(|(_, envelope)| envelope.metadata().quality_of_service())
        .collect();
    assert_eq!(
        qualities,
        [
            QualityOfService::AtLeastOnce,
            QualityOfService::AtMostOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtMostOnce,
            QualityOfService::AtLeastOnce,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn typed_destination_receive_retains_delivery_ownership() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let delivery = Delivery::with_acknowledger(
        Envelope::new(
            1,
            "core.destination.test",
            vec![7],
            MessageMetadata::new(1, Some(12)),
        ),
        Box::new(RecordingAcknowledger(Arc::clone(&counts))),
    )
    .with_attempts(3);
    let backend = Arc::new(DestinationRecordingTransport::with_delivery(delivery));
    let transport = TypedTransport::new_with_codec(backend, ids()?, TestCodec);

    let received = transport.receive_from::<TestMessage>("orders").await?;
    assert_eq!(received.message(), &TestMessage(7));
    assert_eq!(received.envelope().metadata().correlation_id(), Some(12));
    assert_eq!(received.attempts(), 3);
    received.nack().await?;

    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 0);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn typed_destination_process_next_from_acknowledges_successful_handlers() -> CatgaResult<()> {
    let counts = Arc::new(AcknowledgementCounts::default());
    let delivery = Delivery::with_acknowledger(
        Envelope::new(
            1,
            "core.destination.process-next.test",
            vec![7],
            MessageMetadata::new(1, None),
        ),
        Box::new(RecordingAcknowledger(Arc::clone(&counts))),
    );
    let transport = TypedTransport::new_with_codec(
        Arc::new(DestinationRecordingTransport::with_delivery(delivery)),
        ids()?,
        TestCodec,
    );

    let outcome = transport
        .process_next_from::<TestMessage, _>("orders", |message| {
            assert_eq!(message, &TestMessage(7));
            Box::pin(async { Ok(()) })
        })
        .await?;

    assert_eq!(outcome, catga_core::TypedProcessOutcome::Acknowledged);
    assert_eq!(counts.acknowledged.load(Ordering::Relaxed), 1);
    assert_eq!(counts.negatively_acknowledged.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn typed_publication_variants_preserve_headers_and_delivery_guarantees() -> CatgaResult<()> {
    let backend = Arc::new(DestinationRecordingTransport::without_delivery());
    let transport = TypedTransport::new_with_codec(Arc::clone(&backend), ids()?, TestCodec);
    let headers = EnvelopeHeaders::try_new([("tenant", "north")])?;

    transport.clone().publish(&TestMessage(1)).await?;
    transport
        .publish_with_headers(&TestMessage(2), &headers)
        .await?;
    transport.publish_event(&TestMessage(3)).await?;
    transport.publish_reliable_event(&TestMessage(4)).await?;
    transport.publish_batch([TestMessage(5)]).await?;
    transport.publish_event_batch([TestMessage(6)]).await?;
    transport
        .publish_reliable_event_batch([TestMessage(7)])
        .await?;
    transport
        .publish_batch_with_concurrency([TestMessage(8)], 1)
        .await?;
    transport
        .publish_event_batch_with_concurrency([TestMessage(9)], 1)
        .await?;
    transport
        .publish_reliable_event_batch_with_concurrency([TestMessage(10)], 1)
        .await?;

    let published = backend.published()?;
    assert_eq!(published.len(), 10);
    assert_eq!(published[1].header("tenant"), Some("north"));
    let qualities: Vec<_> = published
        .iter()
        .map(|envelope| envelope.metadata().quality_of_service())
        .collect();
    assert_eq!(
        qualities,
        [
            QualityOfService::AtLeastOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtMostOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtMostOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtLeastOnce,
            QualityOfService::AtMostOnce,
            QualityOfService::AtLeastOnce,
        ]
    );
    Ok(())
}

#[tokio::test]
async fn process_next_scopes_context_and_resolves_success_and_failure() -> CatgaResult<()> {
    let success_counts = Arc::new(AcknowledgementCounts::default());
    let inherited_headers = EnvelopeHeaders::try_new([("request", "incoming")])?;
    let successful_delivery = Delivery::with_acknowledger(
        Envelope::new(
            1,
            "core.process-next.test",
            vec![7],
            MessageMetadata::new(1, Some(12)),
        )
        .with_headers(inherited_headers),
        Box::new(RecordingAcknowledger(Arc::clone(&success_counts))),
    );
    let success_backend = Arc::new(DestinationRecordingTransport::with_delivery(
        successful_delivery,
    ));
    let success_transport =
        TypedTransport::new_with_codec(Arc::clone(&success_backend), ids()?, TestCodec);
    let nested_transport = success_transport.clone();

    let outcome = success_transport
        .process_next::<TestMessage, _>(move |message| {
            assert_eq!(message, &TestMessage(7));
            Box::pin(async move { nested_transport.publish(&TestMessage(9)).await })
        })
        .await?;

    assert_eq!(outcome, catga_core::TypedProcessOutcome::Acknowledged);
    assert_eq!(success_counts.acknowledged.load(Ordering::Relaxed), 1);
    let nested_publication = success_backend
        .published()?
        .into_iter()
        .next()
        .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "nested publication is unavailable"))?;
    assert_eq!(nested_publication.metadata().correlation_id(), Some(12));
    assert_eq!(nested_publication.header("request"), Some("incoming"));

    let failure_counts = Arc::new(AcknowledgementCounts::default());
    let failing_delivery = Delivery::with_acknowledger(
        Envelope::new(
            2,
            "core.process-next.test",
            vec![8],
            MessageMetadata::new(2, None),
        ),
        Box::new(RecordingAcknowledger(Arc::clone(&failure_counts))),
    );
    let failure_transport = TypedTransport::new_with_codec(
        Arc::new(DestinationRecordingTransport::with_delivery(
            failing_delivery,
        )),
        ids()?,
        TestCodec,
    );
    let handler_error = CatgaError::new(ErrorCode::Conflict, "test handler rejected delivery");
    let expected_error = handler_error.clone();

    let outcome = failure_transport
        .process_next::<TestMessage, _>(move |_| Box::pin(async move { Err(handler_error) }))
        .await?;

    assert_eq!(
        outcome,
        catga_core::TypedProcessOutcome::Rejected(expected_error)
    );
    assert_eq!(failure_counts.acknowledged.load(Ordering::Relaxed), 0);
    assert_eq!(
        failure_counts
            .negatively_acknowledged
            .load(Ordering::Relaxed),
        1
    );
    Ok(())
}
