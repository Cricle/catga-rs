//! Behavioral tests for the generic statically typed transport facade.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, Destination, DestinationTransport,
    DistributedIdGenerator, Envelope, ErrorCode, Message, MessageMetadata, MessageTransport,
    PayloadDecoder, PayloadEncoder, SnowflakeIdGenerator, SnowflakeLayout, TypedProcessOutcome,
    TypedTransport,
};

#[derive(Debug, Eq, PartialEq)]
struct TestMessage(u8);

impl Message for TestMessage {}

#[derive(Default)]
struct AcknowledgementCounts {
    acknowledged: AtomicUsize,
    negatively_acknowledged: AtomicUsize,
}

struct RecordingAcknowledger {
    counts: Arc<AcknowledgementCounts>,
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
}

impl DestinationDeliveryTransport {
    fn new(delivery: Delivery) -> Self {
        Self {
            delivery: Mutex::new(Some(delivery)),
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
    async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
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
