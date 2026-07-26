//! Direct contract tests for the Core typed transport facade.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, DistributedIdGenerator, Envelope, ErrorCode,
    Message, MessageMetadata, MessageTransport, PayloadDecoder, PayloadEncoder,
    SnowflakeIdGenerator, SnowflakeLayout, TypedTransport,
};

#[derive(Debug)]
struct TestMessage(u8);

impl Message for TestMessage {}

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
