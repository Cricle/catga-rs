//! End-to-end at-least-once CQRS delivery tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CachedResultCodec, CatgaError, CatgaResult, DeliveryMode, Envelope, ErrorCode, Handler,
    InboxBehavior, InboxKey, Mediator, MessageMetadata, MessagePriority, MessageTransport,
    OutboxBehavior, OutboxEnvelope, OutboxProcessor, OutboxStore, Pipeline, QualityOfService,
    Registry, Request,
};
use catga_core::memory::{MemoryInbox, MemoryOutbox, MemoryTransport};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct CreateOrder {
    id: u64,
    quantity: u64,
    metadata: MessageMetadata,
}

impl catga_core::Message for CreateOrder {}

impl Request for CreateOrder {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl OutboxEnvelope for CreateOrder {
    fn outbox_envelope(&self) -> Envelope {
        Envelope::versioned(
            self.id,
            "orders.created",
            self.quantity.to_le_bytes().to_vec(),
            self.metadata,
            3,
        )
        .with_reply_to("orders.reply")
    }
}

struct CreateOrderHandler;

#[async_trait]
impl Handler<CreateOrder> for CreateOrderHandler {
    async fn handle(&self, message: CreateOrder) -> CatgaResult<u64> {
        Ok(message.quantity)
    }
}

struct BlockingCreateOrderHandler(Arc<tokio::sync::Notify>);

#[async_trait]
impl Handler<CreateOrder> for BlockingCreateOrderHandler {
    async fn handle(&self, _: CreateOrder) -> CatgaResult<u64> {
        self.0.notify_one();
        std::future::pending::<CatgaResult<u64>>().await
    }
}

#[derive(Debug)]
struct DeliveredOrder {
    id: u64,
    quantity: u64,
}

impl DeliveredOrder {
    fn from_envelope(envelope: &Envelope) -> CatgaResult<Self> {
        let bytes: [u8; 8] = envelope.payload().try_into().map_err(|_| {
            CatgaError::new(catga_core::ErrorCode::Internal, "invalid order payload")
        })?;
        Ok(Self {
            id: envelope.id(),
            quantity: u64::from_le_bytes(bytes),
        })
    }
}

impl catga_core::Message for DeliveredOrder {}

impl Request for DeliveredOrder {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl InboxKey for DeliveredOrder {
    fn inbox_message_id(&self) -> u64 {
        self.id
    }
}

struct ApplyOrder(Arc<AtomicUsize>);

#[async_trait]
impl Handler<DeliveredOrder> for ApplyOrder {
    async fn handle(&self, order: DeliveredOrder) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(order.quantity)
    }
}

struct U64Codec;

impl CachedResultCodec<u64> for U64Codec {
    fn encode(&self, value: &u64) -> CatgaResult<Arc<[u8]>> {
        Ok(Arc::from(value.to_le_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u64> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| CatgaError::new(catga_core::ErrorCode::Internal, "invalid cached u64"))?;
        Ok(u64::from_le_bytes(bytes))
    }
}

fn mediator<M, H>(handler: H) -> Mediator
where
    M: Request,
    H: Handler<M> + 'static,
{
    let mut registry = Registry::new();
    registry
        .register_request::<M, _>(handler)
        .expect("test handler registration must be unique");
    Mediator::new(registry)
}

#[tokio::test]
async fn at_least_once_pipeline_preserves_metadata_and_applies_a_redelivery_once() -> CatgaResult<()>
{
    let outbox = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(MemoryTransport::new(2)?);
    let producer = mediator::<CreateOrder, _>(CreateOrderHandler);
    let outbound = Pipeline::new().with(OutboxBehavior::new(Arc::clone(&outbox)));
    let metadata = MessageMetadata::new(71, Some(19))
        .with_quality_of_service(QualityOfService::AtLeastOnce)
        .with_delivery_mode(DeliveryMode::AsyncRetry)
        .with_priority(MessagePriority::High)
        .with_not_before_unix_ms(Some(1_700_000_000_123));

    assert_eq!(
        producer
            .send_with(
                CreateOrder {
                    id: 71,
                    quantity: 9,
                    metadata,
                },
                &outbound,
            )
            .await?,
        9
    );

    let processor = OutboxProcessor::new(Arc::clone(&outbox), Arc::clone(&transport), "writer", 1)?;
    assert_eq!(processor.flush_once().await?.published(), 1);
    let published = timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(catga_core::ErrorCode::Timeout, "outbox did not publish"))??;
    let envelope = published.envelope().clone();
    assert_eq!(envelope.id(), 71);
    assert_eq!(envelope.message_type(), "orders.created");
    assert_eq!(envelope.schema_version(), 3);
    assert_eq!(envelope.metadata(), metadata);
    assert_eq!(envelope.reply_to(), Some("orders.reply"));

    let invocations = Arc::new(AtomicUsize::new(0));
    let consumer = mediator::<DeliveredOrder, _>(ApplyOrder(Arc::clone(&invocations)));
    let inbound = Pipeline::new().with(InboxBehavior::new(
        Arc::new(MemoryInbox::default()),
        U64Codec,
    ));

    assert_eq!(
        consumer
            .send_with(DeliveredOrder::from_envelope(&envelope)?, &inbound)
            .await?,
        9
    );
    assert_eq!(
        consumer
            .send_with(DeliveredOrder::from_envelope(&envelope)?, &inbound)
            .await?,
        9
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cancelled_outbox_dispatches_never_enqueue_an_unpublished_envelope() -> CatgaResult<()> {
    let store = Arc::new(MemoryOutbox::default());
    let started = Arc::new(tokio::sync::Notify::new());
    let producer = Arc::new(mediator::<CreateOrder, _>(BlockingCreateOrderHandler(
        Arc::clone(&started),
    )));
    let outbound = Pipeline::new().with(OutboxBehavior::new(Arc::clone(&store)));
    let message = || CreateOrder {
        id: 72,
        quantity: 1,
        metadata: MessageMetadata::new(72, None),
    };

    let pre_cancelled = CancellationToken::new();
    pre_cancelled.cancel();
    assert!(matches!(
        producer
            .send_with_cancellation_and_pipeline(message(), &outbound, pre_cancelled)
            .await,
        Err(error) if error.code() == ErrorCode::Cancelled
    ));
    assert!(store.claim("reader", 1).await?.is_empty());

    let cancellation = CancellationToken::new();
    let dispatch = tokio::spawn({
        let producer = Arc::clone(&producer);
        let cancellation = cancellation.clone();
        async move {
            producer
                .send_with_cancellation_and_pipeline(message(), &outbound, cancellation)
                .await
        }
    });
    started.notified().await;
    cancellation.cancel();
    assert!(matches!(
        dispatch
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?,
        Err(error) if error.code() == ErrorCode::Cancelled
    ));
    assert!(store.claim("reader", 1).await?.is_empty());
    Ok(())
}
