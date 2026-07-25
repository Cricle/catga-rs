//! Outbox processor tests.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, ErrorCode, MAX_OUTBOX_CLAIM_LIMIT,
    MessageMetadata, MessageTransport, OutboxMessage, OutboxProcessor, OutboxStore,
};
use catga_memory::{MemoryOutbox, MemoryTransport};

fn message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(id, None),
    ))
}

#[tokio::test]
async fn processor_publishes_claimed_messages_and_acknowledges_the_outbox() {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(MemoryTransport::new(1).unwrap());
    store.enqueue(message(1)).await.unwrap();
    let processor =
        OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker-a", 8).unwrap();

    assert_eq!(processor.flush_once().await.unwrap().published(), 1);
    assert_eq!(transport.receive().await.unwrap().envelope().id(), 1);
    assert!(store.claim("worker-b", 1).await.unwrap().is_empty());
}

struct FailOnceTransport(AtomicBool);

struct AlwaysFailTransport;

#[derive(Default)]
struct SlowTransport {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

#[async_trait]
impl MessageTransport for FailOnceTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        if !self.0.swap(true, Ordering::Relaxed) {
            return Err(CatgaError::new(ErrorCode::Transient, "offline"));
        }
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "test transport is send-only",
        ))
    }
}

#[async_trait]
impl MessageTransport for AlwaysFailTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Transient, "offline"))
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "test transport is send-only",
        ))
    }
}

#[async_trait]
impl MessageTransport for SlowTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "test transport is send-only",
        ))
    }
}

#[tokio::test]
async fn processor_releases_claims_after_transient_publish_failures() {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(FailOnceTransport(AtomicBool::new(false)));
    store.enqueue(message(2)).await.unwrap();
    let processor = OutboxProcessor::new(store, transport, "worker-a", 8).unwrap();

    assert_eq!(processor.flush_once().await.unwrap().failed(), 1);
    assert_eq!(processor.flush_once().await.unwrap().published(), 1);
}

#[tokio::test]
async fn processor_stops_retrying_after_the_outbox_failure_limit() -> CatgaResult<()> {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(AlwaysFailTransport);
    store.enqueue(message(21)).await?;
    let processor = OutboxProcessor::new(Arc::clone(&store), transport, "worker-a", 8)?;

    for _ in 0..3 {
        assert_eq!(processor.flush_once().await?.failed(), 1);
    }
    assert_eq!(processor.flush_once().await?.failed(), 0);
    assert!(store.claim("worker-b", 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn processor_bounds_concurrent_outbox_publication() {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(SlowTransport::default());
    for id in 1..=3 {
        store.enqueue(message(id)).await.unwrap();
    }
    let processor = OutboxProcessor::new_with_concurrency(
        Arc::clone(&store),
        Arc::clone(&transport),
        "worker-a",
        3,
        2,
    )
    .expect("positive concurrency must create a processor");

    assert_eq!(processor.flush_once().await.unwrap().published(), 3);
    assert_eq!(transport.max_in_flight.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn processor_rejects_a_batch_larger_than_the_outbox_memory_budget() -> CatgaResult<()> {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(MemoryTransport::new(1)?);

    assert!(matches!(
        OutboxProcessor::new(store, transport, "worker-a", MAX_OUTBOX_CLAIM_LIMIT + 1),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}
