//! Outbox processor tests.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, ErrorCode, MessageMetadata, MessageTransport,
    OutboxMessage, OutboxProcessor, OutboxStore,
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
    let transport = Arc::new(MemoryTransport::new(1));
    store.enqueue(message(1)).await.unwrap();
    let processor =
        OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker-a", 8).unwrap();

    assert_eq!(processor.flush_once().await.unwrap().published(), 1);
    assert_eq!(transport.receive().await.unwrap().envelope().id(), 1);
    assert!(store.claim("worker-b", 1).await.unwrap().is_empty());
}

struct FailOnceTransport(AtomicBool);

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

#[tokio::test]
async fn processor_releases_claims_after_transient_publish_failures() {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(FailOnceTransport(AtomicBool::new(false)));
    store.enqueue(message(2)).await.unwrap();
    let processor = OutboxProcessor::new(store, transport, "worker-a", 8).unwrap();

    assert_eq!(processor.flush_once().await.unwrap().failed(), 1);
    assert_eq!(processor.flush_once().await.unwrap().published(), 1);
}
