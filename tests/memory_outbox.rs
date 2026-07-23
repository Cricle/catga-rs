//! In-memory outbox concurrency tests.

use std::sync::Arc;

use catga_core::{Envelope, MessageMetadata, OutboxMessage, OutboxStore};
use catga_memory::MemoryOutbox;

fn message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "order.created",
        vec![1],
        MessageMetadata::new(id, None),
    ))
}

#[tokio::test]
async fn concurrent_claims_do_not_duplicate_a_pending_message() {
    let store = Arc::new(MemoryOutbox::default());
    store.enqueue(message(1)).await.unwrap();

    let first = {
        let store = store.clone();
        async move { store.claim("first", 1).await.unwrap() }
    };
    let second = {
        let store = store.clone();
        async move { store.claim("second", 1).await.unwrap() }
    };
    let (first, second) = tokio::join!(first, second);

    assert_eq!(first.len() + second.len(), 1);
    let owner = first.first().or(second.first()).unwrap().owner();
    store.ack(owner.unwrap(), 1).await.unwrap();
    assert!(store.claim("third", 1).await.unwrap().is_empty());
}
