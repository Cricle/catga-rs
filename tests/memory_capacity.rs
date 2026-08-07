//! In-memory durable-store capacity tests.

use std::time::Duration;

use catga_core::memory::{MemoryIdempotency, MemoryInbox, MemoryOutbox};
use catga_core::{
    Envelope, ErrorCode, IdempotencyStore, InboxStore, MessageMetadata, OutboxMessage, OutboxStore,
};

fn message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "orders.created",
        Vec::new(),
        MessageMetadata::new(id, None),
    ))
}

#[tokio::test]
async fn memory_inbox_rejects_new_claims_when_its_record_capacity_is_exhausted() {
    let inbox = MemoryInbox::new(1).expect("positive capacity is valid");
    assert!(
        inbox
            .try_claim(1)
            .await
            .expect("first inbox claim succeeds")
            .is_some()
    );

    assert!(matches!(
        inbox.try_claim(2).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
}

#[tokio::test]
async fn memory_idempotency_rejects_new_keys_when_its_record_capacity_is_exhausted() {
    let store = MemoryIdempotency::new(1).expect("positive capacity is valid");
    assert!(store.try_claim("first").await.expect("first key claims"));

    assert!(matches!(
        store.try_claim("second").await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
}

#[tokio::test]
async fn memory_outbox_rejects_new_messages_when_its_record_capacity_is_exhausted() {
    let store = MemoryOutbox::new(1).expect("positive capacity is valid");
    store
        .enqueue(message(1))
        .await
        .expect("first message enqueues");

    assert!(matches!(
        store.enqueue(message(2)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
}

#[tokio::test]
async fn capacity_exhaustion_reclaims_expired_idempotency_records_without_a_worker() {
    let store = MemoryIdempotency::with_retention_and_capacity(Duration::from_millis(1), 1)
        .expect("valid bounded store");
    assert!(store.try_claim("first").await.expect("first key claims"));
    store
        .complete("first", None)
        .await
        .expect("first key completes");
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        store
            .try_claim("second")
            .await
            .expect("expired completed key is reclaimed on capacity pressure")
    );
}

#[tokio::test]
async fn capacity_exhaustion_reclaims_expired_inbox_records_without_a_worker() {
    let inbox = MemoryInbox::with_retention_and_capacity(Duration::from_millis(1), 1)
        .expect("valid bounded inbox");
    let claim = inbox
        .try_claim(1)
        .await
        .expect("first message claims")
        .expect("first message is owned");
    inbox
        .complete(claim, None)
        .await
        .expect("first message completes");
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert!(
        inbox
            .try_claim(2)
            .await
            .expect("expired completed message is reclaimed on capacity pressure")
            .is_some()
    );
}

#[tokio::test]
async fn capacity_exhaustion_reclaims_expired_published_outbox_records_without_a_worker() {
    let store = MemoryOutbox::with_published_retention_and_capacity(Duration::from_millis(1), 1)
        .expect("valid bounded outbox");
    store
        .enqueue(message(1))
        .await
        .expect("first message enqueues");
    let first = store
        .claim("worker", 1)
        .await
        .expect("first message claims")
        .pop()
        .expect("one message is claimed");
    store
        .ack("worker", 1, first.claim_token().expect("claim has token"))
        .await
        .expect("first message publishes");
    tokio::time::sleep(Duration::from_millis(20)).await;

    store
        .enqueue(message(2))
        .await
        .expect("expired published message is reclaimed on capacity pressure");
}
