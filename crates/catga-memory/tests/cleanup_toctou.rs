//! Memory inbox and outbox cleanup atomicity regression tests.

#![cfg(feature = "test-hooks")]

use std::{
    sync::{Arc, Barrier},
    time::Duration,
};

use catga_core::{
    Envelope, IdempotencyStore, InboxStore, MessageMetadata, OutboxMessage, OutboxStore,
    ProcessingState,
};
use catga_memory::{MemoryIdempotency, MemoryInbox, MemoryOutbox};

#[tokio::test]
async fn inbox_cleanup_keeps_a_recreated_completed_record_and_its_capacity()
-> catga_core::CatgaResult<()> {
    let store = Arc::new(MemoryInbox::with_retention_and_capacity(
        Duration::from_secs(60),
        1,
    )?);
    let message_id = 41;
    complete_inbox(&store, message_id, Arc::<[u8]>::from([1_u8])).await?;

    let observed = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    store.pause_next_cleanup_after_snapshot(Arc::clone(&observed), Arc::clone(&resume))?;
    let cleanup = start_inbox_cleanup(Arc::clone(&store));
    observed.wait();

    assert_eq!(store.cleanup_completed(Duration::ZERO, 1).await?, 1);
    complete_inbox(&store, message_id, Arc::<[u8]>::from([2_u8])).await?;

    resume.wait();
    assert_eq!(cleanup.join().expect("cleanup thread completes")?, 0);
    assert_eq!(
        store.state(message_id).await?,
        Some(ProcessingState::Completed)
    );
    assert_eq!(
        store.result(message_id).await?.as_deref(),
        Some(&[2_u8][..])
    );
    assert!(store.try_claim(message_id + 1).await.is_err());
    Ok(())
}

#[tokio::test]
async fn idempotency_cleanup_keeps_a_recreated_completed_record_and_its_capacity()
-> catga_core::CatgaResult<()> {
    let store = Arc::new(MemoryIdempotency::with_retention_and_capacity(
        Duration::from_secs(60),
        1,
    )?);
    let key = "cleanup-toctou";
    complete_idempotency(&store, key, Arc::<[u8]>::from([1_u8])).await?;
    store.expire_completed_index_for_test(key)?;

    let observed = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    store.pause_next_cleanup_after_snapshot(Arc::clone(&observed), Arc::clone(&resume))?;
    let cleanup = start_idempotency_cleanup(Arc::clone(&store));
    observed.wait();

    assert_eq!(store.cleanup_completed(1).await?, 1);
    complete_idempotency(&store, key, Arc::<[u8]>::from([2_u8])).await?;

    resume.wait();
    assert_eq!(cleanup.join().expect("cleanup thread completes")?, 0);
    assert_eq!(store.state(key).await?, Some(ProcessingState::Completed));
    assert_eq!(store.result(key).await?.as_deref(), Some(&[2_u8][..]));
    assert!(store.try_claim("cleanup-toctou-next").await.is_err());
    Ok(())
}

#[tokio::test]
async fn outbox_cleanup_keeps_a_recreated_published_record_and_its_capacity()
-> catga_core::CatgaResult<()> {
    let store = Arc::new(MemoryOutbox::with_published_retention_and_capacity(
        Duration::from_secs(60),
        1,
    )?);
    let id = 73;
    publish_outbox(&store, id).await?;

    let observed = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    store.pause_next_cleanup_after_snapshot(Arc::clone(&observed), Arc::clone(&resume))?;
    let cleanup = start_outbox_cleanup(Arc::clone(&store));
    observed.wait();

    assert_eq!(store.cleanup_published(Duration::ZERO, 1).await?, 1);
    publish_outbox(&store, id).await?;

    resume.wait();
    assert_eq!(cleanup.join().expect("cleanup thread completes")?, 0);
    let published = store.list_published(1).await?;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id(), id);
    assert!(store.enqueue(outbox_message(id + 1)).await.is_err());
    Ok(())
}

async fn complete_inbox(
    store: &MemoryInbox,
    message_id: u64,
    result: Arc<[u8]>,
) -> catga_core::CatgaResult<()> {
    let claim = store
        .try_claim(message_id)
        .await?
        .expect("a vacant inbox record is claimed");
    store.complete(claim, Some(result)).await
}

async fn complete_idempotency(
    store: &MemoryIdempotency,
    key: &str,
    result: Arc<[u8]>,
) -> catga_core::CatgaResult<()> {
    assert!(store.try_claim(key).await?);
    store.complete(key, Some(result)).await
}

async fn publish_outbox(store: &MemoryOutbox, id: u64) -> catga_core::CatgaResult<()> {
    store.enqueue(outbox_message(id)).await?;
    let claimed = store.claim("worker", 1).await?;
    let token = claimed[0]
        .claim_token()
        .expect("claimed outbox message has a token")
        .to_owned();
    store.ack("worker", id, &token).await
}

fn outbox_message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "tests.memory.cleanup",
        Vec::new(),
        MessageMetadata::new(id, None),
    ))
}

fn start_inbox_cleanup(
    store: Arc<MemoryInbox>,
) -> std::thread::JoinHandle<catga_core::CatgaResult<usize>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("cleanup runtime builds")
            .block_on(async move { store.cleanup_completed(Duration::ZERO, 1).await })
    })
}

fn start_idempotency_cleanup(
    store: Arc<MemoryIdempotency>,
) -> std::thread::JoinHandle<catga_core::CatgaResult<usize>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("cleanup runtime builds")
            .block_on(async move { store.cleanup_completed(1).await })
    })
}

fn start_outbox_cleanup(
    store: Arc<MemoryOutbox>,
) -> std::thread::JoinHandle<catga_core::CatgaResult<usize>> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("cleanup runtime builds")
            .block_on(async move { store.cleanup_published(Duration::ZERO, 1).await })
    })
}
