//! In-memory reliability store tests.

use std::sync::Arc;

use catga_core::{
    DeadLetter, DeadLetterStore, Envelope, IdempotencyStore, InboxStore, MessageMetadata,
    ProcessingState,
};
use catga_memory::{MemoryDeadLetters, MemoryIdempotency, MemoryInbox};

#[tokio::test]
async fn inbox_and_idempotency_claim_exclusively_cache_results_and_allow_retry_after_failure() {
    let inbox = MemoryInbox::default();
    assert!(inbox.try_claim(7).await.unwrap());
    assert!(!inbox.try_claim(7).await.unwrap());
    inbox
        .complete(7, Some(Arc::from([1_u8, 2, 3])))
        .await
        .unwrap();
    assert_eq!(
        inbox.state(7).await.unwrap(),
        Some(ProcessingState::Completed)
    );
    assert_eq!(
        inbox.result(7).await.unwrap().as_deref(),
        Some(&[1, 2, 3][..])
    );
    assert!(!inbox.try_claim(7).await.unwrap());

    let idempotency = MemoryIdempotency::default();
    assert!(idempotency.try_claim("create:7").await.unwrap());
    idempotency.fail("create:7").await.unwrap();
    assert!(idempotency.try_claim("create:7").await.unwrap());
    idempotency
        .complete("create:7", Some(Arc::from([9_u8])))
        .await
        .unwrap();
    assert_eq!(
        idempotency.result("create:7").await.unwrap().as_deref(),
        Some(&[9][..])
    );
}

#[tokio::test]
async fn bounded_dead_letter_store_preserves_entries_without_unbounded_growth() {
    let dead_letters = MemoryDeadLetters::new(1).unwrap();
    let message = Envelope::new(
        9,
        "order.created",
        vec![1, 2],
        MessageMetadata::new(9, None),
    );
    dead_letters
        .enqueue(DeadLetter::new(message.clone(), "handler failed", 2))
        .await
        .unwrap();
    assert_eq!(dead_letters.list(10).await.unwrap().len(), 1);
    assert!(
        dead_letters
            .enqueue(DeadLetter::new(message, "handler failed again", 3))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inbox_claims_have_exactly_one_owner() {
    let inbox = Arc::new(MemoryInbox::default());
    let mut claims = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let inbox = Arc::clone(&inbox);
        claims.spawn(async move { inbox.try_claim(42).await.unwrap() });
    }

    let mut owners = 0;
    while let Some(claim) = claims.join_next().await {
        owners += usize::from(claim.unwrap());
    }
    assert_eq!(owners, 1);
}
