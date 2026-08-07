//! In-memory reliability store tests.

use std::{sync::Arc, time::Duration};

use catga_core::memory::{MemoryDeadLetters, MemoryIdempotency, MemoryInbox};
use catga_core::{
    DeadLetter, DeadLetterStore, Envelope, IdempotencyStore, InboxStore, MessageMetadata,
    ProcessingState,
};

#[tokio::test]
async fn inbox_and_idempotency_claim_exclusively_cache_results_and_allow_retry_after_failure() {
    let inbox = MemoryInbox::default();
    let claim = inbox
        .try_claim(7)
        .await
        .unwrap()
        .expect("first inbox claim succeeds");
    assert!(inbox.try_claim(7).await.unwrap().is_none());
    inbox
        .complete(claim, Some(Arc::from([1_u8, 2, 3])))
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
    assert!(inbox.try_claim(7).await.unwrap().is_none());

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
        owners += usize::from(claim.unwrap().is_some());
    }
    assert_eq!(owners, 1);
}

#[tokio::test]
async fn expired_inbox_claim_can_be_reclaimed_without_a_background_worker() {
    let inbox = MemoryInbox::default();
    assert!(
        inbox
            .try_claim_for(91, Duration::from_millis(100))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        inbox
            .try_claim_for(91, Duration::from_secs(1))
            .await
            .unwrap()
            .is_none()
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        inbox
            .try_claim_for(91, Duration::from_secs(1))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn expired_inbox_claim_fences_a_stale_owner() {
    let inbox = MemoryInbox::default();
    let first = inbox
        .try_claim_for(93, Duration::from_millis(1))
        .await
        .unwrap()
        .expect("initial owner acquires the inbox claim");

    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = inbox
        .try_claim_for(93, Duration::from_secs(1))
        .await
        .unwrap()
        .expect("expired claim is reclaimed by a new owner");

    assert!(matches!(
        inbox.complete(first, Some(Arc::from([1_u8]))).await,
        Err(error) if error.code() == catga_core::ErrorCode::Conflict
    ));
    assert_eq!(
        inbox.state(93).await.unwrap(),
        Some(ProcessingState::Claimed)
    );
    assert!(matches!(
        inbox.fail(first).await,
        Err(error) if error.code() == catga_core::ErrorCode::Conflict
    ));
    inbox
        .complete(second, Some(Arc::from([2_u8])))
        .await
        .unwrap();
    assert_eq!(inbox.result(93).await.unwrap().as_deref(), Some(&[2][..]));
}

#[tokio::test]
async fn completed_reliability_records_are_removed_by_bounded_retention_cleanup() {
    let idempotency = MemoryIdempotency::with_retention(Duration::from_millis(1)).unwrap();
    assert!(idempotency.try_claim("retained-key").await.unwrap());
    idempotency.complete("retained-key", None).await.unwrap();

    let inbox = MemoryInbox::default();
    let claim = inbox
        .try_claim(92)
        .await
        .unwrap()
        .expect("inbox claim succeeds");
    inbox.complete(claim, None).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(idempotency.cleanup_completed(1).await.unwrap(), 1);
    assert_eq!(
        inbox
            .cleanup_completed(Duration::from_millis(1), 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(idempotency.state("retained-key").await.unwrap(), None);
    assert_eq!(inbox.state(92).await.unwrap(), None);
}
