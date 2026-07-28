//! In-memory outbox concurrency tests.

use std::{
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use catga_core::{
    CatgaResult, DEFAULT_OUTBOX_MAX_RETRIES, Envelope, ErrorCode, MAX_OUTBOX_CLAIM_LEASE,
    MAX_OUTBOX_CLAIM_LIMIT, MAX_OUTBOX_FAILURE_ERROR_BYTES, MessageMetadata, OutboxMessage,
    OutboxState, OutboxStore,
};
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
    let claimed = first.first().or(second.first()).unwrap();
    let owner = claimed.owner().unwrap().to_owned();
    let claim_token = claimed.claim_token().unwrap().to_owned();
    store.ack(&owner, 1, &claim_token).await.unwrap();
    assert!(store.claim("third", 1).await.unwrap().is_empty());
}

#[tokio::test]
async fn outbox_message_bounds_failure_details_and_uses_the_default_retry_limit() -> CatgaResult<()>
{
    let mut outbox = message(2);
    assert_eq!(outbox.max_retries(), DEFAULT_OUTBOX_MAX_RETRIES);
    assert_eq!(outbox.retry_count(), 0);
    assert_eq!(outbox.last_error(), None);
    assert!(matches!(
        outbox.clone().with_max_retries(0),
        Err(error) if error.code() == ErrorCode::Validation
    ));

    outbox.claim("worker-a");
    let reason = "failure-".repeat(MAX_OUTBOX_FAILURE_ERROR_BYTES);
    outbox.record_failure(&reason);

    assert_eq!(outbox.state(), OutboxState::Pending);
    assert_eq!(outbox.owner(), None);
    assert_eq!(outbox.retry_count(), 1);
    let Some(error) = outbox.last_error() else {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Internal,
            "outbox failure reason was not retained",
        ));
    };
    assert!(error.is_char_boundary(error.len()));
    assert!(error.len() <= MAX_OUTBOX_FAILURE_ERROR_BYTES);
    Ok(())
}

#[tokio::test]
async fn memory_outbox_stops_claiming_after_the_configured_failure_limit() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    store.enqueue(message(3).with_max_retries(2)?).await?;

    let first = store.claim("worker-a", 1).await?;
    assert_eq!(first[0].retry_count(), 0);
    store
        .record_failure("worker-a", 3, first[0].claim_token().unwrap(), "offline")
        .await?;

    let second = store.claim("worker-b", 1).await?;
    assert_eq!(second[0].retry_count(), 1);
    store
        .record_failure(
            "worker-b",
            3,
            second[0].claim_token().unwrap(),
            "still offline",
        )
        .await?;

    assert!(store.claim("worker-c", 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn memory_outbox_rejects_claims_above_the_explicit_memory_budget() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    store.enqueue(message(4)).await?;

    assert!(matches!(
        store.claim("worker-a", MAX_OUTBOX_CLAIM_LIMIT + 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(
        store.claim("worker-a", MAX_OUTBOX_CLAIM_LIMIT).await?.len(),
        1
    );
    assert!(store.claim("worker-b", 0).await?.is_empty());
    assert!(matches!(
        store.claim_for("worker-b", 1, Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        store
            .claim_for("worker-b", 0, Duration::from_nanos(1))
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        store
            .claim_for(
                "worker-b",
                1,
                MAX_OUTBOX_CLAIM_LEASE + Duration::from_millis(1)
            )
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
async fn stale_same_owner_claim_token_cannot_complete_a_reclaimed_message() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    store.enqueue(message(42)).await?;

    let original = store
        .claim_for("worker-a", 1, Duration::from_millis(20))
        .await?
        .pop()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let reclaimed = store
        .claim_for("worker-a", 1, Duration::from_millis(200))
        .await?
        .pop()
        .unwrap();

    assert_ne!(original.claim_token(), reclaimed.claim_token());
    store
        .release("worker-a", original.id(), original.claim_token().unwrap())
        .await?;
    assert!(store.claim("worker-a", 1).await?.is_empty());
    store
        .record_failure(
            "worker-a",
            original.id(),
            original.claim_token().unwrap(),
            "stale failure",
        )
        .await?;
    assert!(store.claim("worker-a", 1).await?.is_empty());
    store
        .ack("worker-a", original.id(), original.claim_token().unwrap())
        .await?;
    assert!(store.list_published(1).await?.is_empty());
    store
        .ack("worker-a", reclaimed.id(), reclaimed.claim_token().unwrap())
        .await?;
    assert_eq!(store.list_published(1).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn expired_outbox_claim_is_reclaimed_by_a_new_owner() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    store.enqueue(message(41)).await?;

    let original = store
        .claim_for("worker-a", 1, Duration::from_secs(1))
        .await?
        .pop()
        .unwrap();
    assert!(
        store
            .claim_for("worker-b", 1, Duration::from_secs(1))
            .await?
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let recovered = store
        .claim_for("worker-b", 1, Duration::from_secs(1))
        .await?
        .pop()
        .unwrap();
    assert_eq!(recovered.owner(), Some("worker-b"));

    // The expired owner's completion must not fence the recovered claim.
    store
        .ack("worker-a", 41, original.claim_token().unwrap())
        .await?;
    assert!(store.list_published(1).await?.is_empty());
    store
        .ack("worker-b", 41, recovered.claim_token().unwrap())
        .await?;
    assert!(store.claim("worker-c", 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn rejected_duplicate_enqueue_preserves_the_original_outbox_message() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    store.enqueue(message(5)).await?;
    let duplicate = OutboxMessage::new(Envelope::new(
        5,
        "order.changed",
        vec![9],
        MessageMetadata::new(5, None),
    ));

    assert!(matches!(
        store.enqueue(duplicate).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    let claimed = store.claim("worker-a", 1).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].envelope().message_type(), "order.created");
    assert_eq!(claimed[0].envelope().payload(), [1]);
    Ok(())
}

#[tokio::test]
async fn memory_outbox_rejects_the_reserved_zero_message_identifier() -> CatgaResult<()> {
    let store = MemoryOutbox::default();

    assert!(matches!(
        store.enqueue(message(0)).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(store.claim("worker-a", 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn memory_outbox_claims_the_earliest_created_messages_first() -> CatgaResult<()> {
    let store = MemoryOutbox::default();
    for id in 1_u64..=8 {
        let envelope = Envelope::new(
            id,
            "order.created",
            vec![id as u8],
            MessageMetadata::new(id, None),
        )
        .with_sent_at(UNIX_EPOCH + Duration::from_millis((9 - id) * 1_000))?;
        store.enqueue(OutboxMessage::new(envelope)).await?;
    }

    let claimed = store.claim("worker-a", 4).await?;
    assert_eq!(
        claimed.iter().map(OutboxMessage::id).collect::<Vec<_>>(),
        [8, 7, 6, 5]
    );
    Ok(())
}

#[tokio::test]
async fn acknowledged_outbox_messages_remain_inspectable_until_bounded_cleanup() -> CatgaResult<()>
{
    let store = MemoryOutbox::default();
    store.enqueue(message(88)).await?;
    let claimed = store.claim("worker", 1).await?.pop().unwrap();
    store
        .ack("worker", 88, claimed.claim_token().unwrap())
        .await?;

    assert_eq!(store.list_published(1).await?.len(), 1);
    assert_eq!(store.list_published(1).await?[0].id(), 88);
    assert_eq!(store.cleanup_published(Duration::ZERO, 1).await?, 1);
    assert!(store.list_published(1).await?.is_empty());
    Ok(())
}
