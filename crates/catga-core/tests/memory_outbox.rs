//! Strict contract tests for the process-local delivery stores: the outbox
//! claim/ack lifecycle with token fencing, idempotency claim transitions with
//! retention cleanup, and the bounded dead-letter queue.

use std::sync::Arc;
use std::time::Duration;

use catga_core::memory::{MemoryDeadLetters, MemoryIdempotency, MemoryOutbox};
use catga_core::{
    DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope, ErrorCode, IdempotencyStore,
    MAX_OUTBOX_CLAIM_LIMIT, MAX_RETENTION_CLEANUP_LIMIT, MessageMetadata, OutboxMessage,
    OutboxState, OutboxStore, ProcessingState,
};

fn envelope(id: u64) -> Envelope {
    Envelope::new(id, "Tick", vec![id as u8], MessageMetadata::new(id, None))
}

/// Builds an envelope with a pinned send timestamp.
///
/// Claim ordering sorts by `(sent_at_unix_ms, id)`; pinning the timestamp keeps
/// ordering assertions deterministic across wall-clock millisecond boundaries.
fn envelope_sent(id: u64, sent_at_unix_ms: u64) -> Envelope {
    envelope(id).with_sent_at_unix_ms(Some(sent_at_unix_ms))
}

// ---------------------------------------------------------------------------
// MemoryOutbox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outbox_enqueue_validates_identity_and_capacity() {
    let outbox = MemoryOutbox::default();

    // Identifier zero is rejected before any record transition.
    let zero = Envelope::new(0, "Tick", vec![0], MessageMetadata::new(0, None));
    let error = outbox
        .enqueue(OutboxMessage::new(zero))
        .await
        .expect_err("a zero identifier must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    outbox
        .enqueue(OutboxMessage::new(envelope(7)))
        .await
        .expect("enqueue succeeds");
    let error = outbox
        .enqueue(OutboxMessage::new(envelope(7)))
        .await
        .expect_err("a duplicate identifier conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // A full outbox with no published headroom is routine backpressure.
    let outbox = MemoryOutbox::new(1).expect("capacity builds");
    outbox
        .enqueue(OutboxMessage::new(envelope(1)))
        .await
        .expect("enqueue succeeds");
    let error = outbox
        .enqueue(OutboxMessage::new(envelope(2)))
        .await
        .expect_err("a full outbox without cleanup headroom fails");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // Zero retention is rejected at construction.
    let error = MemoryOutbox::with_published_retention_and_capacity(Duration::ZERO, 4)
        .map(|_| ())
        .expect_err("zero retention must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn outbox_claim_orders_oldest_first_and_fences_completion() {
    let outbox = MemoryOutbox::default();
    for (id, sent_at) in [(3_u64, 300_u64), (1, 100), (2, 200)] {
        outbox
            .enqueue(OutboxMessage::new(envelope_sent(id, sent_at)))
            .await
            .expect("enqueue succeeds");
    }

    // A zero claim limit is valid and empty.
    assert!(
        outbox
            .claim("worker", 0)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
    let error = outbox
        .claim("worker", MAX_OUTBOX_CLAIM_LIMIT + 1)
        .await
        .expect_err("an over-budget claim limit must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // The oldest records claim first in identifier order.
    let claimed = outbox.claim("worker", 2).await.expect("claim succeeds");
    let ids: Vec<u64> = claimed.iter().map(OutboxMessage::id).collect();
    assert_eq!(ids, [1, 2]);
    assert_eq!(claimed[0].state(), OutboxState::Claimed);
    assert_eq!(claimed[0].owner(), Some("worker"));
    let token = claimed[0]
        .claim_token()
        .expect("a claim carries its fencing token")
        .to_string();

    // A claimed message is not handed out again.
    let remaining = outbox.claim("other", 10).await.expect("claim succeeds");
    let ids: Vec<u64> = remaining.iter().map(OutboxMessage::id).collect();
    assert_eq!(ids, [3]);

    // Stale owners and tokens cannot complete the delivery.
    outbox
        .ack("other", 1, &token)
        .await
        .expect("a foreign owner is ignored");
    outbox
        .ack("worker", 1, "stale-token")
        .await
        .expect("a stale token is ignored");
    outbox.ack("worker", 1, &token).await.expect("ack succeeds");

    let published = outbox.list_published(10).await.expect("list succeeds");
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id(), 1);
    assert_eq!(published[0].state(), OutboxState::Published);
    assert!(published[0].published_at_unix_ms().is_some());

    let error = outbox
        .list_published(MAX_OUTBOX_CLAIM_LIMIT + 1)
        .await
        .expect_err("an over-budget list limit must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn outbox_release_and_failure_return_messages_to_pending() {
    let outbox = MemoryOutbox::default();
    outbox
        .enqueue(OutboxMessage::new(envelope(1)))
        .await
        .expect("enqueue succeeds");
    outbox
        .enqueue(
            OutboxMessage::new(envelope(2))
                .with_max_retries(1)
                .expect("retry bound builds"),
        )
        .await
        .expect("enqueue succeeds");

    // A released message is claimable again with a fresh token.
    let claimed = outbox.claim("worker", 2).await.expect("claim succeeds");
    let token = claimed[0]
        .claim_token()
        .expect("claim token present")
        .to_string();
    let terminal_token = claimed
        .iter()
        .find(|message| message.id() == 2)
        .expect("message 2 claimed")
        .claim_token()
        .expect("claim token present")
        .to_string();
    outbox
        .release("worker", 1, "wrong-token")
        .await
        .expect("a stale token is ignored");
    outbox
        .release("worker", 1, &token)
        .await
        .expect("release succeeds");
    let reclaimed = outbox.claim("worker", 1).await.expect("claim succeeds");
    assert_eq!(reclaimed[0].id(), 1);
    assert_ne!(
        reclaimed[0].claim_token(),
        Some(token.as_str()),
        "a reclaim issues a fresh fencing token"
    );

    // A recorded failure re-pends below the retry bound with the reason kept.
    outbox
        .record_failure(
            "worker",
            1,
            reclaimed[0].claim_token().expect("token"),
            "boom",
        )
        .await
        .expect("failure succeeds");
    let reclaimed = outbox.claim("worker", 1).await.expect("claim succeeds");
    assert_eq!(reclaimed[0].retry_count(), 1);
    assert_eq!(reclaimed[0].last_error(), Some("boom"));
    outbox
        .release("worker", 1, reclaimed[0].claim_token().expect("token"))
        .await
        .expect("release succeeds");

    // A message at its retry bound fails terminally and is never claimed.
    outbox
        .record_failure("worker", 2, &terminal_token, "fatal")
        .await
        .expect("failure succeeds");
    let remaining = outbox.claim("worker", 10).await.expect("claim succeeds");
    assert!(
        remaining.iter().all(|message| message.id() != 2),
        "a terminally failed message leaves the claim set"
    );

    // Only a pending message cancels.
    let claimed = remaining
        .into_iter()
        .find(|message| message.id() == 1)
        .expect("message 1 claimed");
    let id = claimed.id();
    assert!(!outbox.cancel(id).await.expect("cancel succeeds"));
    outbox
        .release("worker", id, claimed.claim_token().expect("token"))
        .await
        .expect("release succeeds");
    assert!(outbox.cancel(id).await.expect("cancel succeeds"));
    assert!(!outbox.cancel(id).await.expect("cancel succeeds"));
}

#[tokio::test]
async fn outbox_claim_for_validates_leases_and_recovers_expired_claims() {
    let outbox = MemoryOutbox::default();
    outbox
        .enqueue(OutboxMessage::new(envelope(1)))
        .await
        .expect("enqueue succeeds");

    let error = outbox
        .claim_for("worker", 1, Duration::ZERO)
        .await
        .expect_err("a zero lease must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    let error = outbox
        .claim_for(
            "worker",
            1,
            catga_core::MAX_OUTBOX_CLAIM_LEASE + Duration::from_secs(1),
        )
        .await
        .expect_err("an over-budget lease must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A claim abandoned past its lease passes to the next worker.
    let claimed = outbox
        .claim_for("worker-a", 1, Duration::from_millis(1))
        .await
        .expect("claim succeeds");
    assert_eq!(claimed.len(), 1);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let reclaimed = outbox
        .claim_for("worker-b", 1, Duration::from_secs(30))
        .await
        .expect("claim succeeds");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].owner(), Some("worker-b"));

    // The stale owner can no longer acknowledge the delivery.
    outbox
        .ack(
            "worker-a",
            1,
            claimed[0].claim_token().expect("stale token"),
        )
        .await
        .expect("a fenced ack is ignored");
    assert!(
        outbox
            .list_published(10)
            .await
            .expect("list succeeds")
            .is_empty()
    );

    // A scheduled message is not claimable before its boundary.
    let scheduled = OutboxMessage::scheduled(
        envelope(9),
        std::time::SystemTime::now() + Duration::from_secs(3600),
    )
    .expect("scheduled message builds");
    outbox.enqueue(scheduled).await.expect("enqueue succeeds");
    let claimed = outbox.claim("worker-c", 10).await.expect("claim succeeds");
    assert!(
        claimed.iter().all(|message| message.id() != 9),
        "an undeliverable message stays pending"
    );
}

#[tokio::test]
async fn outbox_cleanup_removes_only_retention_expired_publications() {
    let outbox = MemoryOutbox::new(1).expect("capacity builds");
    outbox
        .enqueue(OutboxMessage::new(envelope(1)))
        .await
        .expect("enqueue succeeds");
    let claimed = outbox.claim("worker", 1).await.expect("claim succeeds");
    outbox
        .ack("worker", 1, claimed[0].claim_token().expect("token"))
        .await
        .expect("ack succeeds");

    // Records younger than the retention survive cleanup.
    let removed = outbox
        .cleanup_published(Duration::from_secs(60), 10)
        .await
        .expect("cleanup succeeds");
    assert_eq!(removed, 0);
    assert_eq!(
        outbox
            .list_published(10)
            .await
            .expect("list succeeds")
            .len(),
        1
    );

    let error = outbox
        .cleanup_published(Duration::ZERO, MAX_RETENTION_CLEANUP_LIMIT + 1)
        .await
        .expect_err("an over-budget cleanup limit must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A zero retention expires every published record and releases capacity.
    let removed = outbox
        .cleanup_published(Duration::ZERO, 10)
        .await
        .expect("cleanup succeeds");
    assert_eq!(removed, 1);
    assert!(
        outbox
            .list_published(10)
            .await
            .expect("list succeeds")
            .is_empty()
    );
    outbox
        .enqueue(OutboxMessage::new(envelope(2)))
        .await
        .expect("the released slot admits new records");
}

// ---------------------------------------------------------------------------
// MemoryIdempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn idempotency_claim_transitions_fence_duplicates() {
    let store = MemoryIdempotency::default();

    assert!(store.try_claim("k").await.expect("claim succeeds"));
    assert!(!store.try_claim("k").await.expect("a claimed key refuses"));
    assert_eq!(
        store.state("k").await.expect("state succeeds"),
        Some(ProcessingState::Claimed)
    );

    // Completion caches the handler result for duplicate detection.
    store
        .complete("k", Some(Arc::from([1_u8, 2])))
        .await
        .expect("complete succeeds");
    assert_eq!(
        store.state("k").await.expect("state succeeds"),
        Some(ProcessingState::Completed)
    );
    assert_eq!(
        store.result("k").await.expect("result succeeds").as_deref(),
        Some(&[1_u8, 2][..])
    );
    let error = store
        .complete("k", None)
        .await
        .expect_err("a completed key is not claimed");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(!store.try_claim("k").await.expect("a completed key refuses"));

    // Unknown keys report not-found for owner operations.
    let error = store
        .complete("missing", None)
        .await
        .expect_err("an unknown key is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    let error = store
        .fail("missing")
        .await
        .expect_err("an unknown key is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(store.state("missing").await.expect("state succeeds"), None);
    assert_eq!(
        store.result("missing").await.expect("result succeeds"),
        None
    );
}

#[tokio::test]
async fn idempotency_failure_releases_the_key_for_reclaim() {
    let store = MemoryIdempotency::default();
    assert!(store.try_claim("k").await.expect("claim succeeds"));
    store.fail("k").await.expect("fail succeeds");
    assert_eq!(
        store.state("k").await.expect("state succeeds"),
        Some(ProcessingState::Failed)
    );
    assert_eq!(store.result("k").await.expect("result succeeds"), None);

    // A failed key reclaims; a second failure without a claim conflicts.
    assert!(store.try_claim("k").await.expect("reclaim succeeds"));
    store.fail("k").await.expect("fail succeeds");
    let error = store
        .fail("k")
        .await
        .expect_err("a failed key is not claimed");
    assert_eq!(error.code(), ErrorCode::Conflict);
}

#[tokio::test]
async fn idempotency_cleanup_removes_only_retention_expired_completions() {
    let store = MemoryIdempotency::with_retention_and_capacity(Duration::from_millis(300), 2)
        .expect("store builds");

    assert!(store.try_claim("a").await.expect("claim succeeds"));
    store.complete("a", None).await.expect("complete succeeds");

    // Records younger than the retention survive cleanup.
    let removed = store.cleanup_completed(10).await.expect("cleanup succeeds");
    assert_eq!(removed, 0);
    assert_eq!(
        store.state("a").await.expect("state succeeds"),
        Some(ProcessingState::Completed)
    );

    let error = store
        .cleanup_completed(MAX_RETENTION_CLEANUP_LIMIT + 1)
        .await
        .expect_err("an over-budget cleanup limit must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Once expired, completion records are removed and capacity released.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let removed = store.cleanup_completed(10).await.expect("cleanup succeeds");
    assert_eq!(removed, 1);
    assert_eq!(store.state("a").await.expect("state succeeds"), None);
    assert!(store.try_claim("a").await.expect("reclaim succeeds"));

    // Zero retention is rejected at construction.
    let error = MemoryIdempotency::with_retention_and_capacity(Duration::ZERO, 2)
        .map(|_| ())
        .expect_err("zero retention must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A full store without cleanup headroom is routine backpressure.
    let store = MemoryIdempotency::new(1).expect("capacity builds");
    assert!(store.try_claim("x").await.expect("claim succeeds"));
    let error = store
        .try_claim("y")
        .await
        .expect_err("a full store without cleanup headroom fails");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

// ---------------------------------------------------------------------------
// MemoryDeadLetters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dead_letters_are_bounded_and_listed_in_order() {
    let error = MemoryDeadLetters::new(0)
        .map(|_| ())
        .expect_err("zero capacity must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    let letters = MemoryDeadLetters::new(2).expect("queue builds");
    letters
        .enqueue(DeadLetter::new(envelope(1), "first", 1))
        .await
        .expect("enqueue succeeds");
    letters
        .enqueue(DeadLetter::new(envelope(2), "second", 2))
        .await
        .expect("enqueue succeeds");
    let error = letters
        .enqueue(DeadLetter::new(envelope(3), "third", 3))
        .await
        .expect_err("a full queue rejects writes");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // Letters list in arrival order and honor the page limit.
    let listed = letters.list(10).await.expect("list succeeds");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].reason(), "first");
    assert_eq!(listed[0].attempts(), 1);
    assert_eq!(listed[0].envelope().id(), 1);
    assert_eq!(listed[0].diagnostics().stage(), "legacy");
    let listed = letters.list(1).await.expect("list succeeds");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].reason(), "first");
}

#[test]
fn dead_letter_constructors_bound_their_payloads() {
    let long_reason = "x".repeat(catga_core::MAX_DEAD_LETTER_DESCRIPTION_BYTES + 1);

    // The legacy constructor truncates; the strict constructor rejects.
    let letter = DeadLetter::new(envelope(1), long_reason.clone(), 1);
    assert_eq!(
        letter.reason().len(),
        catga_core::MAX_DEAD_LETTER_DESCRIPTION_BYTES
    );
    let diagnostics =
        DeadLetterDiagnostics::new(ErrorCode::Internal, "pipeline").expect("diagnostics build");
    let error = DeadLetter::try_with_diagnostics(envelope(1), long_reason, 1, diagnostics)
        .map(|_| ())
        .expect_err("an over-budget description must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Stage identifiers are bounded ASCII.
    assert!(DeadLetterDiagnostics::new(ErrorCode::Internal, "").is_err());
    assert!(DeadLetterDiagnostics::new(ErrorCode::Internal, "bad stage").is_err());

    // Failure capture derives the category from the framework error.
    let failure = catga_core::CatgaError::new(ErrorCode::Timeout, "slow");
    let letter = DeadLetter::from_failure(envelope(2), &failure, 3, "transport")
        .expect("failure letter builds");
    assert_eq!(letter.diagnostics().error_code(), ErrorCode::Timeout);
    assert_eq!(letter.diagnostics().stage(), "transport");
    assert_eq!(letter.attempts(), 3);
}
