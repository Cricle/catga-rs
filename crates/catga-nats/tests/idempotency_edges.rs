//! Idempotency and inbox edge contracts: retention policies, claim state-machine fences,
//! and corrupt broker records.
//!
//! Both stores share one KV claim state machine; these tests pin the transitions a
//! crash-recovering deployment depends on: stale generations fence completions, delete
//! markers and expired leases are reclaimable, and malformed records are internal errors.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_bucket_policy.rs"]
mod raw_bucket_policy;
#[path = "support/raw_kv.rs"]
mod raw_kv;

use std::sync::Arc;
use std::time::Duration;

use catga_core::{
    CatgaResult, ErrorCode, IdempotencyStore, InboxClaim, InboxStore, ProcessingState,
};
use catga_nats::{NatsIdempotency, NatsInbox};
use names::unique;
use nats_server::server_url;
use raw_bucket_policy::raw_kv_with_max_age;
use raw_kv::raw_kv;

const CLAIMED: u8 = 1;

/// Twin of the store-internal KV key encoding: `k` followed by lowercase hex key bytes.
fn kv_key(key: &str) -> String {
    let mut encoded = String::with_capacity(key.len() * 2 + 1);
    encoded.push('k');
    for byte in key.as_bytes() {
        encoded.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex digit"));
        encoded.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("hex digit"));
    }
    encoded
}

fn inbox_key(message_id: u64) -> String {
    kv_key(&message_id.to_string())
}

#[tokio::test]
async fn a_zero_completed_retention_is_a_validation_error() {
    // Validation runs before any network I/O, so this test needs no server.
    assert!(matches!(
        NatsIdempotency::with_retention("nats://127.0.0.1:1", "unused", Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn connecting_resets_a_buckets_max_age_policy() -> CatgaResult<()> {
    let bucket = unique("CATGA_IDEM_POLICY");
    // A pre-existing bucket whose records expire must be neutralized: claimed and failed
    // records may never expire before their state transition.
    raw_kv_with_max_age(&bucket, Duration::from_secs(1)).await?;
    let store = NatsIdempotency::connect(&server_url(), bucket.as_str()).await?;
    assert!(store.try_claim("policy-key").await?);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    // The record survives beyond the original max-age, so the claim is still fenced.
    assert!(!store.try_claim("policy-key").await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transitions_on_unclaimed_or_settled_keys_are_fenced() -> CatgaResult<()> {
    let store = NatsIdempotency::connect(&server_url(), unique("CATGA_IDEM_FENCE")).await?;

    // Completions and failures require an active claim.
    assert!(matches!(
        store.complete("never-claimed", None).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));
    assert!(matches!(
        store.fail("never-claimed").await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    // A settled key rejects further transitions and stays fenced against reclaims.
    assert!(store.try_claim("settled").await?);
    store.complete("settled", None).await?;
    assert!(matches!(
        store.complete("settled", None).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(matches!(
        store.fail("settled").await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(!store.try_claim("settled").await?);

    // A failed key is claimable again, and the new claim owns transitions.
    assert!(store.try_claim("reclaimable").await?);
    store.fail("reclaimable").await?;
    assert!(store.try_claim("reclaimable").await?);
    store
        .complete("reclaimable", Some(Arc::from(&b"done"[..])))
        .await?;
    assert_eq!(
        store.result("reclaimable").await?.as_deref(),
        Some(&b"done"[..])
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_delete_marked_key_is_claimable_again() -> CatgaResult<()> {
    let bucket = unique("CATGA_IDEM_RECLAIM");
    let store = NatsIdempotency::connect(&server_url(), bucket.as_str()).await?;
    assert!(store.try_claim("deleted-key").await?);

    let raw = raw_kv(&bucket).await?;
    raw.delete(kv_key("deleted-key"))
        .await
        .map_err(|error| catga_core::CatgaError::new(ErrorCode::Internal, error.to_string()))?;

    // The delete marker is not a live record: the next claim recreates the entry.
    assert!(store.try_claim("deleted-key").await?);
    assert_eq!(
        store.state("deleted-key").await?,
        Some(ProcessingState::Claimed)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn malformed_records_are_internal_errors_and_expiryless_claims_are_reclaimable()
-> CatgaResult<()> {
    let bucket = unique("CATGA_IDEM_CORRUPT");
    let store = NatsIdempotency::connect(&server_url(), bucket.as_str()).await?;
    let raw = raw_kv(&bucket).await?;
    let inject = |key: &str, value: &[u8]| {
        let raw = raw.clone();
        let key = kv_key(key);
        let value = value.to_vec();
        async move {
            raw.put(key, value.into()).await.map_err(|error| {
                catga_core::CatgaError::new(ErrorCode::Internal, error.to_string())
            })?;
            Ok::<(), catga_core::CatgaError>(())
        }
    };

    // An unknown state byte cannot be decoded into a processing state.
    inject("garbage", &[0xEE]).await?;
    assert!(matches!(
        store.state("garbage").await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    assert!(matches!(
        store.try_claim("garbage").await,
        Err(error) if error.code() == ErrorCode::Internal
    ));

    // A claimed record without its expiry payload is treated as already expired: the
    // inbox claim path reclaims it instead of fencing forever.
    let inbox = NatsInbox::connect(&server_url(), bucket.as_str()).await?;
    inject("61", &[CLAIMED]).await?;
    let reclaimed = inbox
        .try_claim_for(61, Duration::from_secs(30))
        .await?
        .expect("an expiryless claim must be reclaimable");
    inbox.complete(reclaimed, None).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn cleanup_removes_only_records_older_than_retention() -> CatgaResult<()> {
    let expiring = unique("CATGA_IDEM_SWEEP");
    let store =
        NatsIdempotency::with_retention(&server_url(), expiring.as_str(), Duration::from_millis(1))
            .await?;
    assert!(store.try_claim("swept").await?);
    store.complete("swept", None).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(store.cleanup_completed(8).await?, 1);
    // The removal left a delete marker; a second sweep skips it without error.
    assert_eq!(store.cleanup_completed(8).await?, 0);

    // Records younger than the retention stay put.
    let keeping = unique("CATGA_IDEM_KEEP");
    let store =
        NatsIdempotency::with_retention(&server_url(), keeping.as_str(), Duration::from_secs(3600))
            .await?;
    assert!(store.try_claim("kept").await?);
    store.complete("kept", None).await?;
    assert_eq!(store.cleanup_completed(8).await?, 0);
    assert_eq!(store.state("kept").await?, Some(ProcessingState::Completed));

    // The cleanup limit is bounded at the trait level.
    assert!(matches!(
        store.cleanup_completed(usize::MAX).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_claims_reclaim_after_expiry_and_delete_markers() -> CatgaResult<()> {
    let bucket = unique("CATGA_INBOX_LEASE");
    let inbox = NatsInbox::connect(&server_url(), bucket.as_str()).await?;

    // A live claim fences competing claims.
    let first = inbox
        .try_claim_for(7, Duration::from_millis(40))
        .await?
        .expect("first claim must succeed");
    assert!(
        inbox
            .try_claim_for(7, Duration::from_secs(30))
            .await?
            .is_none()
    );

    // Once the lease lapses, a new claim with a fresh generation takes over.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let second = inbox
        .try_claim_for(7, Duration::from_secs(30))
        .await?
        .expect("expired claim must be reclaimable");
    assert_ne!(first.generation(), second.generation());

    // A delete marker is reclaimable too.
    let raw = raw_kv(&bucket).await?;
    raw.delete(inbox_key(9))
        .await
        .map_err(|error| catga_core::CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let claimed = inbox
        .try_claim_for(9, Duration::from_secs(30))
        .await?
        .expect("delete-marked message must be claimable");
    inbox.complete(claimed, None).await?;
    assert_eq!(inbox.state(9).await?, Some(ProcessingState::Completed));
    assert_eq!(inbox.result(9).await?, None);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_completions_fence_stale_or_fabricated_claims() -> CatgaResult<()> {
    let inbox = NatsInbox::connect(&server_url(), unique("CATGA_INBOX_FENCE")).await?;

    // A claim naming a message the store never issued is a miss, not a write.
    let fabricated = InboxClaim::new(41, 7).expect("non-zero generation is a valid claim shape");
    assert!(matches!(
        inbox.complete(fabricated, None).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));
    assert!(matches!(
        inbox.fail(fabricated).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    // A fabricated generation on a live claim does not own it.
    let live = inbox
        .try_claim_for(43, Duration::from_secs(30))
        .await?
        .expect("claim must succeed");
    let impostor = InboxClaim::new(43, live.generation() + 100).expect("valid claim shape");
    assert!(matches!(
        inbox.complete(impostor, None).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));

    // Completing consumes the claim; the stale claim cannot fail it afterwards.
    inbox.complete(live, None).await?;
    assert!(matches!(
        inbox.fail(live).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(matches!(
        inbox.complete(live, None).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_cleanup_honors_its_own_retention_argument() -> CatgaResult<()> {
    let inbox = NatsInbox::connect(&server_url(), unique("CATGA_INBOX_SWEEP")).await?;
    let claim = inbox
        .try_claim_for(51, Duration::from_secs(30))
        .await?
        .expect("claim must succeed");
    inbox.complete(claim, None).await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(
        inbox.cleanup_completed(Duration::from_millis(1), 8).await?,
        1
    );
    assert_eq!(inbox.state(51).await?, None);

    assert!(matches!(
        inbox.try_claim_for(52, Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        inbox.cleanup_completed(Duration::from_secs(1), usize::MAX).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}
