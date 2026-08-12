//! Service-backed contract coverage for [`RedisInbox`] and [`RedisIdempotency`].

use std::sync::Arc;
use std::time::Duration;

use catga_core::{
    CatgaResult, ErrorCode, IdempotencyStore, InboxClaim, InboxStore, ProcessingState,
};
use catga_redis::{RedisIdempotency, RedisInbox};
use redis::AsyncCommands;

#[path = "support/ids.rs"]
mod ids;
#[path = "support/raw.rs"]
mod raw;
#[path = "support/redis_err.rs"]
mod redis_err;
#[path = "support/service_url.rs"]
mod service_url;

use ids::unique_prefix;
use raw::raw_connection;
use redis_err::map_redis_error;

// ==================== inbox ====================

#[tokio::test]
async fn inbox_claim_complete_and_result_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let inbox = RedisInbox::connect(&url, unique_prefix("inbox")).await?;

    // Unknown messages have no retained state or result.
    assert_eq!(inbox.state(101).await?, None);
    assert_eq!(inbox.result(101).await?, None);

    let claim = inbox
        .try_claim(101)
        .await?
        .expect("the first claim must win");
    assert_eq!(claim.message_id(), 101);
    assert_eq!(inbox.state(101).await?, Some(ProcessingState::Claimed));

    // A live claim blocks competing claims.
    assert!(inbox.try_claim(101).await?.is_none());

    // A result-bearing completion retains the bytes and closes the message.
    inbox.complete(claim, Some(Arc::from(&b"done"[..]))).await?;
    assert_eq!(inbox.state(101).await?, Some(ProcessingState::Completed));
    assert_eq!(inbox.result(101).await?, Some(Arc::from(&b"done"[..])));

    // A completed message cannot be claimed or transitioned again.
    assert!(inbox.try_claim(101).await?.is_none());
    let duplicate = inbox.complete(claim, None).await;
    assert!(matches!(duplicate, Err(error) if error.code() == ErrorCode::Conflict));

    Ok(())
}

#[tokio::test]
async fn inbox_completion_without_a_result_reads_back_as_none() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let inbox = RedisInbox::connect(&url, unique_prefix("inbox")).await?;

    let claim = inbox.try_claim(102).await?.expect("the claim must win");
    inbox.complete(claim, None).await?;
    assert_eq!(inbox.state(102).await?, Some(ProcessingState::Completed));
    assert_eq!(inbox.result(102).await?, None);
    Ok(())
}

#[tokio::test]
async fn inbox_failure_releases_the_message_for_a_new_claim() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let inbox = RedisInbox::connect(&url, unique_prefix("inbox")).await?;

    let first = inbox.try_claim(103).await?.expect("the claim must win");
    inbox.fail(first).await?;
    assert_eq!(inbox.state(103).await?, Some(ProcessingState::Failed));

    // A failed message can be claimed again.
    let reclaimed = inbox
        .try_claim(103)
        .await?
        .expect("a failed message must be reclaimable");
    assert_eq!(reclaimed.message_id(), 103);

    // A forged generation is fenced.
    let forged = InboxClaim::new(103, reclaimed.generation() + 100)
        .expect("a nonzero generation builds a claim");
    let forged_complete = inbox.complete(forged, None).await;
    assert!(matches!(forged_complete, Err(error) if error.code() == ErrorCode::Conflict));
    let forged_fail = inbox.fail(forged).await;
    assert!(matches!(forged_fail, Err(error) if error.code() == ErrorCode::Conflict));

    // A never-claimed message reports NotFound instead of Conflict.
    let missing = InboxClaim::new(4_000_000, 1).expect("a nonzero generation builds a claim");
    let missing_complete = inbox.complete(missing, None).await;
    assert!(matches!(missing_complete, Err(error) if error.code() == ErrorCode::NotFound));
    let missing_fail = inbox.fail(missing).await;
    assert!(matches!(missing_fail, Err(error) if error.code() == ErrorCode::NotFound));

    inbox.complete(reclaimed, None).await?;
    assert_eq!(inbox.state(103).await?, Some(ProcessingState::Completed));
    Ok(())
}

#[tokio::test]
async fn inbox_expired_claims_can_be_reclaimed() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let inbox = RedisInbox::connect(&url, unique_prefix("inbox")).await?;

    // A zero-length lease is rejected by the shared lease validation.
    let zero_lease = inbox.try_claim_for(104, Duration::ZERO).await;
    assert!(matches!(zero_lease, Err(error) if error.code() == ErrorCode::Validation));

    let first = inbox
        .try_claim_for(104, Duration::from_millis(30))
        .await?
        .expect("the claim must win");
    assert!(inbox.try_claim(104).await?.is_none());

    tokio::time::sleep(Duration::from_millis(120)).await;
    let second = inbox
        .try_claim(104)
        .await?
        .expect("an expired claim must be reclaimable");
    assert!(second.generation() > first.generation());

    // The expired generation is fenced out of every transition.
    let stale_complete = inbox.complete(first, None).await;
    assert!(matches!(stale_complete, Err(error) if error.code() == ErrorCode::Conflict));
    let stale_fail = inbox.fail(first).await;
    assert!(matches!(stale_fail, Err(error) if error.code() == ErrorCode::Conflict));

    inbox.complete(second, None).await?;
    Ok(())
}

#[tokio::test]
async fn inbox_cleanup_removes_completed_records_only() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let inbox = RedisInbox::connect(&url, unique_prefix("inbox")).await?;

    let completed = inbox.try_claim(201).await?.expect("the claim must win");
    inbox
        .complete(completed, Some(Arc::from(&b"kept"[..])))
        .await?;
    let claimed = inbox.try_claim(202).await?.expect("the claim must win");
    assert_eq!(claimed.message_id(), 202);

    // Claimed records are invisible to cleanup and stay claimed.
    assert_eq!(inbox.cleanup_completed(Duration::ZERO, 10).await?, 1);
    assert_eq!(inbox.state(201).await?, None);
    assert_eq!(inbox.state(202).await?, Some(ProcessingState::Claimed));

    assert_eq!(inbox.cleanup_completed(Duration::ZERO, 0).await?, 0);
    let over_budget = inbox.cleanup_completed(Duration::ZERO, usize::MAX).await;
    assert!(matches!(over_budget, Err(error) if error.code() == ErrorCode::Validation));
    // A retention beyond the u64-millisecond range cannot be a cleanup cutoff.
    let huge_retention = inbox
        .cleanup_completed(Duration::from_secs(u64::MAX), 10)
        .await;
    assert!(matches!(huge_retention, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn inbox_state_rejects_a_malformed_record() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("inbox");
    let inbox = RedisInbox::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    let _: () = raw
        .set(format!("{prefix}:303"), vec![0xFF_u8])
        .await
        .map_err(map_redis_error)?;
    let result = inbox.state(303).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    // A malformed record never exposes a cached result.
    assert_eq!(inbox.result(303).await?, None);
    Ok(())
}

#[tokio::test]
async fn inbox_claim_rejects_an_unrepresentable_generation() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("inbox");
    let inbox = RedisInbox::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A claimed record whose expired lease carries the largest representable generation:
    // reclaiming advances it past the Redis integer reply range, so the claim cannot
    // fence a valid owner and must fail instead of returning a bogus generation.
    let mut record = vec![1_u8];
    record.extend_from_slice(b"0:9223372036854775807:");
    let _: () = raw
        .set(format!("{prefix}:404"), record)
        .await
        .map_err(map_redis_error)?;
    let result = inbox.try_claim(404).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}

// ==================== idempotency ====================

#[tokio::test]
async fn idempotency_claim_complete_and_result_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisIdempotency::connect(&url, unique_prefix("idempotency")).await?;

    assert_eq!(store.state("order-1").await?, None);
    assert_eq!(store.result("order-1").await?, None);

    assert!(store.try_claim("order-1").await?);
    assert!(!store.try_claim("order-1").await?);
    assert_eq!(
        store.state("order-1").await?,
        Some(ProcessingState::Claimed)
    );
    // A claimed key has no cached result yet.
    assert_eq!(store.result("order-1").await?, None);

    store
        .complete("order-1", Some(Arc::from(&b"receipt"[..])))
        .await?;
    assert_eq!(
        store.state("order-1").await?,
        Some(ProcessingState::Completed)
    );
    assert_eq!(
        store.result("order-1").await?,
        Some(Arc::from(&b"receipt"[..]))
    );

    // Completed keys reject both reclaims and repeated transitions.
    assert!(!store.try_claim("order-1").await?);
    let duplicate = store.complete("order-1", None).await;
    assert!(matches!(duplicate, Err(error) if error.code() == ErrorCode::Conflict));
    let fail_after_complete = store.fail("order-1").await;
    assert!(matches!(fail_after_complete, Err(error) if error.code() == ErrorCode::Conflict));

    // Never-claimed keys report NotFound on transitions.
    let missing_complete = store.complete("order-missing", None).await;
    assert!(matches!(missing_complete, Err(error) if error.code() == ErrorCode::NotFound));
    let missing_fail = store.fail("order-missing").await;
    assert!(matches!(missing_fail, Err(error) if error.code() == ErrorCode::NotFound));

    Ok(())
}

#[tokio::test]
async fn idempotency_failure_allows_a_fresh_claim() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisIdempotency::connect(&url, unique_prefix("idempotency")).await?;

    assert!(store.try_claim("payment-9").await?);
    store.fail("payment-9").await?;
    assert_eq!(
        store.state("payment-9").await?,
        Some(ProcessingState::Failed)
    );

    assert!(store.try_claim("payment-9").await?);
    store.complete("payment-9", None).await?;
    assert_eq!(
        store.state("payment-9").await?,
        Some(ProcessingState::Completed)
    );
    assert_eq!(store.result("payment-9").await?, None);
    Ok(())
}

#[tokio::test]
async fn idempotency_rejects_an_oversized_result() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisIdempotency::connect(&url, unique_prefix("idempotency")).await?;

    assert!(store.try_claim("large-result").await?);
    let oversized: Arc<[u8]> = Arc::from(vec![0xAB_u8; 1024 * 1024 + 1].into_boxed_slice());
    let result = store.complete("large-result", Some(oversized)).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    // The failed completion leaves the claim live for a legal retry.
    assert_eq!(
        store.state("large-result").await?,
        Some(ProcessingState::Claimed)
    );
    Ok(())
}

#[tokio::test]
async fn idempotency_completed_records_carry_the_configured_ttl() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("idempotency");
    let store =
        RedisIdempotency::with_retention(&url, prefix.clone(), Duration::from_secs(60)).await?;

    assert!(store.try_claim("ttl-key").await?);
    let hashed = {
        use sha2::{Digest, Sha256};
        format!("{prefix}:{}", hex::encode(Sha256::digest(b"ttl-key")))
    };
    let mut raw = raw_connection(&url).await?;

    // Claimed records persist without an expiration.
    let claimed_ttl: i64 = raw.pttl(&hashed).await.map_err(map_redis_error)?;
    assert_eq!(claimed_ttl, -1);

    store.complete("ttl-key", None).await?;
    let completed_ttl: i64 = raw.pttl(&hashed).await.map_err(map_redis_error)?;
    assert!(completed_ttl > 0 && completed_ttl <= 60_000);

    Ok(())
}

#[tokio::test]
async fn idempotency_cleanup_is_a_bounded_no_op() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisIdempotency::connect(&url, unique_prefix("idempotency")).await?;

    // Redis expires completed records through key TTLs, so cleanup only validates.
    assert_eq!(store.cleanup_completed(10).await?, 0);
    let over_budget = store.cleanup_completed(usize::MAX).await;
    assert!(matches!(over_budget, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn idempotency_state_rejects_a_malformed_record() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("idempotency");
    let store = RedisIdempotency::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    let hashed = {
        use sha2::{Digest, Sha256};
        format!("{prefix}:{}", hex::encode(Sha256::digest(b"broken")))
    };
    let _: () = raw
        .set(hashed, vec![0xEE_u8])
        .await
        .map_err(map_redis_error)?;

    let result = store.state("broken").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    assert_eq!(store.result("broken").await?, None);
    Ok(())
}

#[tokio::test]
async fn idempotency_retention_is_validated_before_connecting() -> CatgaResult<()> {
    // No Redis URL is needed: retention validation precedes the connection attempt.
    let zero = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1/",
        "catga-test-idempotency",
        Duration::ZERO,
    )
    .await;
    assert!(matches!(zero, Err(error) if error.code() == ErrorCode::Validation));
    // A retention beyond the i64-millisecond range cannot be a Redis PX argument.
    let huge = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1/",
        "catga-test-idempotency",
        Duration::from_secs(u64::MAX / 1000 + 1),
    )
    .await;
    assert!(matches!(huge, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}
