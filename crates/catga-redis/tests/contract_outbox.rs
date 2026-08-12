//! Service-backed contract coverage for [`RedisOutbox`].

use std::time::Duration;

use catga_core::{CatgaResult, ErrorCode, OutboxMessage, OutboxState, OutboxStore};
use catga_redis::RedisOutbox;
use redis::AsyncCommands;

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/ids.rs"]
mod ids;
#[path = "support/raw.rs"]
mod raw;
#[path = "support/redis_err.rs"]
mod redis_err;
#[path = "support/service_url.rs"]
mod service_url;

use envelopes::envelope;
use ids::unique_prefix;
use raw::raw_connection;
use redis_err::map_redis_error;

fn message(id: u64) -> CatgaResult<OutboxMessage> {
    Ok(OutboxMessage::new(envelope(id, "catga.test.outbox")))
}

#[tokio::test]
async fn enqueue_claim_ack_and_publish_history() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;

    outbox.enqueue(message(1)?).await?;
    outbox.enqueue(message(2)?).await?;

    // A duplicate identifier conflicts instead of overwriting.
    let duplicate = outbox.enqueue(message(1)?).await;
    assert!(matches!(duplicate, Err(error) if error.code() == ErrorCode::Conflict));

    // Identifier zero is rejected before any Redis write.
    let zero = outbox.enqueue(message(0)?).await;
    assert!(matches!(zero, Err(error) if error.code() == ErrorCode::Validation));

    // A zero claim budget is an empty page, not an error.
    assert!(outbox.claim("worker-a", 0).await?.is_empty());

    let claimed = outbox.claim("worker-a", 10).await?;
    assert_eq!(claimed.len(), 2);
    let first = &claimed[0];
    assert_eq!(first.state(), OutboxState::Claimed);
    assert_eq!(first.owner(), Some("worker-a"));
    let token = first.claim_token().expect("a claim must carry its token");

    // A live claim blocks re-claiming by another worker.
    assert!(outbox.claim("worker-b", 10).await?.is_empty());

    // A wrong owner or token leaves the message claimed and unpublished.
    outbox.ack("worker-b", first.id(), token).await?;
    outbox
        .ack("worker-a", first.id(), "catga:forged-token")
        .await?;
    assert!(outbox.list_published(10).await?.is_empty());

    outbox.ack("worker-a", first.id(), token).await?;
    let published = outbox.list_published(10).await?;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id(), first.id());
    assert_eq!(published[0].state(), OutboxState::Published);
    assert!(published[0].published_at_unix_ms().is_some());

    // Claiming with an over-budget limit is a validation error.
    let over_budget = outbox.claim("worker-a", usize::MAX).await;
    assert!(matches!(over_budget, Err(error) if error.code() == ErrorCode::Validation));
    let over_budget = outbox.list_published(usize::MAX).await;
    assert!(matches!(over_budget, Err(error) if error.code() == ErrorCode::Validation));

    Ok(())
}

#[tokio::test]
async fn release_returns_a_message_to_the_pending_pool() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;
    outbox.enqueue(message(7)?).await?;

    let claimed = outbox.claim("worker-a", 1).await?;
    assert_eq!(claimed.len(), 1);
    let token = claimed[0]
        .claim_token()
        .expect("a claim must carry its token")
        .to_string();

    // A forged release does nothing; the message stays claimed.
    outbox.release("worker-b", 7, &token).await?;
    assert!(outbox.claim("worker-b", 1).await?.is_empty());

    outbox.release("worker-a", 7, &token).await?;
    let reclaimed = outbox.claim("worker-b", 1).await?;
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id(), 7);
    Ok(())
}

#[tokio::test]
async fn record_failure_retries_until_the_terminal_limit() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("outbox");
    let outbox = RedisOutbox::connect(&url, prefix.clone()).await?;

    let message = message(9)?.with_max_retries(2)?;
    outbox.enqueue(message).await?;

    let claimed = outbox.claim("worker-a", 1).await?;
    let token = claimed[0]
        .claim_token()
        .expect("a claim must carry its token")
        .to_string();

    // A forged failure record leaves the retry count untouched.
    outbox
        .record_failure("worker-b", 9, "catga:forged-token", "forged")
        .await?;

    outbox
        .record_failure("worker-a", 9, &token, "first attempt failed")
        .await?;

    let retried = outbox.claim("worker-b", 1).await?;
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].retry_count(), 1);
    assert_eq!(retried[0].last_error(), Some("first attempt failed"));
    let second_token = retried[0]
        .claim_token()
        .expect("a claim must carry its token")
        .to_string();

    outbox
        .record_failure("worker-b", 9, &second_token, "second attempt failed")
        .await?;

    // The retry budget is exhausted: the message is terminal and unclaimable.
    assert!(outbox.claim("worker-c", 1).await?.is_empty());
    let mut raw = raw_connection(&url).await?;
    let state: String = raw
        .hget(format!("{prefix}:9"), "state")
        .await
        .map_err(map_redis_error)?;
    assert_eq!(state, "failed");

    Ok(())
}

#[tokio::test]
async fn cancel_only_removes_pending_messages() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;
    outbox.enqueue(message(21)?).await?;
    outbox.enqueue(message(22)?).await?;

    // A missing message cannot be cancelled.
    assert!(!outbox.cancel(4_000_000).await?);

    let claimed = outbox.claim("worker-a", 1).await?;
    assert_eq!(claimed.len(), 1);
    let claimed_id = claimed[0].id();

    // Claimed messages are protected from cancellation.
    assert!(!outbox.cancel(claimed_id).await?);

    let pending_id = if claimed_id == 21 { 22 } else { 21 };
    assert!(outbox.cancel(pending_id).await?);
    assert!(!outbox.cancel(pending_id).await?);
    Ok(())
}

#[tokio::test]
async fn scheduled_messages_are_not_claimable_before_their_time() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;

    let scheduled = OutboxMessage::scheduled(
        envelope(31, "catga.test.scheduled"),
        std::time::SystemTime::now() + Duration::from_secs(3_600),
    )?;
    outbox.enqueue(scheduled).await?;

    assert!(outbox.claim("worker-a", 10).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn claim_for_applies_a_custom_lease() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;
    outbox.enqueue(message(41)?).await?;

    // A zero lease is rejected by the shared claim-lease validation.
    let zero_lease = outbox.claim_for("worker-a", 1, Duration::ZERO).await;
    assert!(matches!(zero_lease, Err(error) if error.code() == ErrorCode::Validation));

    // A short lease expires and lets another worker recover the message.
    let claimed = outbox
        .claim_for("worker-a", 1, Duration::from_millis(30))
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(claimed[0].claimed_until_unix_ms().is_some());

    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered = outbox.claim("worker-b", 1).await?;
    assert_eq!(recovered.len(), 1);
    Ok(())
}

#[tokio::test]
async fn cleanup_published_removes_only_expired_history() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let outbox = RedisOutbox::connect(&url, unique_prefix("outbox")).await?;
    outbox.enqueue(message(51)?).await?;
    let claimed = outbox.claim("worker-a", 1).await?;
    let token = claimed[0]
        .claim_token()
        .expect("a claim must carry its token")
        .to_string();
    outbox.ack("worker-a", 51, &token).await?;

    // A zero cleanup budget removes nothing successfully.
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 0).await?, 0);
    let over_budget = outbox.cleanup_published(Duration::ZERO, usize::MAX).await;
    assert!(matches!(over_budget, Err(error) if error.code() == ErrorCode::Validation));
    // A retention beyond the u64-millisecond range cannot be a cleanup cutoff.
    let huge_retention = outbox
        .cleanup_published(Duration::from_secs(u64::MAX), 10)
        .await;
    assert!(matches!(huge_retention, Err(error) if error.code() == ErrorCode::Validation));

    // A long retention keeps the published record.
    assert_eq!(
        outbox
            .cleanup_published(Duration::from_secs(3_600), 10)
            .await?,
        0
    );
    assert_eq!(outbox.list_published(10).await?.len(), 1);

    // A zero retention expires everything published so far.
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 10).await?, 1);
    assert!(outbox.list_published(10).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn list_published_skips_records_that_lost_their_state() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("outbox");
    let outbox = RedisOutbox::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A dangling identifier in the published index without a record is skipped.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| catga_core::CatgaError::new(ErrorCode::Internal, "clock before epoch"))?;
    let _: usize = raw
        .zadd(
            format!("{prefix}:published"),
            9_999_999_u64,
            now.as_millis() as u64,
        )
        .await
        .map_err(map_redis_error)?;

    assert!(outbox.list_published(10).await?.is_empty());
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 10).await?, 0);
    Ok(())
}

#[tokio::test]
async fn list_published_validates_record_completeness() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("outbox");
    let outbox = RedisOutbox::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A published record without its payload is skipped during listing.
    let fields: &[(&str, &str)] = &[("state", "published"), ("published_at", "1")];
    let _: () = raw
        .hset_multiple(format!("{prefix}:71"), fields)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw
        .zadd(format!("{prefix}:published"), 71_u64, 1)
        .await
        .map_err(map_redis_error)?;
    assert!(outbox.list_published(10).await?.is_empty());

    // A published record without its publication timestamp is corrupt.
    let corrupt: &[(&str, &[u8])] = &[("state", b"published"), ("payload", b"x")];
    let _: () = raw
        .hset_multiple(format!("{prefix}:72"), corrupt)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw
        .zadd(format!("{prefix}:published"), 72_u64, 2)
        .await
        .map_err(map_redis_error)?;
    let result = outbox.list_published(10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}

#[tokio::test]
async fn claim_skips_payloadless_records_and_rejects_malformed_identifiers() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("outbox");
    let outbox = RedisOutbox::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A zero claim budget on the published history is an empty page.
    assert!(outbox.list_published(0).await?.is_empty());

    // A numeric member without its record hash is claimed server-side, then
    // skipped client-side because the payload field never materialized.
    let pending = format!("{prefix}:pending");
    let _: usize = raw
        .zadd(&pending, 81_u64, 1)
        .await
        .map_err(map_redis_error)?;
    // A non-numeric member breaks identifier decoding after the script claims it.
    let _: usize = raw
        .zadd(&pending, "not-an-id", 2)
        .await
        .map_err(map_redis_error)?;

    let result = outbox.claim("worker-a", 10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}
