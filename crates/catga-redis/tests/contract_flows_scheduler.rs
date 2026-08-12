//! Service-backed contract coverage for [`RedisFlows`] and [`RedisFlowScheduler`].

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::flow::{DueFlowScheduler, FlowScheduler, FlowState, FlowStatus, FlowStore};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_redis::{RedisFlowScheduler, RedisFlows};
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

// ==================== flows ====================

#[tokio::test]
async fn flows_create_get_update_and_delete_paths() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisFlows::connect(&url, unique_prefix("flows")).await?;

    assert_eq!(store.get("flow-1").await?, None);

    let state = FlowState::new("flow-1", "payment", &b"input"[..], "node-a");
    assert!(store.create(state).await?);
    // A duplicate create loses the exists-check race.
    assert!(
        !store
            .create(FlowState::new("flow-1", "payment", &b"input"[..], "node-a"))
            .await?
    );

    let current = store
        .get("flow-1")
        .await?
        .expect("a created flow must load");
    assert_eq!(current.version(), 0);
    assert_eq!(current.status(), FlowStatus::Running);

    // Exact-version updates succeed; stale ones lose the CAS race.
    assert!(store.update(0, current.clone().next_version()?).await?);
    assert!(!store.update(0, current.clone().next_version()?).await?);

    // A version jump is rejected before any Redis round trip.
    let jumped = current.clone().next_version()?.next_version()?;
    assert!(!store.update(0, jumped).await?);

    Ok(())
}

#[tokio::test]
async fn flows_track_every_status_code() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisFlows::connect(&url, unique_prefix("flows")).await?;

    let base = |id: &str| FlowState::new(id, "payment", &b""[..], "node-a");
    store.create(base("flow-running")).await?;
    store
        .create(base("flow-compensating").compensating())
        .await?;
    store.create(base("flow-suspended").suspended()).await?;
    store.create(base("flow-done").done(0)).await?;
    store
        .create(base("flow-failed").failed(CatgaError::new(ErrorCode::Internal, "test failure")))
        .await?;
    store.create(base("flow-cancelled").cancelled()).await?;

    // Fresh heartbeats fall outside a wide staleness window, so nothing is claimable.
    assert!(
        store
            .try_claim("payment", "node-b", Duration::from_secs(86_400))
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn flows_heartbeat_requires_the_current_owner_and_version() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisFlows::connect(&url, unique_prefix("flows")).await?;

    store
        .create(FlowState::new("flow-1", "payment", &b""[..], "node-a"))
        .await?;

    assert!(store.heartbeat("flow-1", "node-a", 0).await?);
    assert!(!store.heartbeat("flow-1", "node-b", 0).await?);
    assert!(!store.heartbeat("flow-1", "node-a", 7).await?);
    assert!(!store.heartbeat("flow-missing", "node-a", 0).await?);

    // Terminal flows drop their owner, so a heartbeat never applies to them.
    store
        .create(FlowState::new("flow-done", "payment", &b""[..], "node-a").done(0))
        .await?;
    assert!(!store.heartbeat("flow-done", "node-a", 0).await?);
    assert!(
        store
            .try_claim("payment", "node-b", Duration::from_secs(86_400))
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn flows_reject_out_of_range_heartbeat_timestamps() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisFlows::connect(&url, unique_prefix("flows")).await?;

    // A heartbeat before the Unix epoch cannot be a Redis score.
    let before_epoch = FlowState::new("flow-early", "payment", &b""[..], "node-a")
        .heartbeated_at(UNIX_EPOCH - Duration::from_secs(1));
    let result = store.create(before_epoch).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

/// Mirrors the crate's hashed key framing for flow records and type indexes.
fn hashed_flow_key(prefix: &str, kind: &str, value: &str) -> String {
    use catga_core::hash::sha256_concat_digest;
    let value_len = value.len().to_be_bytes();
    format!(
        "{prefix}:{kind}:{}",
        hex::encode(sha256_concat_digest(&[&value_len, value.as_bytes()]))
    )
}

#[tokio::test]
async fn flows_claim_skips_stale_index_candidates() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("flows");
    let store = RedisFlows::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A dangling index member without its record hash is skipped.
    let payment_index = hashed_flow_key(&prefix, "flow-type", "payment");
    let dangling = format!("{prefix}:flow:dangling");
    let _: usize = raw
        .zadd(&payment_index, &dangling, 0)
        .await
        .map_err(map_redis_error)?;

    // A stale running flow whose record sits in a foreign type index is skipped there.
    store
        .create(
            FlowState::new("flow-typed", "payment", &b""[..], "node-a")
                .heartbeated_at(UNIX_EPOCH + Duration::from_secs(1)),
        )
        .await?;
    let typed_key = hashed_flow_key(&prefix, "flow", "flow-typed");
    let other_index = hashed_flow_key(&prefix, "flow-type", "other");
    let _: usize = raw
        .zadd(&other_index, &typed_key, 0)
        .await
        .map_err(map_redis_error)?;

    // The foreign index yields no claim for its apparent type.
    assert!(
        store
            .try_claim("other", "node-b", Duration::from_secs(86_400))
            .await?
            .is_none()
    );

    // The genuine stale flow is still claimable through its real index, past the
    // dangling member.
    let claimed = store
        .try_claim("payment", "node-b", Duration::from_secs(86_400))
        .await?;
    assert_eq!(
        claimed
            .expect("the stale running flow must be claimable")
            .id(),
        "flow-typed"
    );
    Ok(())
}

#[tokio::test]
async fn flows_get_rejects_a_corrupt_record() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("flows");
    let store = RedisFlows::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    store
        .create(FlowState::new("flow-1", "payment", &b""[..], "node-a"))
        .await?;
    let keys: Vec<String> = raw
        .keys(format!("{prefix}:flow:*"))
        .await
        .map_err(map_redis_error)?;
    let key = keys.first().expect("the flow record must exist").clone();
    let _: usize = raw
        .hset(key, "value", b"not-a-frame".as_slice())
        .await
        .map_err(map_redis_error)?;

    assert!(store.get("flow-1").await.is_err());
    Ok(())
}

#[tokio::test]
async fn flows_claim_window_saturates_at_the_epoch() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisFlows::connect(&url, unique_prefix("flows")).await?;
    store
        .create(FlowState::new("flow-1", "payment", &b""[..], "node-a"))
        .await?;

    // An enormous staleness window clamps to the epoch; fresh flows stay unclaimed.
    assert!(
        store
            .try_claim("payment", "node-b", Duration::from_secs(u64::MAX))
            .await?
            .is_none()
    );
    Ok(())
}

// ==================== scheduler ====================

#[tokio::test]
async fn scheduler_cancel_only_applies_to_pending_work() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let scheduler = RedisFlowScheduler::connect(&url, unique_prefix("scheduler")).await?;

    // Cancelling unknown work is a no-op.
    assert!(!scheduler.cancel_resume("catga-missing-schedule").await?);

    let due = SystemTime::now() + Duration::from_secs(3_600);
    let pending = scheduler.schedule_resume("flow-a", "state-a", due).await?;
    assert!(scheduler.cancel_resume(&pending).await?);
    assert!(!scheduler.cancel_resume(&pending).await?);

    // Claimed work inside its lease cannot be cancelled.
    let claimable = scheduler
        .schedule_resume("flow-b", "state-b", SystemTime::now())
        .await?;
    let claimed = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(!scheduler.cancel_resume(&claimable).await?);
    Ok(())
}

#[tokio::test]
async fn scheduler_release_returns_work_to_the_due_queue() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let scheduler = RedisFlowScheduler::connect(&url, unique_prefix("scheduler")).await?;

    let schedule_id = scheduler
        .schedule_resume("flow-c", "state-c", SystemTime::now())
        .await?;
    let claimed = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), schedule_id.as_ref());

    // Foreign owners can neither release nor acknowledge the claim.
    assert!(!scheduler.release_due("worker-b", &schedule_id).await?);
    assert!(!scheduler.ack_due("worker-b", &schedule_id).await?);

    assert!(scheduler.release_due("worker-a", &schedule_id).await?);
    // Releasing twice reports the lost ownership.
    assert!(!scheduler.release_due("worker-a", &schedule_id).await?);

    let reclaimed = scheduler
        .claim_due("worker-b", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(reclaimed.len(), 1);
    assert!(scheduler.ack_due("worker-b", &schedule_id).await?);
    // Acknowledged work is gone for good.
    assert!(!scheduler.ack_due("worker-b", &schedule_id).await?);
    Ok(())
}

#[tokio::test]
async fn scheduler_renews_a_live_lease() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let scheduler = RedisFlowScheduler::connect(&url, unique_prefix("scheduler")).await?;

    let schedule_id = scheduler
        .schedule_resume("flow-d", "state-d", SystemTime::now())
        .await?;
    let claimed = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    assert!(
        scheduler
            .renew_due(
                "worker-a",
                &schedule_id,
                SystemTime::now(),
                Duration::from_secs(60)
            )
            .await?
    );
    assert!(
        !scheduler
            .renew_due(
                "worker-b",
                &schedule_id,
                SystemTime::now(),
                Duration::from_secs(60)
            )
            .await?
    );
    assert!(
        !scheduler
            .renew_due(
                "worker-a",
                "catga-missing",
                SystemTime::now(),
                Duration::from_secs(60)
            )
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn scheduler_reclaims_work_with_an_expired_lease() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let scheduler = RedisFlowScheduler::connect(&url, unique_prefix("scheduler")).await?;

    scheduler
        .schedule_resume("flow-e", "state-e", SystemTime::now())
        .await?;
    let claimed = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_millis(40), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    // The live lease blocks other workers.
    assert!(
        scheduler
            .claim_due("worker-b", SystemTime::now(), Duration::from_secs(1), 1)
            .await?
            .is_empty()
    );

    // Once the lease expires, another worker recovers the entry.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered = scheduler
        .claim_due("worker-b", SystemTime::now(), Duration::from_secs(30), 1)
        .await?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].flow_id(), "flow-e");
    Ok(())
}

#[tokio::test]
async fn scheduler_validates_its_time_arguments() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let scheduler = RedisFlowScheduler::connect(&url, unique_prefix("scheduler")).await?;

    // A zero-length lease is rejected on both claim and renew.
    let zero_claim = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::ZERO, 1)
        .await;
    assert!(matches!(zero_claim, Err(error) if error.code() == ErrorCode::Validation));
    let zero_renew = scheduler
        .renew_due("worker-a", "catga-any", SystemTime::now(), Duration::ZERO)
        .await;
    assert!(matches!(zero_renew, Err(error) if error.code() == ErrorCode::Validation));

    // A zero limit is an empty page, not an error.
    assert!(
        scheduler
            .claim_due("worker-a", SystemTime::now(), Duration::from_secs(1), 0)
            .await?
            .is_empty()
    );

    // Due times must not precede the Unix epoch.
    let before_epoch = scheduler
        .schedule_resume("flow-x", "state-x", UNIX_EPOCH - Duration::from_secs(1))
        .await;
    assert!(matches!(before_epoch, Err(error) if error.code() == ErrorCode::Validation));

    // Leases beyond the Redis millisecond range are rejected on both claim and renew.
    let huge_lease = scheduler
        .claim_due(
            "worker-a",
            SystemTime::now(),
            Duration::from_secs(u64::MAX),
            1,
        )
        .await;
    assert!(matches!(huge_lease, Err(error) if error.code() == ErrorCode::Validation));

    // A representable lease whose deadline overflows Unix time is rejected too.
    let overflowing = scheduler
        .claim_due(
            "worker-a",
            SystemTime::now(),
            Duration::from_millis(i64::MAX as u64),
            1,
        )
        .await;
    assert!(matches!(overflowing, Err(error) if error.code() == ErrorCode::Validation));
    let overflowing_renew = scheduler
        .renew_due(
            "worker-a",
            "catga-any",
            SystemTime::now(),
            Duration::from_millis(i64::MAX as u64),
        )
        .await;
    assert!(matches!(overflowing_renew, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn scheduler_claim_rejects_a_corrupt_due_time() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("scheduler");
    let scheduler = RedisFlowScheduler::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A record with a non-numeric due time breaks receipt decoding.
    let record = format!("{prefix}:schedule:corrupt-1");
    let fields: &[(&str, &str)] = &[
        ("flow_id", "flow-corrupt"),
        ("state_id", "state-corrupt"),
        ("due_at", "not-a-number"),
        ("target", "target-corrupt"),
        ("owner", ""),
        ("lease_until", "0"),
    ];
    let _: () = raw
        .hset_multiple(&record, fields)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw
        .zadd(format!("{prefix}:due"), "corrupt-1", 0)
        .await
        .map_err(map_redis_error)?;

    let result = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 5)
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}

#[tokio::test]
async fn scheduler_claim_rejects_an_out_of_range_due_time() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("scheduler");
    let scheduler = RedisFlowScheduler::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A numeric due time beyond the SystemTime ceiling parses but cannot decode.
    let record = format!("{prefix}:schedule:huge-1");
    let fields: &[(&str, &str)] = &[
        ("flow_id", "flow-huge"),
        ("state_id", "state-huge"),
        ("due_at", "18446744073709551615"),
        ("target", "target-huge"),
        ("owner", ""),
        ("lease_until", "0"),
    ];
    let _: () = raw
        .hset_multiple(&record, fields)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw
        .zadd(format!("{prefix}:due"), "huge-1", 0)
        .await
        .map_err(map_redis_error)?;

    let result = scheduler
        .claim_due("worker-a", SystemTime::now(), Duration::from_secs(30), 5)
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}
