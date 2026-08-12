//! Service-backed contract coverage for dead letters, projections, leases,
//! subscriptions, and DSL step progress.

use std::time::Duration;

use catga_core::flow::{DslStepProgress, DslStepProgressStore};
use catga_core::{
    CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, ErrorCode, LeaseStore,
    PersistentSubscription, ProjectionCheckpoint, ProjectionCheckpointStore,
    SubscriptionCheckpoint, SubscriptionStore,
};
use catga_redis::{
    RedisDeadLetters, RedisDslStepProgress, RedisLeases, RedisProjectionCheckpoints,
    RedisSubscriptions,
};
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

// ==================== dead letters ====================

#[tokio::test]
async fn dead_letters_preserve_fifo_order_and_diagnostics() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisDeadLetters::connect(&url, unique_prefix("dead-letter")).await?;

    assert!(store.list(10).await?.is_empty());
    assert!(store.list(0).await?.is_empty());

    store
        .enqueue(DeadLetter::new(envelope(1, "catga.test.first"), "boom", 1))
        .await?;
    let diagnostics = DeadLetterDiagnostics::new(ErrorCode::Validation, "parsing")?;
    store
        .enqueue(DeadLetter::try_with_diagnostics(
            envelope(2, "catga.test.second"),
            "bad frame",
            3,
            diagnostics,
        )?)
        .await?;

    let letters = store.list(10).await?;
    assert_eq!(letters.len(), 2);
    assert_eq!(letters[0].envelope().message_type(), "catga.test.first");
    assert_eq!(letters[0].reason(), "boom");
    assert_eq!(letters[0].attempts(), 1);
    assert_eq!(letters[1].envelope().message_type(), "catga.test.second");
    assert_eq!(letters[1].diagnostics().error_code(), ErrorCode::Validation);
    assert_eq!(letters[1].diagnostics().stage(), "parsing");
    assert!(letters[1].diagnostics().failed_at_unix_ms() > 0);

    // The limit window truncates from the head of the queue.
    let first_only = store.list(1).await?;
    assert_eq!(first_only.len(), 1);
    assert_eq!(first_only[0].envelope().message_type(), "catga.test.first");
    Ok(())
}

#[tokio::test]
async fn dead_letters_skip_dangling_queue_entries() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("dead-letter");
    let store = RedisDeadLetters::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A queued identifier without its detail hash is skipped during listing.
    let _: usize = raw
        .rpush(format!("{prefix}:queue"), 4_242_u64)
        .await
        .map_err(map_redis_error)?;
    store
        .enqueue(DeadLetter::new(envelope(3, "catga.test.real"), "kept", 2))
        .await?;

    let letters = store.list(10).await?;
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].envelope().message_type(), "catga.test.real");
    Ok(())
}

// ==================== projection checkpoints ====================

#[tokio::test]
async fn projection_checkpoints_partition_by_projection_name() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisProjectionCheckpoints::connect(&url, unique_prefix("projection")).await?;

    assert_eq!(store.load("orders", "stream-1").await?, None);

    store
        .save(ProjectionCheckpoint::new("orders", "stream-1", 10))
        .await?;
    store
        .save(ProjectionCheckpoint::new("orders", "stream-2", 20))
        .await?;
    store
        .save(ProjectionCheckpoint::new("payments", "stream-1", 99))
        .await?;

    let checkpoint = store
        .load("orders", "stream-1")
        .await?
        .expect("a saved checkpoint must load");
    assert_eq!(checkpoint.projection_name(), "orders");
    assert_eq!(checkpoint.stream_id(), "stream-1");
    assert_eq!(checkpoint.version(), 10);
    assert_eq!(
        store
            .load("payments", "stream-1")
            .await?
            .expect("a saved checkpoint must load")
            .version(),
        99
    );

    store.delete("orders", "stream-1").await?;
    assert_eq!(store.load("orders", "stream-1").await?, None);
    assert!(store.load("orders", "stream-2").await?.is_some());

    store.delete_all("orders").await?;
    assert_eq!(store.load("orders", "stream-2").await?, None);
    assert!(store.load("payments", "stream-1").await?.is_some());
    Ok(())
}

#[tokio::test]
async fn projection_checkpoints_reject_malformed_values() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("projection");
    let store = RedisProjectionCheckpoints::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    let key = format!("{prefix}:orders");

    // No tab separator.
    let _: usize = raw
        .hset(&key, "broken-a", "garbage")
        .await
        .map_err(map_redis_error)?;
    let result = store.load("orders", "broken-a").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // A non-numeric version.
    let _: usize = raw
        .hset(&key, "broken-b", "abc\t123")
        .await
        .map_err(map_redis_error)?;
    let result = store.load("orders", "broken-b").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // A non-numeric timestamp.
    let _: usize = raw
        .hset(&key, "broken-c", "123\tabc")
        .await
        .map_err(map_redis_error)?;
    let result = store.load("orders", "broken-c").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    Ok(())
}

// ==================== leases ====================

#[tokio::test]
async fn leases_acquire_renew_and_release_atomically() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let leases = RedisLeases::connect(&url, unique_prefix("lease")).await?;

    let ttl = Duration::from_secs(30);
    assert!(leases.try_acquire("resource-1", "owner-a", ttl).await?);
    // The owner re-acquiring extends the lease.
    assert!(leases.try_acquire("resource-1", "owner-a", ttl).await?);
    // A competing owner is fenced out.
    assert!(!leases.try_acquire("resource-1", "owner-b", ttl).await?);

    assert!(leases.renew("resource-1", "owner-a", ttl).await?);
    assert!(!leases.renew("resource-1", "owner-b", ttl).await?);

    assert!(!leases.release("resource-1", "owner-b").await?);
    assert!(leases.release("resource-1", "owner-a").await?);
    assert!(!leases.release("resource-1", "owner-a").await?);

    assert!(leases.try_acquire("resource-1", "owner-b", ttl).await?);
    Ok(())
}

#[tokio::test]
async fn leases_expire_and_allow_takeover() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let leases = RedisLeases::connect(&url, unique_prefix("lease")).await?;

    // Sub-millisecond TTLs round up to one Redis millisecond.
    assert!(
        leases
            .try_acquire("resource-2", "owner-a", Duration::from_nanos(1))
            .await?
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        leases
            .try_acquire("resource-2", "owner-b", Duration::from_millis(40))
            .await?
    );

    // Renewing an expired lease owned by someone else fails.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !leases
            .renew("resource-2", "owner-b", Duration::from_secs(1))
            .await?
    );
    Ok(())
}

// ==================== subscriptions ====================

#[tokio::test]
async fn subscriptions_store_definitions_checkpoints_and_leases() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSubscriptions::connect(&url, unique_prefix("subscription")).await?;

    assert_eq!(store.load("sub-b").await?, None);
    assert!(store.list().await?.is_empty());

    store
        .save(PersistentSubscription::new("sub-b", "orders-*"))
        .await?;
    store
        .save(
            PersistentSubscription::new("sub-a", "payments-*")
                .with_event_types(["catga.created", "catga.paid"]),
        )
        .await?;

    let loaded = store
        .load("sub-a")
        .await?
        .expect("a saved subscription must load");
    assert_eq!(loaded.name(), "sub-a");
    assert_eq!(loaded.stream_pattern(), "payments-*");
    assert_eq!(
        loaded
            .event_types()
            .iter()
            .map(|event_type| event_type.as_ref())
            .collect::<Vec<&str>>(),
        vec!["catga.created", "catga.paid"]
    );

    // Listing is sorted by name and round-trips every definition.
    let names: Vec<String> = store
        .list()
        .await?
        .iter()
        .map(|sub| sub.name().to_string())
        .collect();
    assert_eq!(names, vec!["sub-a".to_string(), "sub-b".to_string()]);

    // Checkpoints are versioned per stream.
    store
        .save_checkpoint(SubscriptionCheckpoint::new("sub-a", "payments-1", 12))
        .await?;
    let checkpoint = store
        .load_checkpoint("sub-a", "payments-1")
        .await?
        .expect("a saved checkpoint must load");
    assert_eq!(checkpoint.version(), 12);
    assert_eq!(store.load_checkpoint("sub-a", "payments-2").await?, None);

    // Owner leases are exclusive until released.
    assert!(store.try_acquire("sub-a", "owner-a").await?);
    assert!(!store.try_acquire("sub-a", "owner-a").await?);
    assert!(!store.try_acquire("sub-a", "owner-b").await?);
    store.release("sub-a", "owner-b").await?;
    assert!(!store.try_acquire("sub-a", "owner-b").await?);
    store.release("sub-a", "owner-a").await?;
    assert!(store.try_acquire("sub-a", "owner-b").await?);

    // Deleting a definition removes its checkpoints and lease too.
    store.delete("sub-a").await?;
    assert_eq!(store.load("sub-a").await?, None);
    assert_eq!(store.load_checkpoint("sub-a", "payments-1").await?, None);
    assert!(store.try_acquire("sub-a", "owner-c").await?);
    assert_eq!(
        store
            .list()
            .await?
            .iter()
            .map(|sub| sub.name().to_string())
            .collect::<Vec<_>>(),
        vec!["sub-b".to_string()]
    );
    Ok(())
}

// ==================== DSL step progress ====================

#[tokio::test]
async fn dsl_step_progress_cas_its_versions() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisDslStepProgress::connect(&url, unique_prefix("dsl-progress")).await?;

    assert_eq!(store.get("flow-1", 0).await?, None);

    let initial = DslStepProgress::new("flow-1", 0, &b"state-0"[..]);
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial).await?);

    let loaded = store
        .get("flow-1", 0)
        .await?
        .expect("created progress must load");
    assert_eq!(loaded.flow_id(), "flow-1");
    assert_eq!(loaded.step_index(), 0);
    assert_eq!(loaded.version(), 0);
    assert_eq!(loaded.payload(), &b"state-0"[..]);

    let next = loaded.next_version(&b"state-1"[..])?;
    assert!(store.update(0, next).await?);
    assert_eq!(
        store
            .get("flow-1", 0)
            .await?
            .expect("created progress must load")
            .payload(),
        &b"state-1"[..]
    );

    // A stale expected version loses the compare-and-set race.
    let replayed = store
        .get("flow-1", 0)
        .await?
        .expect("created progress must load")
        .next_version(&b"state-2"[..])?;
    assert!(!store.update(0, replayed).await?);

    // A version jump is rejected before any Redis round trip.
    let current = store
        .get("flow-1", 0)
        .await?
        .expect("created progress must load");
    let skipped = current.version() + 1;
    let jumped = current.next_version(&b"state-3"[..])?;
    assert!(!store.update(skipped, jumped).await?);

    // Steps are keyed independently.
    assert!(
        store
            .create(DslStepProgress::new("flow-1", 1, &b"other"[..]))
            .await?
    );
    assert!(store.delete("flow-1", 1).await?);
    assert!(!store.delete("flow-1", 1).await?);
    assert_eq!(store.get("flow-1", 1).await?, None);
    assert!(store.get("flow-1", 0).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn dead_letters_reject_malformed_detail_records() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("dead-letter");
    let store = RedisDeadLetters::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;
    let queue = format!("{prefix}:queue");
    // Valid frames keep the diagnostics checks reachable; a raw payload fails
    // decoding first with a validation error instead.
    let frame = catga_core::EnvelopeCodec::encode(
        &catga_core::MemoryPackCodec::default(),
        &envelope(7, "catga.test.dead"),
    )?;

    // Partially present diagnostics are internally inconsistent.
    let partial: &[(&str, &[u8])] = &[
        ("payload", &frame),
        ("reason", b"partial"),
        ("attempts", b"1"),
        ("failed_at_unix_ms", b"7"),
    ];
    let _: () = raw
        .hset_multiple(format!("{prefix}:details:1"), partial)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw.rpush(&queue, 1_u64).await.map_err(map_redis_error)?;
    let result = store.list(10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // An unknown stable error code cannot be decoded.
    let _: usize = raw.del(&queue).await.map_err(map_redis_error)?;
    let unknown: &[(&str, &[u8])] = &[
        ("payload", &frame),
        ("reason", b"unknown"),
        ("attempts", b"1"),
        ("failed_at_unix_ms", b"7"),
        ("error_code", b"catga.bogus"),
        ("stage", b"decoding"),
    ];
    let _: () = raw
        .hset_multiple(format!("{prefix}:details:2"), unknown)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw.rpush(&queue, 2_u64).await.map_err(map_redis_error)?;
    let result = store.list(10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // A payload that is not a valid envelope frame fails decoding.
    let _: usize = raw.del(&queue).await.map_err(map_redis_error)?;
    let broken_frame: &[(&str, &[u8])] = &[
        ("payload", b"not-a-frame"),
        ("reason", b"broken"),
        ("attempts", b"1"),
    ];
    let _: () = raw
        .hset_multiple(format!("{prefix}:details:3"), broken_frame)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw.rpush(&queue, 3_u64).await.map_err(map_redis_error)?;
    let result = store.list(10).await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn dead_letters_reject_invalid_diagnostics_and_descriptions() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("dead-letter");
    let store = RedisDeadLetters::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;
    let queue = format!("{prefix}:queue");
    let frame = catga_core::EnvelopeCodec::encode(
        &catga_core::MemoryPackCodec::default(),
        &envelope(8, "catga.test.dead"),
    )?;

    // A blank stage cannot build diagnostics even with a known error code.
    let blank_stage: &[(&str, &[u8])] = &[
        ("payload", &frame),
        ("reason", b"blank stage"),
        ("attempts", b"1"),
        ("failed_at_unix_ms", b"7"),
        ("error_code", b"validation"),
        ("stage", b""),
    ];
    let _: () = raw
        .hset_multiple(format!("{prefix}:details:1"), blank_stage)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw.rpush(&queue, 1_u64).await.map_err(map_redis_error)?;
    let result = store.list(10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // A reason beyond the description budget is rejected as a record.
    let oversized_reason = vec![b'x'; 1_025];
    let oversized: &[(&str, &[u8])] = &[
        ("payload", &frame),
        ("reason", &oversized_reason),
        ("attempts", b"1"),
        ("failed_at_unix_ms", b"7"),
        ("error_code", b"validation"),
        ("stage", b"decoding"),
    ];
    let _: usize = raw.del(&queue).await.map_err(map_redis_error)?;
    let _: () = raw
        .hset_multiple(format!("{prefix}:details:2"), oversized)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw.rpush(&queue, 2_u64).await.map_err(map_redis_error)?;
    let result = store.list(10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}
