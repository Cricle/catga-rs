//! Service-backed contract coverage for Redis snapshot stores and state machines.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use catga_core::flow::{StateMachineSnapshot, StateMachineStore};
use catga_core::{CatgaResult, EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotStore};
use catga_redis::{RedisEnhancedSnapshots, RedisSnapshotStore, RedisStateMachines};
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

// ==================== latest snapshots ====================

#[tokio::test]
async fn snapshot_save_load_and_delete_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSnapshotStore::<String>::connect(&url, unique_prefix("snapshot")).await?;

    assert!(store.load::<String>("agg-1").await?.is_none());

    store
        .save(Snapshot::new("agg-1", "state-one".to_string(), 1))
        .await?;
    let loaded = store
        .load::<String>("agg-1")
        .await?
        .expect("a saved snapshot must load");
    assert_eq!(loaded.stream_id(), "agg-1");
    assert_eq!(loaded.state(), "state-one");
    assert_eq!(loaded.version(), 1);

    // An equal or newer version replaces; an older one conflicts.
    store
        .save(Snapshot::new("agg-1", "state-one-b".to_string(), 1))
        .await?;
    store
        .save(Snapshot::new("agg-1", "state-two".to_string(), 2))
        .await?;
    let conflict = store
        .save(Snapshot::new("agg-1", "state-old".to_string(), 1))
        .await;
    assert!(matches!(conflict, Err(error) if error.code() == ErrorCode::Conflict));
    assert_eq!(
        store
            .load::<String>("agg-1")
            .await?
            .expect("a saved snapshot must load")
            .state(),
        "state-two"
    );

    store.delete("agg-1").await?;
    assert!(store.load::<String>("agg-1").await?.is_none());
    // Deleting a missing snapshot is a no-op.
    store.delete("agg-1").await?;
    Ok(())
}

#[tokio::test]
async fn snapshot_store_enforces_its_state_type() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSnapshotStore::<String>::connect(&url, unique_prefix("snapshot")).await?;

    let wrong_save = store.save(Snapshot::new("agg-1", 42_u64, 1)).await;
    assert!(matches!(wrong_save, Err(error) if error.code() == ErrorCode::Validation));

    store
        .save(Snapshot::new("agg-1", "state".to_string(), 1))
        .await?;
    let wrong_load = store.load::<u64>("agg-1").await;
    assert!(matches!(wrong_load, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn snapshot_store_with_codec_shares_the_same_contract() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisSnapshotStore::<u64>::with_codec(
        &url,
        unique_prefix("snapshot"),
        catga_core::codec::memorypack::MemoryPackSnapshotCodec::default(),
    )
    .await?;
    store.save(Snapshot::new("agg-9", 99_u64, 3)).await?;
    assert_eq!(
        store
            .load::<u64>("agg-9")
            .await?
            .expect("a saved snapshot must load")
            .state(),
        &99_u64
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_load_rejects_incomplete_records() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("snapshot");
    let store = RedisSnapshotStore::<String>::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    store
        .save(Snapshot::new("agg-1", "state".to_string(), 7))
        .await?;
    let key = format!("{prefix}:snapshot:agg-1");

    // A record without its timestamp is internally inconsistent.
    let _: usize = raw.hdel(&key, "timestamp").await.map_err(map_redis_error)?;
    let missing_timestamp = store.load::<String>("agg-1").await;
    assert!(matches!(missing_timestamp, Err(error) if error.code() == ErrorCode::Internal));

    // A record without its state payload is internally inconsistent.
    store
        .save(Snapshot::new("agg-1", "state".to_string(), 7))
        .await?;
    let _: usize = raw.hdel(&key, "state").await.map_err(map_redis_error)?;
    let missing_state = store.load::<String>("agg-1").await;
    assert!(matches!(missing_state, Err(error) if error.code() == ErrorCode::Internal));

    Ok(())
}

// ==================== enhanced snapshots ====================

#[tokio::test]
async fn enhanced_snapshots_keep_an_ordered_version_history() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<String>::connect(&url, unique_prefix("enhanced")).await?;

    assert!(store.load::<String>("agg-1").await?.is_none());
    assert!(store.load_at_version::<String>("agg-1", 5).await?.is_none());
    assert!(store.history("agg-1").await?.is_empty());

    // Versions may be negative; lexicographic members preserve the numeric order.
    for (version, state) in [(-1, "before"), (0, "zero"), (1, "one"), (2, "two")] {
        store
            .save(Snapshot::new("agg-1", state.to_string(), version))
            .await?;
    }

    let latest = store
        .load::<String>("agg-1")
        .await?
        .expect("snapshots must load");
    assert_eq!(latest.version(), 2);
    assert_eq!(latest.state(), "two");

    // Loading at a version returns the newest snapshot not exceeding it.
    let at_zero = store
        .load_at_version::<String>("agg-1", 0)
        .await?
        .expect("a bounded load must find version zero");
    assert_eq!(at_zero.state(), "zero");
    let beyond = store
        .load_at_version::<String>("agg-1", 100)
        .await?
        .expect("a load beyond the newest version returns the newest");
    assert_eq!(beyond.version(), 2);
    assert!(
        store
            .load_at_version::<String>("agg-1", -5)
            .await?
            .is_none()
    );

    // A rewind conflicts instead of rewriting history.
    let conflict = store
        .save(Snapshot::new("agg-1", "rewind".to_string(), 1))
        .await;
    assert!(matches!(conflict, Err(error) if error.code() == ErrorCode::Conflict));

    let history = store.history("agg-1").await?;
    assert_eq!(history.len(), 4);
    assert_eq!(
        history
            .iter()
            .map(|info| info.version())
            .collect::<Vec<_>>(),
        vec![-1, 0, 1, 2]
    );

    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_delete_cleanup_and_expire_ranges() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<String>::connect(&url, unique_prefix("enhanced")).await?;
    for version in 0..6 {
        store
            .save(Snapshot::new("agg-1", format!("state-{version}"), version))
            .await?;
    }

    // Deleting before a version drops strictly older entries.
    store.delete_before_version("agg-1", 3).await?;
    assert_eq!(store.history("agg-1").await?.len(), 3);
    assert!(store.load_at_version::<String>("agg-1", 1).await?.is_none());

    // Cleanup retains only the newest entries.
    store.cleanup("agg-1", 2).await?;
    let history = store.history("agg-1").await?;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].version(), 4);

    // Cleanup beyond the population is a no-op.
    store.cleanup("agg-1", 10).await?;
    assert_eq!(store.history("agg-1").await?.len(), 2);

    store.delete("agg-1").await?;
    assert!(store.load::<String>("agg-1").await?.is_none());
    assert!(store.history("agg-1").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_roundtrip_a_pre_epoch_timestamp() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<String>::connect(&url, unique_prefix("enhanced")).await?;

    let before_epoch = UNIX_EPOCH - Duration::from_secs(90);
    store
        .save(Snapshot::from_shared(
            "agg-old",
            Arc::new("ancient".to_string()),
            -3,
            before_epoch,
        ))
        .await?;
    let loaded = store
        .load::<String>("agg-old")
        .await?
        .expect("a pre-epoch snapshot must load");
    assert_eq!(loaded.version(), -3);
    assert_eq!(loaded.timestamp(), before_epoch);
    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_detect_corrupt_indexes() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("enhanced");
    let store = RedisEnhancedSnapshots::<String>::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // An index member without its record is corruption, not a missing snapshot.
    let versions = format!("{prefix}:enhanced-snapshot:agg-corrupt:versions");
    let records = format!("{prefix}:enhanced-snapshot:agg-corrupt:records");
    let _: usize = raw
        .zadd(&versions, "0000000000000001", 0)
        .await
        .map_err(map_redis_error)?;
    let corrupt = store.load::<String>("agg-corrupt").await;
    assert!(matches!(corrupt, Err(error) if error.code() == ErrorCode::Internal));
    let corrupt_history = store.history("agg-corrupt").await;
    assert!(matches!(corrupt_history, Err(error) if error.code() == ErrorCode::Internal));

    // A non-hex index member cannot decode its version at all.
    let _: usize = raw.del(&versions).await.map_err(map_redis_error)?;
    let _: usize = raw
        .zadd(&versions, "not-a-version", 0)
        .await
        .map_err(map_redis_error)?;
    let _: usize = raw
        .hset(&records, "not-a-version", b"ignored".as_slice())
        .await
        .map_err(map_redis_error)?;
    let malformed = store.load::<String>("agg-corrupt").await;
    assert!(matches!(malformed, Err(error) if error.code() == ErrorCode::Internal));

    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_enforce_their_state_type() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<String>::connect(&url, unique_prefix("enhanced")).await?;

    let wrong_save = store.save(Snapshot::new("agg-1", 1_u64, 1)).await;
    assert!(matches!(wrong_save, Err(error) if error.code() == ErrorCode::Validation));
    store
        .save(Snapshot::new("agg-1", "state".to_string(), 1))
        .await?;
    let wrong_load = store.load::<u64>("agg-1").await;
    assert!(matches!(wrong_load, Err(error) if error.code() == ErrorCode::Validation));
    let wrong_load_at = store.load_at_version::<u64>("agg-1", 1).await;
    assert!(matches!(wrong_load_at, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_with_codec_shares_the_same_contract() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<u64>::with_codec(
        &url,
        unique_prefix("enhanced"),
        catga_core::codec::memorypack::MemoryPackSnapshotCodec::default(),
    )
    .await?;
    store.save(Snapshot::new("agg-7", 77_u64, 1)).await?;
    assert_eq!(
        store
            .load::<u64>("agg-7")
            .await?
            .expect("a saved snapshot must load")
            .state(),
        &77_u64
    );
    Ok(())
}

// ==================== state machines ====================

#[tokio::test]
async fn state_machines_cas_their_versions() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisStateMachines::<String>::connect(&url, unique_prefix("machine")).await?;

    assert!(store.get("machine-1").await?.is_none());

    let created = StateMachineSnapshot::new("machine-1", "created".to_string());
    assert!(store.create(created.clone()).await?);
    assert!(!store.create(created.clone()).await?);

    let loaded = store
        .get("machine-1")
        .await?
        .expect("a created machine must load");
    assert_eq!(loaded.instance_id(), "machine-1");
    assert_eq!(loaded.state(), "created");
    assert_eq!(loaded.version(), 0);

    // Versioned updates succeed only for the exact expected version.
    let next = loaded.next_version("running".to_string())?;
    assert!(store.update(0, next).await?);
    let stale = store
        .get("machine-1")
        .await?
        .expect("a created machine must load")
        .next_version("finished".to_string())?;
    assert!(!store.update(0, stale).await?);

    // A version jump is rejected before any Redis round trip.
    let current = store
        .get("machine-1")
        .await?
        .expect("a created machine must load");
    let skipped = current.version() + 1;
    let jumped = current.next_version("finished".to_string())?;
    assert!(!store.update(skipped, jumped).await?);

    // Updating a missing machine finds nothing to compare against.
    let missing = StateMachineSnapshot::new("machine-missing", "created".to_string())
        .next_version("running".to_string())?;
    assert!(!store.update(0, missing).await?);

    Ok(())
}

#[tokio::test]
async fn state_machines_with_codec_shares_the_same_contract() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisStateMachines::<u64>::with_codec(
        &url,
        unique_prefix("machine"),
        catga_core::codec::memorypack::MemoryPackSnapshotCodec::default(),
    )
    .await?;
    assert!(
        store
            .create(StateMachineSnapshot::new("machine-7", 7_u64))
            .await?
    );
    assert_eq!(
        store
            .get("machine-7")
            .await?
            .expect("a created machine must load")
            .state(),
        &7_u64
    );
    Ok(())
}

// ==================== error mapping ====================

#[tokio::test]
async fn snapshot_save_maps_unexpected_script_errors_to_transient() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("snapshot");
    let store = RedisSnapshotStore::<String>::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A plain string where the snapshot hash belongs breaks the save script's HGET.
    let _: () = raw
        .set(format!("{prefix}:snapshot:agg-1"), "a-plain-string")
        .await
        .map_err(map_redis_error)?;
    let result = store
        .save(Snapshot::new("agg-1", "state".to_string(), 1))
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_reject_an_oversized_retention_count() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEnhancedSnapshots::<String>::connect(&url, unique_prefix("enhanced")).await?;

    // A retention count beyond the Redis integer range is rejected before any round trip.
    let result = store.cleanup("agg-1", usize::MAX).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn enhanced_snapshots_map_unexpected_script_errors_to_transient() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("enhanced");
    let store = RedisEnhancedSnapshots::<String>::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A plain string where the version sorted set belongs breaks the load script's ZREVRANGE.
    let _: () = raw
        .set(
            format!("{prefix}:enhanced-snapshot:agg-1:versions"),
            "a-plain-string",
        )
        .await
        .map_err(map_redis_error)?;
    let result = store.load::<String>("agg-1").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}
