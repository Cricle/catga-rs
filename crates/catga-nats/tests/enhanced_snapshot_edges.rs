//! Enhanced-snapshot edge contracts: ambiguous broker writes are verified before failing,
//! corrupt or legacy records surface deterministic errors, and history mutations on missing
//! or delete-marked streams are silent no-ops.
//!
//! Every stream maps to one SHA-256-derived KV key holding a framed `StoredHistory`, so the
//! fixtures place bytes behind the exact internal keys the store reads.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;
#[path = "support/raw_kv.rs"]
mod raw_kv;
#[path = "support/record_frames.rs"]
mod record_frames;

use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};
use catga_core::{CatgaResult, EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotStore};
use catga_nats::NatsEnhancedSnapshots;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;
use record_frames::framed;
use serde::{Deserialize, Serialize};

/// Twin of the store-internal snapshot entry.
#[derive(Clone, Deserialize, MemoryPackable, Serialize)]
struct StoredSnapshot {
    version: i64,
    timestamp_unix_ms: u64,
    state: Vec<u8>,
}

/// Twin of the store-internal ordered version history.
#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredHistory {
    entries: Vec<StoredSnapshot>,
}

fn encode<T: MemoryPackSerialize>(value: &T) -> Vec<u8> {
    MemoryPackSerializer::serialize(value).expect("test record must serialize")
}

fn stream_key(stream_id: &str) -> String {
    format!(
        "s{}",
        hex::encode(catga_core::hash::sha256_digest(stream_id.as_bytes()))
    )
}

async fn connect(bucket: &str) -> CatgaResult<NatsEnhancedSnapshots<u64>> {
    NatsEnhancedSnapshots::<u64>::connect(&server_url(), bucket).await
}

fn entry(version: i64, state: Vec<u8>) -> StoredSnapshot {
    StoredSnapshot {
        version,
        timestamp_unix_ms: 1_700_000_000_000,
        state,
    }
}

fn u64_state(value: u64) -> Vec<u8> {
    MemoryPackSerializer::serialize(&value).expect("u64 state must serialize")
}

async fn inject(bucket: &str, stream_id: &str, value: Vec<u8>) -> CatgaResult<()> {
    raw_kv(bucket)
        .await?
        .put(stream_key(stream_id), value.into())
        .await
        .map_err(|error| test_error("inject enhanced-snapshot record", error))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn ambiguous_writes_are_verified_against_the_broker_before_failing() -> CatgaResult<()> {
    // The bucket rejects writes that would grow the stream past its byte cap, so both the
    // create and the compare-and-set error arms must re-read the broker and discover that
    // their write never committed before reporting a transient failure.
    let bucket = unique("CATGA_ESNAP_CAPPED");
    raw_kv_with_byte_cap(&bucket, 600).await?;
    let store = NatsEnhancedSnapshots::<Vec<u8>>::connect(&server_url(), bucket.as_str()).await?;

    // An oversized first save cannot commit the initial record.
    assert!(matches!(
        store.save(Snapshot::new("stream-wide", vec![0x0Au8; 800], 1)).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(store.load::<Vec<u8>>("stream-wide").await?.is_none());

    // Growing the history past the byte cap fails the compare-and-set without committing.
    store
        .save(Snapshot::new("stream-cap", vec![0x01u8; 200], 1))
        .await?;
    assert!(matches!(
        store.save(Snapshot::new("stream-cap", vec![0x02u8; 200], 2)).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original history intact.
    assert_eq!(
        store
            .load::<Vec<u8>>("stream-cap")
            .await?
            .map(|snapshot| snapshot.version()),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_histories_are_internal_errors() -> CatgaResult<()> {
    for (name, bytes) in [
        ("raw garbage", vec![0xFF; 32]),
        ("truncated frame", b"CNR1short".to_vec()),
        ("framed garbage", framed(&[0xFF; 8])),
    ] {
        let bucket = unique("CATGA_ESNAP_CORRUPT");
        let store = connect(&bucket).await?;
        inject(&bucket, "stream-bad", bytes).await?;
        assert!(
            matches!(
                store.load::<u64>("stream-bad").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: load must surface an internal decode error"
        );
        assert!(
            matches!(
                store.history("stream-bad").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: history must surface an internal decode error"
        );
        assert!(
            matches!(
                store.load_at_version::<u64>("stream-bad", 9).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: load_at_version must surface an internal decode error"
        );
        assert!(
            matches!(
                store.delete_before_version("stream-bad", 9).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: delete_before_version must surface an internal decode error"
        );
        assert!(
            matches!(
                store.cleanup("stream-bad", 1).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: cleanup must surface an internal decode error"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_corrupt_state_payload_is_a_validation_error() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_BADSTATE");
    let store = connect(&bucket).await?;
    let history = StoredHistory {
        entries: vec![entry(1, vec![0xFFu8; 3])],
    };
    inject(&bucket, "stream-state", framed(&encode(&history))).await?;
    // State payloads decode through the caller-visible codec, so truncation is Validation.
    assert!(matches!(
        store.load::<u64>("stream-state").await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    // The metadata view does not decode state payloads and stays readable.
    assert_eq!(
        store
            .history("stream-state")
            .await?
            .iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        vec![1]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn mutations_on_missing_and_deleted_streams_are_silent_noops() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_NOOP");
    let store = connect(&bucket).await?;

    // Missing streams accept every mutation as a no-op.
    store.delete_before_version("missing", 5).await?;
    store.cleanup("missing", 0).await?;
    store.delete("missing").await?;

    // Delete-marked records are misses for reads and mutations alike.
    store.save(Snapshot::new("stream-del", 1_u64, 1)).await?;
    store.delete("stream-del").await?;
    assert!(store.load::<u64>("stream-del").await?.is_none());
    assert!(store.history("stream-del").await?.is_empty());
    assert!(
        store
            .load_at_version::<u64>("stream-del", 9)
            .await?
            .is_none()
    );
    store.delete_before_version("stream-del", 2).await?;
    store.cleanup("stream-del", 0).await?;
    store.delete("stream-del").await?;

    // A delete marker is terminal: the revision-zero create used for resurrection can never
    // supersede it, so recreating a deleted stream exhausts its bounded retries.
    assert!(matches!(
        store.save(Snapshot::new("stream-del", 2_u64, 4)).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(store.load::<u64>("stream-del").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transforms_that_change_nothing_skip_the_broker_write() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_UNCHANGED");
    let store = connect(&bucket).await?;
    for version in [1, 2] {
        store
            .save(Snapshot::new("stream-keep", version as u64, version))
            .await?;
    }

    // Every retained entry is at or above the cutoff, so nothing is rewritten.
    store.delete_before_version("stream-keep", 1).await?;
    // Keeping more than (or exactly) the retained count is also a no-op.
    store.cleanup("stream-keep", 5).await?;
    store.cleanup("stream-keep", 2).await?;
    assert_eq!(
        store
            .history("stream-keep")
            .await?
            .iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    // Draining every entry removes the key outright.
    store.cleanup("stream-keep", 0).await?;
    assert!(store.load::<u64>("stream-keep").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn legacy_unframed_histories_round_trip() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_LEGACY");
    let store = connect(&bucket).await?;
    let history = StoredHistory {
        entries: vec![entry(1, u64_state(10))],
    };
    // A record written before the CNR1 frame existed is still decoded.
    inject(&bucket, "stream-legacy", encode(&history)).await?;
    assert_eq!(
        store
            .load::<u64>("stream-legacy")
            .await?
            .map(|snapshot| (*snapshot.state(), snapshot.version())),
        Some((10, 1))
    );

    // Updating a legacy record preserves its unframed wire shape.
    store
        .save(Snapshot::new("stream-legacy", 20_u64, 2))
        .await?;
    assert_eq!(
        store
            .history("stream-legacy")
            .await?
            .iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn reads_and_writes_enforce_the_store_state_type() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_TYPES");
    let store = connect(&bucket).await?;
    store.save(Snapshot::new("stream-typed", 1_u64, 1)).await?;

    assert!(matches!(
        store.save(Snapshot::new("stream-typed", "text".to_string(), 2)).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        store.load::<String>("stream-typed").await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        store.load_at_version::<String>("stream-typed", 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    // A historical read below the oldest retained version is a miss.
    assert!(
        store
            .load_at_version::<u64>("stream-typed", 0)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn history_contention_settles_every_writer() -> CatgaResult<()> {
    let bucket = unique("CATGA_ESNAP_STORM");
    let store = std::sync::Arc::new(connect(&bucket).await?);
    store.save(Snapshot::new("stream-storm", 0_u64, 1)).await?;

    // Concurrent saves race the shared history record compare-and-set.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16_i64 {
        let store = std::sync::Arc::clone(&store);
        tasks.spawn(async move {
            store
                .save(Snapshot::new("stream-storm", index as u64, index + 2))
                .await
        });
    }
    let mut saved = 0_usize;
    while let Some(result) = tasks.join_next().await {
        match result.expect("save task must not panic") {
            Ok(()) => saved += 1,
            Err(error) => assert!(matches!(
                error.code(),
                ErrorCode::Transient | ErrorCode::Conflict
            )),
        }
    }
    assert!(saved > 0);

    // Concurrent retention sweeps race the same record, including the drain-to-delete arm.
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16_u32 {
        let store = std::sync::Arc::clone(&store);
        tasks.spawn(async move {
            if index % 2 == 0 {
                store.cleanup("stream-storm", 0).await
            } else {
                store.cleanup("stream-storm", 1).await
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("cleanup task must not panic") {
            Ok(()) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert!(store.history("stream-storm").await?.len() <= 1);
    Ok(())
}
