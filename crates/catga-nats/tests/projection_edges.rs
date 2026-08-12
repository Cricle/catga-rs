//! Projection-checkpoint edge contracts: ambiguous broker writes are verified before
//! failing, corrupt records surface internal decode errors while `delete_all` stays a
//! metadata-only operation, and legacy unframed records keep decoding.

#[path = "support/envelopes.rs"]
mod envelopes;
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
use catga_core::{
    CatgaResult, ErrorCode, EventStore, ProjectionCheckpoint, ProjectionCheckpointStore,
};
use catga_nats::NatsProjectionCheckpoints;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;
use record_frames::framed;
use serde::{Deserialize, Serialize};

/// Twin of the store-internal per-stream checkpoint entry.
#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredCheckpoint {
    stream_id: Box<str>,
    version: i64,
    updated_at_unix_ms: u64,
}

/// Twin of the store-internal projection checkpoint list.
#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredCheckpoints {
    checkpoints: Vec<StoredCheckpoint>,
}

fn encode<T: MemoryPackSerialize>(value: &T) -> Vec<u8> {
    MemoryPackSerializer::serialize(value).expect("test record must serialize")
}

fn projection_key(projection_name: &str) -> String {
    format!(
        "p{}",
        hex::encode(catga_core::hash::sha256_digest(projection_name.as_bytes()))
    )
}

async fn connect(bucket: &str) -> CatgaResult<NatsProjectionCheckpoints> {
    NatsProjectionCheckpoints::connect(&server_url(), bucket).await
}

async fn inject(bucket: &str, projection: &str, value: Vec<u8>) -> CatgaResult<()> {
    raw_kv(bucket)
        .await?
        .put(projection_key(projection), value.into())
        .await
        .map_err(|error| test_error("inject projection record", error))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn ambiguous_writes_are_verified_against_the_broker_before_failing() -> CatgaResult<()> {
    // The byte cap rejects oversized writes, so the create and compare-and-set error arms
    // must re-read the broker and discover their write never committed before failing.
    let bucket = unique("CATGA_PROJ_CAPPED");
    raw_kv_with_byte_cap(&bucket, 400).await?;
    let store = connect(&bucket).await?;

    // An oversized first save cannot commit the initial record.
    assert!(matches!(
        store
            .save(ProjectionCheckpoint::new("orders/wide", "s".repeat(500), 1))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(store.load("orders/wide", &"s".repeat(500)).await?.is_none());

    // Growing the record past the byte cap fails the compare-and-set without committing.
    store
        .save(ProjectionCheckpoint::new("orders/cap", "order-1", 1))
        .await?;
    assert!(matches!(
        store
            .save(ProjectionCheckpoint::new("orders/cap", "w".repeat(500), 2))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original checkpoint list intact.
    assert_eq!(
        store
            .load("orders/cap", "order-1")
            .await?
            .map(|checkpoint| checkpoint.version()),
        Some(1)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_records_are_internal_errors_but_delete_all_is_metadata_only() -> CatgaResult<()> {
    for (name, bytes) in [
        ("raw garbage", vec![0xFF; 32]),
        ("truncated frame", b"CNR1short".to_vec()),
        ("framed garbage", framed(&[0xFF; 8])),
    ] {
        let bucket = unique("CATGA_PROJ_CORRUPT");
        let store = connect(&bucket).await?;
        inject(&bucket, "orders/bad", bytes).await?;
        assert!(
            matches!(
                store.load("orders/bad", "order-1").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: load must surface an internal decode error"
        );
        assert!(
            matches!(
                store
                    .save(ProjectionCheckpoint::new("orders/bad", "order-1", 1))
                    .await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: save must surface an internal decode error"
        );
        assert!(
            matches!(
                store.delete("orders/bad", "order-1").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: delete must surface an internal decode error"
        );
        // delete_all never decodes the record: it drops the key outright.
        store.delete_all("orders/bad").await?;
        assert!(store.load("orders/bad", "order-1").await?.is_none());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn saves_recreate_delete_marked_projections_and_deletes_stay_idempotent() -> CatgaResult<()> {
    let bucket = unique("CATGA_PROJ_RECREATE");
    let store = connect(&bucket).await?;

    // delete_all on a missing projection is a no-op.
    store.delete_all("orders/missing").await?;
    store.delete("orders/missing", "order-1").await?;

    store
        .save(ProjectionCheckpoint::new("orders/recycle", "order-1", 1))
        .await?;
    store.delete_all("orders/recycle").await?;
    // A save over the delete-marked key recreates the record at the marker's revision.
    store
        .save(ProjectionCheckpoint::new("orders/recycle", "order-2", 5))
        .await?;
    assert!(store.load("orders/recycle", "order-1").await?.is_none());
    assert_eq!(
        store
            .load("orders/recycle", "order-2")
            .await?
            .map(|checkpoint| checkpoint.version()),
        Some(5)
    );

    // Deleting the last remaining checkpoint removes the whole key; both repeats are no-ops.
    store.delete("orders/recycle", "order-2").await?;
    store.delete("orders/recycle", "order-2").await?;
    store.delete_all("orders/recycle").await?;
    assert!(store.load("orders/recycle", "order-2").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn legacy_unframed_records_round_trip() -> CatgaResult<()> {
    let bucket = unique("CATGA_PROJ_LEGACY");
    let store = connect(&bucket).await?;
    let legacy = StoredCheckpoints {
        checkpoints: vec![StoredCheckpoint {
            stream_id: "order-7".into(),
            version: 3,
            updated_at_unix_ms: 1_700_000_000_000,
        }],
    };
    inject(&bucket, "orders/legacy", encode(&legacy)).await?;

    // A record written before the CNR1 frame existed is still decoded.
    let checkpoint = store
        .load("orders/legacy", "order-7")
        .await?
        .expect("the legacy checkpoint must decode");
    assert_eq!(
        (
            checkpoint.projection_name(),
            checkpoint.stream_id(),
            checkpoint.version()
        ),
        ("orders/legacy", "order-7", 3)
    );

    // Updating a legacy record preserves its unframed wire shape.
    store
        .save(ProjectionCheckpoint::new("orders/legacy", "order-8", 4))
        .await?;
    let stored = raw_kv(&bucket)
        .await?
        .entry(projection_key("orders/legacy"))
        .await
        .map_err(|error| test_error("read updated legacy record", error))?
        .expect("the updated record must exist");
    assert!(!stored.value.starts_with(record_frames::PREFIX));
    assert_eq!(
        store
            .load("orders/legacy", "order-8")
            .await?
            .map(|checkpoint| checkpoint.version()),
        Some(4)
    );
    Ok(())
}

/// Counting projection for runner tests.
struct ProjectionCounter {
    total: std::sync::atomic::AtomicUsize,
}

impl ProjectionCounter {
    const fn new() -> Self {
        Self {
            total: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn total(&self) -> usize {
        self.total.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl catga_core::Projection for ProjectionCounter {
    fn name(&self) -> &str {
        "nats-projection-runner-edges"
    }

    async fn apply(&self, event: &catga_core::StoredEvent) -> CatgaResult<()> {
        self.total.fetch_add(
            usize::from(event.envelope().payload()[0]),
            std::sync::atomic::Ordering::AcqRel,
        );
        Ok(())
    }

    async fn reset(&self) -> CatgaResult<()> {
        self.total.store(0, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn the_runner_pages_replays_through_an_explicit_batch_size() -> CatgaResult<()> {
    let config = catga_nats::NatsProjectionConfig {
        event_stream: unique("CATGA_PROJRUN_EVENTS").into(),
        event_subject_prefix: unique("catga.projrun.events").into(),
        checkpoint_bucket: unique("CATGA_PROJRUN_CHECKPOINTS").into(),
    };
    let events = catga_nats::NatsEventStore::connect(
        &server_url(),
        config.event_stream.clone(),
        config.event_subject_prefix.clone(),
    )
    .await?;
    events
        .append(
            "order-1",
            vec![
                envelopes::envelope(21, catga_core::QualityOfService::AtLeastOnce),
                envelopes::envelope(22, catga_core::QualityOfService::AtLeastOnce),
            ],
            None,
        )
        .await?;

    // A one-event batch size forces the runner through its paged replay arm.
    let runner =
        catga_nats::NatsProjectionRunner::connect(&server_url(), config, ProjectionCounter::new())
            .await?
            .with_batch_size(std::num::NonZeroUsize::new(1).expect("batch size must be nonzero"));
    let run = runner.run().await?;
    assert_eq!(run.applied(), 2);
    assert_eq!(runner.projection().total(), 43);

    // A rebuild replays the same pages from the beginning.
    let run = runner.rebuild().await?;
    assert_eq!(run.applied(), 2);
    assert_eq!(runner.projection().total(), 43);
    Ok(())
}
