//! State-machine edge contracts: ambiguous broker writes are verified before failing,
//! corrupt or legacy records behave deterministically, and missing or delete-marked
//! instances are misses for reads and optimistic updates alike.

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

use catga_core::flow::{StateMachineSnapshot, StateMachineStore};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsStateMachines;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;

fn kv_key(instance_id: &str) -> String {
    format!(
        "s{}",
        hex::encode(catga_core::hash::sha256_digest(instance_id.as_bytes()))
    )
}

async fn connect(bucket: &str) -> CatgaResult<NatsStateMachines<u64>> {
    NatsStateMachines::<u64>::connect(&server_url(), bucket).await
}

async fn inject(bucket: &str, instance_id: &str, value: Vec<u8>) -> CatgaResult<()> {
    raw_kv(bucket)
        .await?
        .put(kv_key(instance_id), value.into())
        .await
        .map_err(|error| test_error("inject state-machine record", error))?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn ambiguous_updates_are_verified_against_the_broker_before_failing() -> CatgaResult<()> {
    // The byte cap rejects the larger rewrite, so the compare-and-set error arm must re-read
    // the broker and discover that its write never committed before failing transiently.
    let bucket = unique("CATGA_SM_CAPPED");
    raw_kv_with_byte_cap(&bucket, 600).await?;
    let store = NatsStateMachines::<Vec<u8>>::connect(&server_url(), bucket.as_str()).await?;

    store
        .create(StateMachineSnapshot::new("sm-cap", vec![0x01u8; 100]))
        .await?;
    let next =
        StateMachineSnapshot::new("sm-cap", vec![0x01u8; 100]).next_version(vec![0x02u8; 400])?;
    assert!(matches!(
        store.update(0, next).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original snapshot untouched.
    let current = store
        .get("sm-cap")
        .await?
        .expect("the original snapshot must survive a failed rewrite");
    assert_eq!((current.version(), current.state().len()), (0, 100));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_records_are_internal_errors() -> CatgaResult<()> {
    for (name, bytes) in [
        ("raw garbage", vec![0xFF; 32]),
        ("truncated frame", b"CNR1short".to_vec()),
        ("framed garbage", record_frames::framed(&[0xFF; 8])),
    ] {
        let bucket = unique("CATGA_SM_CORRUPT");
        let store = connect(&bucket).await?;
        inject(&bucket, "sm-bad", bytes).await?;
        assert!(
            matches!(
                store.get("sm-bad").await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: get must surface an internal decode error"
        );
        let next = StateMachineSnapshot::new("sm-bad", 1_u64).next_version(2_u64)?;
        assert!(
            matches!(
                store.update(0, next).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name}: update must surface an internal decode error"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn missing_and_deleted_instances_are_update_misses() -> CatgaResult<()> {
    let bucket = unique("CATGA_SM_MISSES");
    let store = connect(&bucket).await?;

    // A missing instance cannot be updated.
    let next = StateMachineSnapshot::new("sm-missing", 1_u64).next_version(2_u64)?;
    assert!(!store.update(0, next).await?);

    // A delete-marked record is a miss for reads and updates.
    store
        .create(StateMachineSnapshot::new("sm-del", 1_u64))
        .await?;
    raw_kv(&bucket)
        .await?
        .delete(kv_key("sm-del"))
        .await
        .map_err(|error| test_error("delete state-machine key", error))?;
    assert!(store.get("sm-del").await?.is_none());
    let next = StateMachineSnapshot::new("sm-del", 9_u64).next_version(10_u64)?;
    assert!(!store.update(0, next).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn updates_fence_non_successor_and_stale_versions() -> CatgaResult<()> {
    let bucket = unique("CATGA_SM_FENCE");
    let store = connect(&bucket).await?;
    store
        .create(StateMachineSnapshot::new("sm-fence", 1_u64))
        .await?;

    // The stored version advanced past the expected one, so the fence compares versions.
    let next = StateMachineSnapshot::new("sm-fence", 2_u64).next_version(3_u64)?;
    assert!(store.update(0, next.clone()).await?);
    assert!(!store.update(0, next).await?);

    // A non-successor write is rejected before any broker round trip.
    let v2 = StateMachineSnapshot::new("sm-fence", 4_u64)
        .next_version(5_u64)?
        .next_version(6_u64)?;
    assert!(!store.update(5, v2).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn legacy_unframed_records_round_trip() -> CatgaResult<()> {
    let bucket = unique("CATGA_SM_LEGACY");
    let store = connect(&bucket).await?;
    store
        .create(StateMachineSnapshot::new("sm-legacy", 41_u64))
        .await?;

    // Downgrade the stored record to its pre-CNR1 wire shape by stripping the frame.
    let raw = raw_kv(&bucket).await?;
    let entry = raw
        .entry(kv_key("sm-legacy"))
        .await
        .map_err(|error| test_error("read framed state-machine record", error))?
        .expect("the created record must exist");
    let payload = entry.value[record_frames::HEADER_BYTES..].to_vec();
    raw.put(kv_key("sm-legacy"), payload.clone().into())
        .await
        .map_err(|error| test_error("write legacy state-machine record", error))?;

    // The legacy record decodes, and an update preserves its unframed shape.
    let current = store
        .get("sm-legacy")
        .await?
        .expect("the legacy record must decode");
    assert_eq!((current.version(), *current.state()), (0, 41));
    let next = current.next_version(42_u64)?;
    assert!(store.update(0, next).await?);
    let stored = raw
        .entry(kv_key("sm-legacy"))
        .await
        .map_err(|error| test_error("read updated legacy record", error))?
        .expect("the updated record must exist");
    assert!(!stored.value.starts_with(record_frames::PREFIX));
    assert_eq!(
        store
            .get("sm-legacy")
            .await?
            .map(|snapshot| (snapshot.version(), *snapshot.state())),
        Some((1, 42))
    );
    Ok(())
}
