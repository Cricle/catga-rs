//! Latest-snapshot edge contracts: a truncated stored value is an internal decode error on
//! both reads and version-checking writes.
//!
//! The remaining uncovered error arms in `snapshot.rs` are unreachable by construction:
//! the downcast guards run after `require_state` has already proven `T == S`, and the
//! fixed-width metadata slices can never fail their `try_into` conversions.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_kv.rs"]
mod raw_kv;

use catga_core::{CatgaResult, ErrorCode, Snapshot, SnapshotStore};
use catga_nats::NatsSnapshotStore;
use names::unique;
use nats_server::{server_url, test_error};
use raw_kv::raw_kv;

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn truncated_values_are_internal_errors() -> CatgaResult<()> {
    let bucket = unique("CATGA_SNAP_SHORT");
    let store = NatsSnapshotStore::<u64>::connect(&server_url(), bucket.as_str()).await?;

    // A value shorter than the 16-byte metadata header cannot decode.
    raw_kv(&bucket)
        .await?
        .put("stream-short", vec![0x01; 8].into())
        .await
        .map_err(|error| test_error("inject truncated snapshot value", error))?;
    assert!(matches!(
        store.load::<u64>("stream-short").await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    // The save path decodes the current value for its version fence and fails the same way.
    assert!(matches!(
        store.save(Snapshot::new("stream-short", 1_u64, 1)).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));

    // A well-formed value with undecodable state bytes surfaces the codec's Validation error.
    let mut value = vec![0u8; 16];
    value.extend_from_slice(&[0xFF; 3]);
    raw_kv(&bucket)
        .await?
        .put("stream-state", value.into())
        .await
        .map_err(|error| test_error("inject corrupt snapshot state", error))?;
    assert!(matches!(
        store.load::<u64>("stream-state").await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}
