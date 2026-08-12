//! DSL step-progress edge contracts: corrupt records are internal decode errors, and
//! broker write failures are verified against the broker before failing transiently.

#[path = "support/full_bucket.rs"]
mod full_bucket;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;
#[path = "support/raw_kv.rs"]
mod raw_kv;

use catga_core::flow::{DslStepProgress, DslStepProgressStore};
use catga_core::{CatgaResult, ErrorCode};
use catga_nats::NatsDslStepProgress;
use full_bucket::fill_bucket;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;
use sha2::{Digest, Sha256};

/// Twin of the store-internal progress key derivation.
fn progress_key(flow_id: &str, step_index: u32) -> String {
    let mut digest = Sha256::new();
    digest.update(flow_id.as_bytes());
    digest.update(step_index.to_be_bytes());
    format!("d{}", hex::encode(digest.finalize()))
}

async fn connect(bucket: &str) -> CatgaResult<NatsDslStepProgress> {
    NatsDslStepProgress::connect(&server_url(), bucket).await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_records_are_internal_errors() -> CatgaResult<()> {
    let bucket = unique("CATGA_DSL_CORRUPT");
    let store = connect(&bucket).await?;
    raw_kv(&bucket)
        .await?
        .put(progress_key("flow-bad", 0), vec![0xFF; 16].into())
        .await
        .map_err(|error| test_error("inject corrupt DSL progress record", error))?;

    assert!(matches!(
        store.get("flow-bad", 0).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    assert!(matches!(
        store
            .update(0, DslStepProgress::new("flow-bad", 0, vec![1_u8]).next_version(vec![2_u8])?)
            .await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_write_failures_are_transient_and_never_committed() -> CatgaResult<()> {
    // A bucket that admits nothing rejects the create outright.
    let bucket = unique("CATGA_DSL_CAP0");
    raw_kv_with_byte_cap(&bucket, 1).await?;
    let store = connect(&bucket).await?;
    assert!(matches!(
        store.create(DslStepProgress::new("flow-wide", 0, vec![1_u8])).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(store.get("flow-wide", 0).await?.is_none());

    // A bucket that fills up later rejects the versioned rewrite.
    let bucket = unique("CATGA_DSL_CAPFILL");
    let raw = raw_kv_with_byte_cap(&bucket, 4_096).await?;
    let store = connect(&bucket).await?;
    assert!(
        store
            .create(DslStepProgress::new("flow-full", 0, vec![1_u8]))
            .await?
    );
    fill_bucket(&raw).await?;
    assert!(matches!(
        store
            .update(0, DslStepProgress::new("flow-full", 0, vec![1_u8]).next_version(vec![2_u8])?)
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original progress intact.
    assert_eq!(
        store
            .get("flow-full", 0)
            .await?
            .map(|progress| progress.version()),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_ambiguous_create_over_a_live_record_is_verified_against_the_broker() -> CatgaResult<()>
{
    // An oversized create is rejected before any network round trip, so the store must
    // compare its record token against the broker — where a live record already exists —
    // before it can report a transient failure.
    let bucket = unique("CATGA_DSL_AMBIG");
    let store = connect(&bucket).await?;
    assert!(
        store
            .create(DslStepProgress::new("flow-ambig", 0, vec![1_u8]))
            .await?
    );

    assert!(matches!(
        store
            .create(DslStepProgress::new("flow-ambig", 0, vec![2_u8; 2_000_000]))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The verification saw the originally committed record, not the rejected rewrite.
    assert_eq!(
        store
            .get("flow-ambig", 0)
            .await?
            .map(|progress| progress.payload().to_vec()),
        Some(vec![1_u8])
    );
    Ok(())
}
