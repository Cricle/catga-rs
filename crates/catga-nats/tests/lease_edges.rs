//! Lease edge contracts: corrupt lease values fence every operation, and a broker-side
//! write rejection during acquisition is a clean miss rather than an error.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;
#[path = "support/raw_kv.rs"]
mod raw_kv;

use std::time::Duration;

use catga_core::{CatgaResult, LeaseStore};
use catga_nats::NatsLeases;
use names::unique;
use nats_server::{server_url, test_error};
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;

async fn connect(bucket: &str) -> CatgaResult<NatsLeases> {
    NatsLeases::connect(&server_url(), bucket).await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_lease_values_fence_every_operation() -> CatgaResult<()> {
    let bucket = unique("CATGA_LEASE_CORRUPT");
    let store = connect(&bucket).await?;
    for (name, value) in [
        ("no-separator", b"owner-without-expiry".to_vec()),
        ("non-numeric-expiry", b"owner\tnot-a-number".to_vec()),
        ("invalid-utf8", vec![0xFF, 0xFE, 0x09, 0x31]),
    ] {
        let resource = format!("resource-{name}");
        raw_kv(&bucket)
            .await?
            .put(resource.as_str(), value.into())
            .await
            .map_err(|error| test_error("inject corrupt lease value", error))?;

        // An unparseable value can never prove ownership, so every operation is a miss.
        assert!(
            !store
                .try_acquire(&resource, "owner-a", Duration::from_secs(30))
                .await?,
            "case {name}: try_acquire must treat a corrupt value as unparsable"
        );
        assert!(
            !store
                .renew(&resource, "owner-a", Duration::from_secs(30))
                .await?,
            "case {name}: renew must treat a corrupt value as unparsable"
        );
        assert!(
            !store.release(&resource, "owner-a").await?,
            "case {name}: release must treat a corrupt value as unparsable"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_broker_rejected_acquire_is_a_clean_miss() -> CatgaResult<()> {
    // The byte cap rejects the initial create, and with no entry to compare against the
    // acquisition reports a plain miss instead of an error.
    let bucket = unique("CATGA_LEASE_CAPPED");
    raw_kv_with_byte_cap(&bucket, 300).await?;
    let store = connect(&bucket).await?;
    assert!(
        !store
            .try_acquire("resource-capped", &"o".repeat(400), Duration::from_secs(30))
            .await?
    );

    // A TTL that overflows milliseconds-to-unix-time arithmetic still clamps safely.
    let store = connect(&unique("CATGA_LEASE_TTL")).await?;
    assert!(
        store
            .try_acquire("resource-eternal", "owner-a", Duration::MAX)
            .await?
    );
    assert!(
        store
            .renew("resource-eternal", "owner-a", Duration::MAX)
            .await?
    );
    Ok(())
}
