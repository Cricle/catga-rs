//! Subscription-store edge contracts: lease-TTL validation, idempotent deletes with and
//! without a held lease, contention retries on shared keys, broker write failures, and
//! deterministic list ordering.
//!
//! The store keeps definitions, checkpoints, and competing-consumer leases in one bucket,
//! so these tests pin how the CAS loops degrade: contention retries are bounded, a delete
//! marker cannot be superseded by a create, and storage failures are transient, never
//! silently committed.

#[path = "support/full_bucket.rs"]
mod full_bucket;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaResult, ErrorCode, PersistentSubscription, SubscriptionStore};
use catga_nats::NatsSubscriptions;
use full_bucket::fill_bucket;
use names::unique;
use nats_server::server_url;
use raw_capped_kv::raw_kv_with_byte_cap;

fn subscription(name: &str) -> PersistentSubscription {
    PersistentSubscription::new(name, "orders.*").with_event_types(["created"])
}

async fn connect(bucket: &str) -> CatgaResult<NatsSubscriptions> {
    NatsSubscriptions::connect(&server_url(), bucket).await
}

#[tokio::test]
async fn a_zero_lease_ttl_is_a_validation_error() {
    // The TTL check runs before any network I/O, so the unreachable address is never used.
    assert!(matches!(
        NatsSubscriptions::with_lease_ttl(
            "nats://127.0.0.1:1",
            unique("CATGA_SUB_TTL"),
            Duration::ZERO
        )
        .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn deletes_are_idempotent_with_or_without_a_lease() -> CatgaResult<()> {
    let store = connect(&unique("CATGA_SUB_DEL")).await?;

    // No lease was ever taken: cleanup finds no lease entry and succeeds.
    store.save(subscription("sub-no-lease")).await?;
    store.delete("sub-no-lease").await?;
    store.delete("sub-no-lease").await?;
    assert!(store.load("sub-no-lease").await?.is_none());

    // A held lease is removed with the definition; the repeat delete sees only markers.
    store.save(subscription("sub-leased")).await?;
    assert!(store.try_acquire("sub-leased", "consumer-1").await?);
    store.delete("sub-leased").await?;
    store.delete("sub-leased").await?;
    assert!(store.load("sub-leased").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn concurrent_deletes_settle_to_one_terminal_state() -> CatgaResult<()> {
    let store = Arc::new(connect(&unique("CATGA_SUB_DELRACE")).await?);
    store.save(subscription("sub-raced")).await?;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = Arc::clone(&store);
        tasks.spawn(async move { store.delete("sub-raced").await });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("delete task must not panic") {
            Ok(()) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    assert!(store.load("sub-raced").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn concurrent_saves_and_deletes_retry_until_they_settle() -> CatgaResult<()> {
    let store = Arc::new(connect(&unique("CATGA_SUB_SAVERACE")).await?);
    store.save(subscription("sub-contended")).await?;

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let deleter = Arc::clone(&store);
        tasks.spawn(async move { deleter.delete("sub-contended").await });
        let saver = Arc::clone(&store);
        tasks.spawn(async move { saver.save(subscription("sub-contended")).await });
    }
    while let Some(result) = tasks.join_next().await {
        match result.expect("save/delete task must not panic") {
            Ok(()) => {}
            Err(error) => assert_eq!(error.code(), ErrorCode::Transient),
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn saving_over_a_delete_marked_definition_fails_transiently() -> CatgaResult<()> {
    let store = connect(&unique("CATGA_SUB_RECREATE")).await?;
    store.save(subscription("sub-recycled")).await?;
    store.delete("sub-recycled").await?;

    // A revision-zero create can never supersede the delete marker, so the bounded
    // create retries exhaust and report a transient CAS failure.
    assert!(matches!(
        store.save(subscription("sub-recycled")).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_write_failures_are_transient_and_never_committed() -> CatgaResult<()> {
    // A bucket that admits nothing rejects the definition create outright.
    let bucket = unique("CATGA_SUB_CAP0");
    raw_kv_with_byte_cap(&bucket, 1).await?;
    let store = connect(&bucket).await?;
    assert!(matches!(
        store.save(subscription("sub-nowrite")).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(store.load("sub-nowrite").await?.is_none());

    // A bucket that fills up later rejects rewrites, checkpoints, and lease writes.
    let bucket = unique("CATGA_SUB_CAPFILL");
    let raw = raw_kv_with_byte_cap(&bucket, 4_096).await?;
    let store = connect(&bucket).await?;
    store.save(subscription("sub-full")).await?;
    fill_bucket(&raw).await?;
    // A changed rewrite no longer fits and the broker proves it never committed.
    assert!(matches!(
        store
            .save(PersistentSubscription::new("sub-full", "payments.*").with_event_types(["created"]))
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(matches!(
        store.try_acquire("sub-full", "consumer-1").await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    // The failed rewrite left the original definition intact.
    assert_eq!(
        store
            .load("sub-full")
            .await?
            .map(|subscription| subscription.stream_pattern().to_owned()),
        Some("orders.*".into())
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn reviving_a_lease_on_a_full_bucket_fails_transiently() -> CatgaResult<()> {
    let bucket = unique("CATGA_SUB_CAPREVIVE");
    let raw = raw_kv_with_byte_cap(&bucket, 4_096).await?;
    let store = connect(&bucket).await?;

    assert!(store.try_acquire("sub-revive", "consumer-1").await?);
    store.release("sub-revive", "consumer-1").await?;
    fill_bucket(&raw).await?;

    // The released lease persists as a delete marker; rewriting it crosses the byte cap.
    assert!(matches!(
        store.try_acquire("sub-revive", "consumer-2").await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn listing_sorts_multiple_subscriptions_by_name() -> CatgaResult<()> {
    let store = connect(&unique("CATGA_SUB_SORT")).await?;
    store.save(subscription("zz-sub")).await?;
    store.save(subscription("aa-sub")).await?;
    store.save(subscription("mm-sub")).await?;

    let names: Vec<String> = store
        .list()
        .await?
        .iter()
        .map(|subscription| subscription.name().to_owned())
        .collect();
    assert_eq!(names, vec!["aa-sub", "mm-sub", "zz-sub"]);
    Ok(())
}
