//! NATS JetStream integration tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{
    Envelope, ErrorCode, EventStore, IdempotencyStore, LeaseStore, MessageMetadata,
    MessageTransport, ProcessingState, Snapshot, SnapshotStore,
};
use catga_nats::{
    NatsConfig, NatsEventStore, NatsIdempotency, NatsLeases, NatsSnapshotStore, NatsTransport,
};

#[tokio::test]
async fn nats_leases_compare_owner_with_kv_revisions() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        return;
    };
    let leases = NatsLeases::connect(&server, format!("CATGA_LEASE_{}", std::process::id()))
        .await
        .unwrap();
    assert!(
        leases
            .try_acquire("outbox", "node-a", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(
        !leases
            .try_acquire("outbox", "node-b", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(!leases.release("outbox", "node-b").await.unwrap());
    assert!(
        leases
            .renew("outbox", "node-a", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(leases.release("outbox", "node-a").await.unwrap());
}

#[tokio::test]
async fn jetstream_round_trip_and_ack() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let suffix = format!("{}", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_{suffix}").into(),
        subject: format!("catga.{suffix}").into(),
        consumer: format!("catga_{suffix}").into(),
    })
    .await
    .unwrap();

    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![1, 2],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();
    let delivery = transport.receive().await.unwrap();
    assert_eq!(delivery.envelope().payload(), [1, 2]);
    transport.ack(delivery).await.unwrap();
}

#[tokio::test]
async fn nats_event_store_persists_versioned_history_with_subject_cas() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let suffix = format!("{}", std::process::id());
    let store = Arc::new(
        NatsEventStore::connect(
            &server,
            format!("CATGA_EVENTS_{suffix}"),
            format!("catga.events.{suffix}"),
        )
        .await
        .unwrap(),
    );
    assert_eq!(
        store
            .append(
                "orders-7",
                vec![Envelope::new(
                    1,
                    "order.created",
                    vec![1],
                    MessageMetadata::new(1, None),
                )],
                Some(-1),
            )
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .append(
                "orders-7",
                vec![Envelope::new(
                    1,
                    "order.paid",
                    vec![2],
                    MessageMetadata::new(2, Some(1)),
                )],
                Some(0),
            )
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .append(
                "orders-7",
                vec![Envelope::new(
                    1,
                    "order.duplicate",
                    vec![3],
                    MessageMetadata::new(3, None),
                )],
                Some(0),
            )
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(
        store.read("orders-7", 1, 1).await.unwrap().events()[0]
            .envelope()
            .payload(),
        [2]
    );
    assert_eq!(
        store
            .read_to_version("orders-7", 0)
            .await
            .unwrap()
            .events()
            .len(),
        1
    );
    assert_eq!(
        store
            .read_to_time("orders-7", SystemTime::now())
            .await
            .unwrap()
            .events()
            .len(),
        2
    );
    assert_eq!(
        store.version_history("orders-7").await.unwrap()[1].event_type(),
        "order.paid"
    );
    assert_eq!(store.stream_ids().await.unwrap(), ["orders-7"]);
}

#[tokio::test]
async fn nats_snapshots_round_trip_and_reject_stale_writers_with_kv_revisions() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let suffix = format!("{}", std::process::id());
    let store = Arc::new(
        NatsSnapshotStore::<u64>::connect(&server, format!("CATGA_SNAPSHOTS_{suffix}"))
            .await
            .unwrap(),
    );
    store
        .save(Snapshot::new("orders-7", 10_u64, 4))
        .await
        .unwrap();
    let loaded = store.load::<u64>("orders-7").await.unwrap().unwrap();
    assert_eq!(*loaded.state(), 10);
    assert_eq!(loaded.version(), 4);

    let first_writer = Arc::clone(&store);
    let second_writer = Arc::clone(&store);
    let (first, second) = tokio::join!(
        first_writer.save(Snapshot::new("orders-7", 11_u64, 5)),
        second_writer.save(Snapshot::new("orders-7", 12_u64, 3)),
    );
    assert!(first.is_ok());
    assert_eq!(second.unwrap_err().code(), ErrorCode::Conflict);
    assert_eq!(
        *store
            .load::<u64>("orders-7")
            .await
            .unwrap()
            .unwrap()
            .state(),
        11
    );
    assert_eq!(
        store.load::<String>("orders-7").await.unwrap_err().code(),
        ErrorCode::Validation
    );
    store.delete("orders-7").await.unwrap();
    assert!(store.load::<u64>("orders-7").await.unwrap().is_none());
    store
        .save(Snapshot::new("orders-7", 13_u64, 6))
        .await
        .unwrap();
    assert_eq!(
        *store
            .load::<u64>("orders-7")
            .await
            .unwrap()
            .unwrap()
            .state(),
        13
    );
}

#[tokio::test]
async fn nats_idempotency_claims_exclusively_retries_failures_and_caches_results() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let store = NatsIdempotency::connect(&server, format!("CATGA_IDEMP_{}", std::process::id()))
        .await
        .unwrap();
    assert!(store.try_claim("create:7").await.unwrap());
    assert!(!store.try_claim("create:7").await.unwrap());
    store.fail("create:7").await.unwrap();
    assert!(store.try_claim("create:7").await.unwrap());
    store
        .complete("create:7", Some(Arc::from([9_u8, 8])))
        .await
        .unwrap();
    assert_eq!(
        store.state("create:7").await.unwrap(),
        Some(ProcessingState::Completed)
    );
    assert_eq!(
        store.result("create:7").await.unwrap().as_deref(),
        Some(&[9, 8][..])
    );
    assert!(!store.try_claim("create:7").await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nats_idempotency_concurrent_claims_have_exactly_one_owner() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let store = Arc::new(
        NatsIdempotency::connect(&server, format!("CATGA_IDEMP_RACE_{}", std::process::id()))
            .await
            .unwrap(),
    );
    let mut claims = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let store = Arc::clone(&store);
        claims.spawn(async move { store.try_claim("create:race").await.unwrap() });
    }
    let mut owners = 0;
    while let Some(claim) = claims.join_next().await {
        owners += usize::from(claim.unwrap());
    }
    assert_eq!(owners, 1);
}
