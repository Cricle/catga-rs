//! NATS JetStream integration tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{Envelope, ErrorCode, EventStore, LeaseStore, MessageMetadata, MessageTransport};
use catga_nats::{NatsConfig, NatsEventStore, NatsLeases, NatsTransport};

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
