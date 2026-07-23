//! Redis Streams integration tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_core::{Envelope, LeaseStore, MessageMetadata, MessageTransport};
use catga_redis::{RedisConfig, RedisLeases, RedisTransport};
use redis::AsyncCommands;

static TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

fn redis_config() -> Option<RedisConfig> {
    let server = std::env::var("CATGA_REDIS_URL").ok()?;
    let suffix = format!(
        "{}:{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    Some(RedisConfig {
        server: server.into(),
        stream: format!("catga:{suffix}").into(),
        group: format!("catga:{suffix}").into(),
        consumer: format!("consumer:{suffix}").into(),
    })
}

#[tokio::test]
async fn redis_leases_compare_owner_atomically() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let leases = RedisLeases::connect(config.server, format!("{}:lease", config.stream))
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
async fn redis_stream_round_trip_and_ack() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let transport = RedisTransport::connect(config).await.unwrap();
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
async fn redis_idle_receive_does_not_block_publish() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let transport = Arc::new(RedisTransport::connect(config).await.unwrap());
    let receiver = Arc::clone(&transport);
    let receive = tokio::spawn(async move { receiver.receive().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![3, 4],
            MessageMetadata::new(1, None),
        ))
        .await
        .expect("publish succeeds while receive blocks");

    let delivery = tokio::time::timeout(Duration::from_secs(1), receive)
        .await
        .expect("receive completes after publish")
        .expect("receive task does not panic")
        .expect("receive succeeds");
    assert_eq!(delivery.envelope().payload(), [3, 4]);
    transport.ack(delivery).await.unwrap();
}

#[tokio::test]
async fn redis_dropped_delivery_is_recovered_from_its_pending_entries() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let transport = RedisTransport::connect(config).await.unwrap();
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![5, 6],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();

    let delivery = transport.receive().await.unwrap();
    drop(delivery);

    let recovered = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .expect("pending delivery is recovered")
        .expect("receive succeeds");
    assert_eq!(recovered.envelope().payload(), [5, 6]);
    transport.ack(recovered).await.unwrap();
}

#[tokio::test]
async fn redis_live_delivery_does_not_block_the_next_stream_entry() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let transport = RedisTransport::connect(config).await.unwrap();
    for payload in [vec![9], vec![10]] {
        transport
            .publish(Envelope::new(
                1,
                "order.created",
                payload,
                MessageMetadata::new(1, None),
            ))
            .await
            .unwrap();
    }

    let first = transport.receive().await.unwrap();
    assert_eq!(first.envelope().payload(), [9]);
    let second = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .expect("second stream entry is delivered")
        .expect("receive succeeds");
    assert_eq!(second.envelope().payload(), [10]);
    transport.ack(first).await.unwrap();
    transport.ack(second).await.unwrap();
}

#[tokio::test]
async fn redis_decode_error_does_not_block_pending_entry_recovery() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let server = config.server.to_string();
    let stream = config.stream.to_string();
    let transport = RedisTransport::connect(config).await.unwrap();
    let client = redis::Client::open(server).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: Option<String> = connection
        .xadd(&stream, "*", &[("payload", vec![255])])
        .await
        .unwrap();

    assert!(transport.receive().await.is_err());
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![11],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();

    let retry = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .expect("pending entry recovery completes");
    assert!(retry.is_err());
}

#[tokio::test]
async fn redis_ack_errors_when_the_pending_entry_was_already_acknowledged() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let server = config.server.to_string();
    let stream = config.stream.to_string();
    let group = config.group.to_string();
    let transport = RedisTransport::connect(config).await.unwrap();
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![7, 8],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();
    let delivery = transport.receive().await.unwrap();

    let client = redis::Client::open(server).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let pending: Vec<(String, String, usize, usize)> = redis::cmd("XPENDING")
        .arg(&stream)
        .arg(&group)
        .arg("-")
        .arg("+")
        .arg(1)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    let acknowledged: usize = connection
        .xack(&stream, &group, &[pending[0].0.as_str()])
        .await
        .unwrap();
    assert_eq!(acknowledged, 1);

    let error = transport.ack(delivery).await.unwrap_err();
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
}
