//! Redis Streams integration tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use catga_core::{
    DeadLetter, DeadLetterStore, Envelope, ErrorCode, EventStore, InboxStore, LeaseStore,
    MessageMetadata, MessageTransport, OutboxMessage, OutboxStore, PersistentSubscription,
    ProjectionCheckpoint, ProjectionCheckpointStore, Snapshot, SnapshotStore,
    SubscriptionCheckpoint, SubscriptionStore,
};
use catga_redis::{
    RedisConfig, RedisDeadLetters, RedisEventStore, RedisInbox, RedisLeases, RedisOutbox,
    RedisProjectionCheckpoints, RedisSnapshotStore, RedisSubscriptions, RedisTransport,
};
use redis::AsyncCommands;

static TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

#[tokio::test]
async fn redis_subscriptions_persist_definitions_checkpoints_and_owner_leases() {
    let Some(config) = redis_config() else {
        return;
    };
    let store =
        RedisSubscriptions::connect(&config.server, format!("{}:subscriptions", config.stream))
            .await
            .unwrap();
    store
        .save(PersistentSubscription::new("orders", "order-*").with_event_types(["created"]))
        .await
        .unwrap();
    assert_eq!(
        store
            .load("orders")
            .await
            .unwrap()
            .unwrap()
            .event_types()
            .iter()
            .map(|value| value.as_ref())
            .collect::<Vec<_>>(),
        ["created"]
    );
    store
        .save_checkpoint(SubscriptionCheckpoint::new("orders", "order-7", 4))
        .await
        .unwrap();
    assert_eq!(
        store
            .load_checkpoint("orders", "order-7")
            .await
            .unwrap()
            .unwrap()
            .version(),
        4
    );
    assert!(store.try_acquire("orders", "worker-a").await.unwrap());
    assert!(!store.try_acquire("orders", "worker-b").await.unwrap());
    store.release("orders", "worker-b").await.unwrap();
    assert!(!store.try_acquire("orders", "worker-b").await.unwrap());
    store.release("orders", "worker-a").await.unwrap();
    assert!(store.try_acquire("orders", "worker-b").await.unwrap());
}

#[tokio::test]
async fn redis_dead_letters_preserve_queue_order_and_envelopes() {
    let Some(config) = redis_config() else {
        return;
    };
    let letters =
        RedisDeadLetters::connect(&config.server, format!("{}:dead-letters", config.stream))
            .await
            .unwrap();
    for id in [1_u64, 2] {
        letters
            .enqueue(DeadLetter::new(
                Envelope::new(
                    id,
                    "order.failed",
                    vec![id as u8],
                    MessageMetadata::new(id, None),
                ),
                "failed",
                3,
            ))
            .await
            .unwrap();
    }
    let letters = letters.list(1).await.unwrap();
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].envelope().id(), 1);
}

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
async fn redis_event_store_appends_atomically_and_reads_versioned_history() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let store = Arc::new(
        RedisEventStore::connect(&config.server, format!("{}:events", config.stream))
            .await
            .unwrap(),
    );
    let first = Envelope::new(1, "order.created", vec![1], MessageMetadata::new(1, None));
    let second = Envelope::new(1, "order.paid", vec![2], MessageMetadata::new(2, Some(1)));

    assert_eq!(
        store
            .append("orders-7", vec![first], Some(-1))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .append("orders-7", vec![second], Some(0))
            .await
            .unwrap(),
        1
    );
    let conflict = store
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
        .unwrap_err();
    assert_eq!(conflict.code(), ErrorCode::Conflict);

    let stream = store.read("orders-7", 1, 1).await.unwrap();
    assert_eq!(stream.version(), 1);
    assert_eq!(stream.events().len(), 1);
    assert_eq!(stream.events()[0].version(), 1);
    assert_eq!(stream.events()[0].envelope().payload(), [2]);
    assert_eq!(store.version("orders-7").await.unwrap(), 1);
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
    let history = store.version_history("orders-7").await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].event_type(), "order.paid");
    assert_eq!(store.stream_ids().await.unwrap(), ["orders-7"]);

    let first_writer = Arc::clone(&store);
    let second_writer = Arc::clone(&store);
    let (first_result, second_result) = tokio::join!(
        first_writer.append(
            "orders-7",
            vec![Envelope::new(
                1,
                "order.shipped",
                vec![4],
                MessageMetadata::new(4, None),
            )],
            Some(1),
        ),
        second_writer.append(
            "orders-7",
            vec![Envelope::new(
                1,
                "order.refunded",
                vec![5],
                MessageMetadata::new(5, None),
            )],
            Some(1),
        ),
    );
    let outcomes = [first_result, second_result];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter_map(|result| result.as_ref().err())
            .map(|error| error.code())
            .collect::<Vec<_>>(),
        [ErrorCode::Conflict]
    );
    assert_eq!(store.version("orders-7").await.unwrap(), 2);
}

#[tokio::test]
async fn redis_snapshots_round_trip_and_reject_stale_writers_atomically() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let store = Arc::new(
        RedisSnapshotStore::<u64>::connect(&config.server, format!("{}:snapshots", config.stream))
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
}

#[tokio::test]
async fn redis_projection_checkpoints_are_isolated_by_projection_and_stream() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let checkpoints = RedisProjectionCheckpoints::connect(
        &config.server,
        format!("{}:projection-checkpoints", config.stream),
    )
    .await
    .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("orders", "order-1", 4))
        .await
        .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("orders", "order-2", 9))
        .await
        .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("audit", "order-1", 2))
        .await
        .unwrap();
    assert_eq!(
        checkpoints
            .load("orders", "order-1")
            .await
            .unwrap()
            .unwrap()
            .version(),
        4
    );
    checkpoints.delete("orders", "order-1").await.unwrap();
    assert!(
        checkpoints
            .load("orders", "order-1")
            .await
            .unwrap()
            .is_none()
    );
    checkpoints.delete_all("orders").await.unwrap();
    assert!(
        checkpoints
            .load("orders", "order-2")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        checkpoints
            .load("audit", "order-1")
            .await
            .unwrap()
            .unwrap()
            .version(),
        2
    );
}

#[tokio::test]
async fn redis_inbox_claims_exclusively_retries_failures_and_caches_results() {
    let Some(config) = redis_config() else {
        eprintln!("skipping Redis integration test: CATGA_REDIS_URL is unset");
        return;
    };
    let inbox = RedisInbox::connect(&config.server, format!("{}:inbox", config.stream))
        .await
        .unwrap();
    assert!(inbox.try_claim(7).await.unwrap());
    assert!(!inbox.try_claim(7).await.unwrap());
    inbox.fail(7).await.unwrap();
    assert!(inbox.try_claim(7).await.unwrap());
    inbox.complete(7, Some(Arc::from([1_u8, 2]))).await.unwrap();
    assert_eq!(
        inbox.state(7).await.unwrap(),
        Some(catga_core::ProcessingState::Completed)
    );
    assert_eq!(inbox.result(7).await.unwrap().as_deref(), Some(&[1, 2][..]));
}

#[tokio::test]
async fn redis_outbox_claims_and_acknowledges_only_the_current_owner() {
    let Some(config) = redis_config() else {
        return;
    };
    let outbox = RedisOutbox::connect(&config.server, format!("{}:outbox", config.stream))
        .await
        .unwrap();
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            7,
            "order.created",
            vec![1],
            MessageMetadata::new(7, None),
        )))
        .await
        .unwrap();
    let claimed = outbox.claim("worker-a", 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(outbox.claim("worker-b", 1).await.unwrap().is_empty());
    outbox.ack("worker-b", 7).await.unwrap();
    assert!(outbox.claim("worker-b", 1).await.unwrap().is_empty());
    outbox.ack("worker-a", 7).await.unwrap();
    assert!(outbox.claim("worker-b", 1).await.unwrap().is_empty());
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
