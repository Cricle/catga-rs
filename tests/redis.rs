//! Redis Streams integration tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use catga_codec_memorypack::MemoryPackCodec;
use catga_core::flow::{
    DslStepProgress, DslStepProgressStore, DueFlowScheduler, FlowContinuation, FlowScheduler,
    FlowState, FlowStore, SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{
    AsyncInitializable, CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, DeadLetter,
    DeadLetterStore, Destination, DestinationTransport, EnhancedSnapshotStore, Envelope,
    EnvelopeCodec, ErrorCode, EventStore, HealthCheckable, IdempotencyStore, InboxStore,
    LeaseStore, MAX_OUTBOX_CLAIM_LIMIT, MAX_RETENTION_CLEANUP_LIMIT, MessageMetadata,
    MessageTransport, OutboxMessage, OutboxState, OutboxStore, PersistentSubscription,
    ProcessingState, ProjectionCheckpoint, ProjectionCheckpointStore, QualityOfService, Snapshot,
    SnapshotStore, Stoppable, SubscriptionCheckpoint, SubscriptionStore, Waitable,
};
use catga_redis::{
    MAX_REDIS_PENDING_RECLAIM_SCANS, RedisConfig, RedisDeadLetters, RedisDslStepProgress,
    RedisEnhancedSnapshots, RedisEventStore, RedisFlowScheduler, RedisFlows, RedisIdempotency,
    RedisInbox, RedisLeases, RedisOutbox, RedisPendingReclaimOptions, RedisProjectionCheckpoints,
    RedisPubSubConfig, RedisPubSubTransport, RedisRequest, RedisRequestClient, RedisRequestServer,
    RedisSnapshotStore, RedisSubscriptions, RedisSuspendedFlows, RedisTransport,
};
use redis::AsyncCommands;
use tokio_util::sync::CancellationToken;

#[path = "flow/dsl_progress_contract.rs"]
mod dsl_progress_contract;
#[path = "flow/timeout_store_contract.rs"]
mod timeout_store_contract;

static TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

const REDIS_BROKER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);

/// Receives one Redis delivery within the E2E test's bounded broker-response budget.
async fn redis_delivery(
    transport: &impl MessageTransport,
    context: &str,
) -> CatgaResult<catga_core::Delivery> {
    tokio::time::timeout(REDIS_BROKER_RESPONSE_TIMEOUT, transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, format!("{context} timed out")))?
}

/// Receives one Redis RPC request within the E2E test's bounded broker-response budget.
async fn redis_request(
    server: &mut RedisRequestServer,
    context: &str,
) -> CatgaResult<RedisRequest> {
    tokio::time::timeout(REDIS_BROKER_RESPONSE_TIMEOUT, server.next())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, format!("{context} timed out")))?
}

/// Redis Pub/Sub delivers ephemeral broadcast messages without a durable acknowledgement token.
#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_pubsub_transport_broadcasts_without_acknowledgements() -> CatgaResult<()> {
    let config = redis_config();
    let transport = RedisPubSubTransport::connect(RedisPubSubConfig {
        server: config.server,
        channel: format!(
            "{}:pubsub:{}",
            config.stream,
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .into(),
    })
    .await?;

    transport
        .publish(Envelope::new(
            91,
            "order.updated",
            vec![9, 1],
            MessageMetadata::new(91, None),
        ))
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "Redis Pub/Sub delivery timed out"))??;
    assert_eq!(delivery.envelope().payload(), [9, 1]);
    assert_eq!(delivery.attempts(), 1);
    transport.ack(delivery).await?;
    assert_eq!(transport.pending_operations(), 0);

    transport.stop_accepting();
    assert!(matches!(
        transport
            .publish(Envelope::new(
                92,
                "order.updated",
                Vec::new(),
                MessageMetadata::new(92, None),
            ))
            .await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    Ok(())
}

/// Exactly-once Pub/Sub publications use Redis atomically to suppress duplicate message IDs.
#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_pubsub_transport_deduplicates_exactly_once_publications() -> CatgaResult<()> {
    let config = redis_config();
    let transport = RedisPubSubTransport::connect(RedisPubSubConfig {
        server: config.server,
        channel: format!(
            "{}:pubsub:dedup:{}",
            config.stream,
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .into(),
    })
    .await?;
    let envelope = Envelope::new(
        93,
        "order.paid",
        vec![9, 3],
        MessageMetadata::new(93, None).with_quality_of_service(QualityOfService::ExactlyOnce),
    );

    transport.publish(envelope.clone()).await?;
    let first = redis_delivery(&transport, "Redis Pub/Sub first delivery").await?;
    assert_eq!(first.envelope().id(), 93);
    transport.publish(envelope).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), transport.receive())
            .await
            .is_err()
    );
    Ok(())
}

/// Each Pub/Sub receiver independently suppresses repeated exactly-once envelopes.
#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_pubsub_transport_deduplicates_exactly_once_receptions() -> CatgaResult<()> {
    let config = redis_config();
    let server = config.server.to_string();
    let channel = format!(
        "{}:pubsub:receive-dedup:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let transport = RedisPubSubTransport::connect(RedisPubSubConfig {
        server: server.clone().into(),
        channel: channel.clone().into(),
    })
    .await?;
    let envelope = Envelope::new(
        94,
        "order.refunded",
        vec![9, 4],
        MessageMetadata::new(94, None).with_quality_of_service(QualityOfService::ExactlyOnce),
    );
    let payload = MemoryPackCodec::default().encode(&envelope)?;
    let client = redis::Client::open(server)
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let _: usize = connection
        .publish(&channel, payload.clone())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let first = redis_delivery(&transport, "Redis Pub/Sub deduplicated delivery").await?;
    assert_eq!(first.envelope().id(), 94);
    let _: usize = connection
        .publish(&channel, payload)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), transport.receive())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_pubsub_request_reply_uses_a_private_reply_channel() -> CatgaResult<()> {
    let config = redis_config();
    let destination = format!(
        "{}:rpc:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut server = RedisRequestServer::connect(&config.server, &destination)
        .await
        .map_err(|error| {
            CatgaError::new(
                error.code(),
                format!("Redis RPC server setup failed: {error:?}"),
            )
        })?;
    let responder = tokio::spawn(async move {
        let request = redis_request(&mut server, "Redis RPC server request").await?;
        request
            .respond(Envelope::new(
                42,
                "reply",
                vec![9],
                MessageMetadata::new(42, Some(42)),
            ))
            .await
    });
    let client = RedisRequestClient::connect(&config.server)?;
    let response = client
        .request_to(
            &destination,
            Envelope::new(42, "request", vec![1], MessageMetadata::new(42, Some(42))),
            Duration::from_secs(2),
        )
        .await?;
    responder.await.map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("Redis RPC responder task failed: {error}"),
        )
    })??;
    assert_eq!(response.payload(), [9]);
    Ok(())
}

#[tokio::test]
async fn redis_request_client_reports_destination_and_timeout_validation_separately() {
    let client = RedisRequestClient::connect("redis://127.0.0.1:6379").unwrap();
    let request = Envelope::new(
        1,
        "inventory.lookup",
        vec![],
        MessageMetadata::new(1, Some(1)),
    );

    let destination_error = client
        .request_to("", request.clone(), Duration::from_secs(1))
        .await
        .expect_err("an empty destination must fail before opening a connection");
    let timeout_error = client
        .request_to("inventory", request, Duration::ZERO)
        .await
        .expect_err("a zero timeout must fail before opening a connection");

    assert_eq!(destination_error.code(), ErrorCode::Validation);
    assert_eq!(
        destination_error.message(),
        "Redis request destination must not be empty"
    );
    assert_eq!(timeout_error.code(), ErrorCode::Validation);
    assert_eq!(
        timeout_error.message(),
        "Redis request timeout must be greater than zero"
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_subscriptions_persist_definitions_checkpoints_and_owner_leases() {
    let config = redis_config();
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_dead_letters_preserve_queue_order_and_envelopes() {
    let config = redis_config();
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

fn redis_config() -> RedisConfig {
    let server = std::env::var("CATGA_REDIS_URL")
        .expect("CATGA_REDIS_URL must be set for ignored Redis tests");
    let suffix = format!(
        "{}:{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    RedisConfig {
        server: server.into(),
        stream: format!("catga:{suffix}").into(),
        group: format!("catga:{suffix}").into(),
        consumer: format!("consumer:{suffix}").into(),
    }
}

#[test]
fn redis_pending_reclaim_options_reject_zero_idle() {
    let error = RedisPendingReclaimOptions::new(Duration::ZERO, 1)
        .expect_err("zero reclaim idle duration must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn redis_pending_reclaim_options_bound_scan_work() {
    for max_scans in [0, MAX_REDIS_PENDING_RECLAIM_SCANS + 1] {
        let error = RedisPendingReclaimOptions::new(Duration::from_millis(1), max_scans)
            .expect_err("an unbounded reclaim scan limit must be rejected");
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_transport_reclaim_fences_a_stale_acknowledgement() -> CatgaResult<()> {
    let first_config = redis_config();
    let second_config = RedisConfig {
        server: first_config.server.clone(),
        stream: first_config.stream.clone(),
        group: first_config.group.clone(),
        consumer: format!(
            "reclaimer:{}",
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .into(),
    };
    let reclaim = RedisPendingReclaimOptions::new(Duration::from_millis(1), 1)?;
    let first = RedisTransport::connect_with_reclaim_options(first_config, reclaim.clone()).await?;
    let second = RedisTransport::connect_with_reclaim_options(second_config, reclaim).await?;
    let envelope = Envelope::new(
        302,
        "order.reclaim",
        vec![3, 0, 2],
        MessageMetadata::new(302, None),
    );

    first.publish(envelope.clone()).await?;
    let stale_delivery = redis_delivery(&first, "Redis stale delivery").await?;
    assert_eq!(stale_delivery.envelope().id(), envelope.id());
    tokio::time::sleep(Duration::from_millis(20)).await;

    let reclaimed = tokio::time::timeout(Duration::from_secs(1), second.receive())
        .await
        .map_err(|_| {
            CatgaError::new(ErrorCode::Timeout, "Redis idle delivery was not reclaimed")
        })??;
    assert_eq!(reclaimed.envelope().id(), envelope.id());
    assert!(reclaimed.attempts() >= 2);
    let stale_ack = first
        .ack(stale_delivery)
        .await
        .expect_err("a former Redis consumer must not acknowledge a reclaimed delivery");
    assert_eq!(stale_ack.code(), ErrorCode::Transient);
    second.ack(reclaimed).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_transport_reclaims_idle_delivery_with_multiple_receivers() -> CatgaResult<()> {
    let first_config = redis_config();
    let second_config = RedisConfig {
        server: first_config.server.clone(),
        stream: first_config.stream.clone(),
        group: first_config.group.clone(),
        consumer: format!(
            "concurrent-reclaimer:{}",
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )
        .into(),
    };
    let reclaim = RedisPendingReclaimOptions::new(Duration::from_millis(100), 1)?;
    let first = RedisTransport::connect_with_reclaim_options(first_config, reclaim.clone()).await?;
    let second =
        Arc::new(RedisTransport::connect_with_reclaim_options(second_config, reclaim).await?);
    let envelope = Envelope::new(
        303,
        "order.concurrent-reclaim",
        vec![3, 0, 3],
        MessageMetadata::new(303, None),
    );

    first.publish(envelope.clone()).await?;
    let abandoned = redis_delivery(&first, "Redis abandoned delivery").await?;
    // Make the pending entry reclaimable before either receiver performs its first recovery
    // scan. Starting them earlier lets both scans observe a non-idle entry and enter Redis's
    // one-second blocking read, which makes this concurrency assertion depend on a poll cycle.
    tokio::time::sleep(Duration::from_millis(120)).await;
    drop(abandoned);

    let mut waiters = tokio::task::JoinSet::new();
    {
        let transport = Arc::clone(&second);
        waiters.spawn(async move {
            redis_delivery(transport.as_ref(), "first concurrent Redis receiver").await
        });
    }
    {
        let transport = Arc::clone(&second);
        waiters.spawn(async move {
            redis_delivery(transport.as_ref(), "second concurrent Redis receiver").await
        });
    }

    let reclaimed = tokio::time::timeout(Duration::from_secs(1), waiters.join_next())
        .await
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Timeout,
                "no concurrent Redis receiver reclaimed the idle delivery",
            )
        })?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "all concurrent Redis receiver tasks ended before returning a result",
            )
        })?
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("concurrent Redis receiver task failed: {error}"),
            )
        })??;
    waiters.abort_all();
    while waiters.join_next().await.is_some() {}
    assert_eq!(reclaimed.envelope().id(), envelope.id());
    second.ack(reclaimed).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_transport_does_not_redeliver_a_local_in_flight_delivery() -> CatgaResult<()> {
    let transport = RedisTransport::connect(redis_config()).await?;
    let first_envelope = Envelope::new(
        304,
        "order.local-in-flight",
        vec![3, 0, 4],
        MessageMetadata::new(304, None),
    );
    let second_envelope = Envelope::new(
        305,
        "order.next",
        vec![3, 0, 5],
        MessageMetadata::new(305, None),
    );

    transport.publish(first_envelope.clone()).await?;
    transport.publish(second_envelope.clone()).await?;
    let first = redis_delivery(&transport, "Redis first delivery").await?;
    let second = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "second Redis delivery timed out"))??;

    assert_eq!(first.envelope().id(), first_envelope.id());
    assert_eq!(second.envelope().id(), second_envelope.id());
    transport.ack(first).await?;
    transport.ack(second).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_destination_uses_an_explicit_stream_queue() -> CatgaResult<()> {
    let config = redis_config();
    let transport = RedisTransport::connect(config).await?;
    let destination = Destination::parse(format!(
        "orders:{}",
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))?;

    transport
        .send_to(
            &destination,
            Envelope::new(
                301,
                "order.queued",
                vec![3, 0, 1],
                MessageMetadata::new(301, None),
            ),
        )
        .await?;
    let delivery = transport.receive_from(&destination).await?;
    assert_eq!(delivery.envelope().id(), 301);
    transport.ack(delivery).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_suspended_flows_preserve_wait_results_and_claims() {
    let config = redis_config();
    let store = RedisSuspendedFlows::connect(&config.server, format!("{}:flows", config.stream))
        .await
        .unwrap();
    let continuation = waiting_continuation("redis-flow");
    assert!(store.create(continuation).await.unwrap());
    assert!(
        store
            .record_wait_success("redis-flow", 0, "child-a", b"ok".to_vec())
            .await
            .unwrap()
    );
    assert!(
        store
            .record_wait_failure(
                "redis-flow",
                0,
                "child-b",
                catga_core::CatgaError::new(ErrorCode::Transient, "unavailable"),
            )
            .await
            .unwrap()
    );

    let current = store.get("redis-flow").await.unwrap().unwrap();
    assert_eq!(current.wait().unwrap().completed_count(), 2);
    assert!(store.heartbeat("redis-flow", "node-a", 0).await.unwrap());
    let stale_claim = current.clone().with_state(
        current
            .state()
            .clone()
            .claimed_by("node-b")
            .next_version()
            .unwrap(),
    );
    assert!(!store.claim(&current, stale_claim).await.unwrap());

    let current = store.get("redis-flow").await.unwrap().unwrap();
    let next = current.clone().with_state(
        current
            .state()
            .clone()
            .claimed_by("node-b")
            .next_version()
            .unwrap(),
    );
    assert!(store.claim(&current, next.clone()).await.unwrap());
    assert!(
        store
            .heartbeat("redis-flow", "node-b", next.state().version())
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_suspended_flows_look_up_indexed_wait_correlations() -> CatgaResult<()> {
    let config = redis_config();
    let prefix = format!(
        "{}:wait-correlation:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let store = RedisSuspendedFlows::connect(&config.server, prefix).await?;
    let correlation = "redis-wait-correlation";
    let waiting = waiting_continuation_with_correlation("redis-correlation-one", correlation);
    assert!(store.create(waiting.clone()).await?);

    let found = store
        .get_by_wait_correlation(correlation)
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "Redis indexed wait was not found"))?;
    assert_eq!(found.state().id(), waiting.state().id());
    assert!(
        store
            .get_by_wait_correlation("redis-wait-correlation-missing")
            .await?
            .is_none()
    );

    let ready = waiting
        .clone()
        .ready()
        .with_state(waiting.state().clone().next_version()?);
    assert!(store.update(0, ready).await?);
    assert!(store.get_by_wait_correlation(correlation).await?.is_none());

    let shared = "redis-wait-correlation-shared";
    for flow_id in ["redis-correlation-two", "redis-correlation-three"] {
        assert!(
            store
                .create(waiting_continuation_with_correlation(flow_id, shared))
                .await?
        );
    }
    assert_eq!(
        store
            .get_by_wait_correlation(shared)
            .await
            .expect_err("ambiguous Redis correlation must not select a continuation")
            .code(),
        ErrorCode::Conflict
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_suspended_flows_page_bounded_timeout_queries() -> CatgaResult<()> {
    let config = redis_config();
    let store = RedisSuspendedFlows::connect(
        &config.server,
        format!("{}:timeout-contract", config.stream),
    )
    .await?;
    timeout_store_contract::run_timeout_store_contract(&store, "redis-timeout", false).await
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_suspended_flow_query_ignores_auxiliary_hashes() -> CatgaResult<()> {
    let config = redis_config();
    let prefix = format!(
        "{}:query-index:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let store = RedisSuspendedFlows::connect(&config.server, prefix.clone()).await?;
    assert!(
        store
            .create(waiting_continuation("redis-query-index"))
            .await?
    );

    let client = redis::Client::open(config.server.as_ref()).map_err(|error| {
        CatgaError::new(ErrorCode::Transient, "connect Redis query auxiliary key")
            .with_details(error.to_string())
    })?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Transient,
                "open Redis query auxiliary connection",
            )
            .with_details(error.to_string())
        })?;
    let _: () = connection
        .hset(format!("{prefix}:auxiliary"), "field", "value")
        .await
        .map_err(|error| {
            CatgaError::new(ErrorCode::Transient, "write Redis query auxiliary hash")
                .with_details(error.to_string())
        })?;

    let summaries = store
        .query(&catga_core::flow::FlowQuery::new(2, 2)?)
        .await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id(), "redis-query-index");
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_flow_scheduler_claims_recovers_and_releases_target_indexes() {
    let config = redis_config();
    let scheduler =
        RedisFlowScheduler::connect(&config.server, format!("{}:scheduler", config.stream))
            .await
            .unwrap();
    let now = SystemTime::now();
    let id = scheduler
        .schedule_resume("redis-payment", "charge", now)
        .await
        .unwrap();
    assert_eq!(
        scheduler
            .schedule_resume("redis-payment", "charge", now + Duration::from_secs(60))
            .await
            .unwrap(),
        id,
        "a duplicate target keeps its original schedule identity and due time"
    );

    assert_eq!(
        scheduler
            .claim_due("worker-a", now, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        scheduler
            .claim_due("worker-b", now, Duration::from_secs(1), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!scheduler.ack_due("worker-b", &id).await.unwrap());
    assert_eq!(
        scheduler
            .claim_due(
                "worker-b",
                now + Duration::from_secs(2),
                Duration::from_secs(1),
                1
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(scheduler.ack_due("worker-b", &id).await.unwrap());
    assert!(
        scheduler
            .schedule_resume("redis-payment", "charge", now)
            .await
            .is_ok()
    );

    let error = scheduler
        .claim_due("worker", now, Duration::ZERO, 1)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_flow_scheduler_does_not_renew_or_keep_expired_leases() -> CatgaResult<()> {
    let config = redis_config();
    let scheduler = RedisFlowScheduler::connect(
        &config.server,
        format!(
            "{}:expired-schedule:{}",
            config.stream,
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
    )
    .await?;
    let epoch = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let schedule_id = scheduler
        .schedule_resume("expired-schedule-flow", "resume", epoch)
        .await?;
    assert_eq!(
        scheduler
            .claim_due("worker-a", epoch, Duration::from_secs(1), 1)
            .await?
            .len(),
        1
    );
    assert!(
        !scheduler
            .renew_due(
                "worker-a",
                &schedule_id,
                epoch + Duration::from_secs(2),
                Duration::from_secs(30),
            )
            .await?,
        "an expired owner must not revive its lease"
    );
    assert!(
        scheduler.cancel_resume(&schedule_id).await?,
        "cancelling an expired lease must remove the schedule"
    );
    Ok(())
}

fn waiting_continuation(id: &str) -> FlowContinuation {
    waiting_continuation_with_correlation(id, format!("{id}-wait").as_str())
}

fn waiting_continuation_with_correlation(id: &str, correlation_id: &str) -> FlowContinuation {
    FlowContinuation::waiting(
        FlowState::new(id, "payment", b"input".to_vec(), "node-a"),
        "charge",
        WaitCondition::new(
            correlation_id,
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    )
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_leases_compare_owner_atomically() {
    let config = redis_config();
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_event_store_appends_atomically_and_reads_versioned_history() {
    let config = redis_config();
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

    let stream = store.read_page("orders-7", 1, 1).await.unwrap();
    let stream = stream.stream();
    assert_eq!(stream.version(), 1);
    assert_eq!(stream.events().len(), 1);
    assert_eq!(stream.events()[0].version(), 1);
    assert_eq!(stream.events()[0].envelope().payload(), [2]);
    assert_eq!(store.version("orders-7").await.unwrap(), 1);
    assert_eq!(
        store
            .read_to_version_page("orders-7", 0, 0, 2)
            .await
            .unwrap()
            .stream()
            .events()
            .len(),
        1
    );
    assert_eq!(
        store
            .read_to_time_page("orders-7", 0, SystemTime::now(), 2)
            .await
            .unwrap()
            .stream()
            .events()
            .len(),
        2
    );
    let history = store.version_history_page("orders-7", 0, 2).await.unwrap();
    assert_eq!(history.entries().len(), 2);
    assert_eq!(history.entries()[1].event_type(), "order.paid");
    assert_eq!(
        store.stream_ids_page(None, 2).await.unwrap().ids(),
        ["orders-7"]
    );

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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_event_store_rejects_version_exhaustion_without_changing_the_stream() {
    let config = redis_config();
    let prefix = format!("{}:events", config.stream);
    let stream_id = "exhausted";
    let mut connection = redis::Client::open(config.server.as_ref())
        .expect("configured Redis URL is valid")
        .get_multiplexed_async_connection()
        .await
        .expect("Redis connection is available");
    let version_key = format!("{prefix}:version:{stream_id}");
    connection
        .set::<_, _, ()>(&version_key, i64::MAX - 1)
        .await
        .expect("test version is seeded");

    let store = RedisEventStore::connect(&config.server, prefix)
        .await
        .expect("event store connects");
    let error = store
        .append(
            stream_id,
            vec![
                Envelope::new(1, "order.created", vec![1], MessageMetadata::new(1, None)),
                Envelope::new(2, "order.paid", vec![2], MessageMetadata::new(2, None)),
            ],
            Some(i64::MAX - 1),
        )
        .await
        .expect_err("appending beyond i64::MAX must fail");

    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(
        connection
            .get::<_, i64>(&version_key)
            .await
            .expect("failed append leaves the version intact"),
        i64::MAX - 1
    );
    assert!(
        store
            .read_page(stream_id, 0, 1)
            .await
            .expect("failed append leaves no stream entries")
            .stream()
            .events()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_snapshots_round_trip_and_reject_stale_writers_atomically() {
    let config = redis_config();
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_enhanced_snapshots_preserve_versioned_history_and_bounded_cleanup() -> CatgaResult<()>
{
    let config = redis_config();
    let store = RedisEnhancedSnapshots::<u64>::connect(
        &config.server,
        format!("{}:enhanced-snapshots", config.stream),
    )
    .await?;
    store
        .save(Snapshot::new("orders-history", 10_u64, 1))
        .await?;
    store
        .save(Snapshot::new("orders-history", 30_u64, 3))
        .await?;

    let latest = store.load::<u64>("orders-history").await?.ok_or_else(|| {
        catga_core::CatgaError::new(ErrorCode::NotFound, "latest snapshot missing")
    })?;
    assert_eq!((*latest.state(), latest.version()), (30, 3));
    let at_two = store
        .load_at_version::<u64>("orders-history", 2)
        .await?
        .ok_or_else(|| {
            catga_core::CatgaError::new(ErrorCode::NotFound, "historical snapshot missing")
        })?;
    assert_eq!((*at_two.state(), at_two.version()), (10, 1));
    assert_eq!(
        store
            .history("orders-history")
            .await?
            .into_iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        [1, 3]
    );

    store.delete_before_version("orders-history", 3).await?;
    store.cleanup("orders-history", 0).await?;
    assert!(store.load::<u64>("orders-history").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_dsl_progress_uses_versioned_create_update_and_delete() -> CatgaResult<()> {
    let config = redis_config();
    let store =
        RedisDslStepProgress::connect(&config.server, format!("{}:dsl-progress", config.stream))
            .await?;
    let initial = DslStepProgress::new("order-flow", 4, vec![1_u8]);
    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);

    let next = initial.clone().next_version(vec![2_u8])?;
    assert!(!store.update(1, next.clone()).await?);
    assert!(store.update(0, next).await?);
    let current = store
        .get("order-flow", 4)
        .await?
        .ok_or_else(|| catga_core::CatgaError::new(ErrorCode::NotFound, "DSL progress missing"))?;
    assert_eq!(
        (current.version(), current.payload()),
        (1, &[2_u8] as &[u8])
    );
    assert!(store.delete("order-flow", 4).await?);
    assert!(!store.delete("order-flow", 4).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_flows_use_atomic_versions_and_claim_only_stale_matching_type() -> CatgaResult<()> {
    let config = redis_config();
    let store =
        RedisFlows::connect(&config.server, format!("{}:plain-flows", config.stream)).await?;
    let initial = FlowState::new("redis-flow", "payment", b"input".to_vec(), "node-a");

    assert!(store.create(initial.clone()).await?);
    assert!(!store.create(initial.clone()).await?);
    assert!(!store.update(1, initial.clone().next_version()?).await?);

    let next = initial.clone().next_version()?;
    assert!(store.update(0, next.clone()).await?);
    assert!(
        store
            .heartbeat("redis-flow", "node-a", next.version())
            .await?
    );
    assert!(
        store
            .try_claim("invoice", "node-b", Duration::from_secs(86_400))
            .await?
            .is_none()
    );

    let terminal = FlowState::new("redis-terminal", "payment", b"input".to_vec(), "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(terminal.clone()).await?);
    assert!(
        store
            .update(terminal.version(), terminal.done(1).next_version()?)
            .await?
    );

    let stale = FlowState::new("redis-stale", "payment", b"input".to_vec(), "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await?);
    let claimed = store
        .try_claim("payment", "node-b", Duration::from_secs(86_400))
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "stale Redis flow was not claimed"))?;
    assert_eq!(claimed.id(), "redis-stale");
    assert_eq!(claimed.owner(), Some("node-b"));
    assert_eq!(claimed.version(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_dsl_progress_runs_durable_recovery_contract() -> CatgaResult<()> {
    let config = redis_config();
    let store =
        RedisDslStepProgress::connect(&config.server, format!("{}:dsl-recovery", config.stream))
            .await?;

    dsl_progress_contract::run_durable_recovery_contracts(&store, "order-flow/recovery-contract")
        .await
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_projection_checkpoints_are_isolated_by_projection_and_stream() {
    let config = redis_config();
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_inbox_claims_exclusively_retries_failures_and_caches_results() {
    let config = redis_config();
    let inbox = RedisInbox::connect(&config.server, format!("{}:inbox", config.stream))
        .await
        .unwrap();
    let first = inbox
        .try_claim(7)
        .await
        .unwrap()
        .expect("inbox claim succeeds");
    assert!(inbox.try_claim(7).await.unwrap().is_none());
    inbox.fail(first).await.unwrap();
    let second = inbox
        .try_claim(7)
        .await
        .unwrap()
        .expect("inbox retry succeeds");
    inbox
        .complete(second, Some(Arc::from([1_u8, 2])))
        .await
        .unwrap();
    assert_eq!(
        inbox.state(7).await.unwrap(),
        Some(catga_core::ProcessingState::Completed)
    );
    assert_eq!(inbox.result(7).await.unwrap().as_deref(), Some(&[1, 2][..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_idempotency_atomically_claims_retries_failures_and_caches_results() {
    let config = redis_config();
    let prefix = format!(
        "{}:idempotency:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let store = Arc::new(
        RedisIdempotency::connect(&config.server, prefix.clone())
            .await
            .unwrap(),
    );

    let mut claims = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let store = Arc::clone(&store);
        claims.spawn(async move { store.try_claim("create:7").await.unwrap() });
    }
    let mut owners = 0;
    while let Some(claim) = claims.join_next().await {
        owners += usize::from(claim.unwrap());
    }
    assert_eq!(owners, 1);
    assert_eq!(
        store.state("create:7").await.unwrap(),
        Some(ProcessingState::Claimed)
    );

    store.fail("create:7").await.unwrap();
    assert_eq!(
        store.state("create:7").await.unwrap(),
        Some(ProcessingState::Failed)
    );
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

    let long_key = "x".repeat(16 * 1024);
    assert!(store.try_claim(&long_key).await.unwrap());
    let client = redis::Client::open(config.server.as_ref()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let persisted_keys: Vec<String> = connection.keys(format!("{prefix}:*")).await.unwrap();
    assert!(
        persisted_keys
            .iter()
            .all(|key| key.len() <= prefix.len() + 65)
    );

    assert!(store.try_claim("create:oversized").await.unwrap());
    let error = store
        .complete(
            "create:oversized",
            Some(Arc::from(vec![0_u8; 1024 * 1024 + 1])),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        store.state("create:oversized").await.unwrap(),
        Some(ProcessingState::Claimed)
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_idempotency_completed_records_receive_the_default_retention_ttl() {
    let config = redis_config();
    let prefix = format!(
        "{}:idempotency:retention:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let store = RedisIdempotency::connect(&config.server, prefix.clone())
        .await
        .unwrap();
    store.try_claim("completed").await.unwrap();
    store.complete("completed", None).await.unwrap();

    let client = redis::Client::open(config.server.as_ref()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let keys: Vec<String> = connection.keys(format!("{prefix}:*")).await.unwrap();
    assert_eq!(keys.len(), 1);
    let ttl: i64 = connection.pttl(&keys[0]).await.unwrap();
    let expected_ttl = i64::try_from(DEFAULT_IDEMPOTENCY_RETENTION.as_millis()).unwrap();
    let tolerance = i64::try_from(Duration::from_secs(5).as_millis()).unwrap();
    assert!(
        (expected_ttl.saturating_sub(tolerance)..=expected_ttl).contains(&ttl),
        "expected default TTL within {tolerance}ms of {expected_ttl}ms, got {ttl}ms"
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_idempotency_retention_expires_only_completed_records() {
    let config = redis_config();
    let prefix = format!(
        "{}:idempotency:retention:{}",
        config.stream,
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let store = RedisIdempotency::with_retention(&config.server, prefix, Duration::from_millis(50))
        .await
        .unwrap();

    assert!(store.try_claim("completed").await.unwrap());
    store.complete("completed", None).await.unwrap();
    assert!(store.try_claim("in-progress").await.unwrap());
    assert_eq!(store.cleanup_completed(1).await.unwrap(), 0);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(store.state("completed").await.unwrap(), None);
    assert!(store.try_claim("completed").await.unwrap());
    assert_eq!(
        store.state("in-progress").await.unwrap(),
        Some(ProcessingState::Claimed)
    );
}

#[tokio::test]
async fn redis_idempotency_rejects_zero_retention_before_connecting() {
    let result = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1",
        "idempotency:retention:validation",
        Duration::ZERO,
    )
    .await;
    assert!(matches!(
        result,
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
async fn redis_idempotency_rejects_retention_beyond_redis_millisecond_range_before_connecting() {
    let result = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1",
        "idempotency:retention:range-validation",
        Duration::from_millis((i64::MAX as u64) + 1),
    )
    .await;
    assert!(matches!(
        result,
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
async fn redis_idempotency_rejects_redis_absolute_expiry_overflow_before_connecting() {
    let result = RedisIdempotency::with_retention(
        "redis://127.0.0.1:1",
        "idempotency:retention:absolute-expiry-validation",
        Duration::from_millis(i64::MAX as u64),
    )
    .await;
    assert!(matches!(
        result,
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_inbox_reclaims_an_expired_processing_lease() {
    let config = redis_config();
    let inbox = RedisInbox::connect(&config.server, format!("{}:inbox-lease", config.stream))
        .await
        .unwrap();

    assert!(
        inbox
            .try_claim_for(91, Duration::from_millis(1))
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        inbox
            .try_claim_for(91, Duration::from_secs(1))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_inbox_fences_a_reclaimed_claim_owner() {
    let config = redis_config();
    let inbox = RedisInbox::connect(
        &config.server,
        format!(
            "{}:inbox-fence:{}",
            config.stream,
            TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
    )
    .await
    .unwrap();
    let first = inbox
        .try_claim_for(92, Duration::from_millis(1))
        .await
        .unwrap()
        .expect("first owner acquires the inbox claim");

    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = inbox
        .try_claim_for(92, Duration::from_secs(1))
        .await
        .unwrap()
        .expect("second owner reclaims the expired inbox claim");

    assert!(matches!(
        inbox.complete(first, Some(Arc::from([1_u8]))).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(matches!(
        inbox.fail(first).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    inbox
        .complete(second, Some(Arc::from([2_u8])))
        .await
        .unwrap();
    assert_eq!(inbox.result(92).await.unwrap().as_deref(), Some(&[2][..]));
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_inbox_removes_completed_records_with_a_bounded_scan() -> CatgaResult<()> {
    let config = redis_config();
    let inbox =
        RedisInbox::connect(&config.server, format!("{}:inbox-retention", config.stream)).await?;
    for message_id in [201_u64, 202] {
        let claim = inbox
            .try_claim(message_id)
            .await?
            .expect("inbox claim succeeds");
        inbox.complete(claim, None).await?;
    }

    assert!(matches!(
        inbox
            .cleanup_completed(Duration::ZERO, MAX_RETENTION_CLEANUP_LIMIT + 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(inbox.cleanup_completed(Duration::ZERO, 1).await?, 1);
    assert_eq!(inbox.cleanup_completed(Duration::ZERO, 1).await?, 1);
    assert_eq!(inbox.state(201).await?, None);
    assert_eq!(inbox.state(202).await?, None);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_claims_and_acknowledges_only_the_current_owner() {
    let config = redis_config();
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
    outbox
        .ack("worker-b", 7, claimed[0].claim_token().unwrap())
        .await
        .unwrap();
    assert!(outbox.claim("worker-b", 1).await.unwrap().is_empty());
    outbox
        .ack("worker-a", 7, claimed[0].claim_token().unwrap())
        .await
        .unwrap();
    assert!(outbox.claim("worker-b", 1).await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_reclaims_an_expired_claim_without_accepting_a_stale_ack() -> CatgaResult<()> {
    let config = redis_config();
    let outbox = RedisOutbox::connect(
        &config.server,
        format!("{}:outbox-claim-lease", config.stream),
    )
    .await?;
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            73,
            "order.created",
            vec![1],
            MessageMetadata::new(73, None),
        )))
        .await?;

    let original = outbox
        .claim_for("worker-a", 1, Duration::from_secs(1))
        .await?
        .pop()
        .unwrap();
    assert!(
        outbox
            .claim_for("worker-b", 1, Duration::from_secs(1))
            .await?
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let reclaimed = outbox
        .claim_for("worker-b", 1, Duration::from_secs(1))
        .await?
        .pop()
        .unwrap();
    outbox
        .ack("worker-a", 73, original.claim_token().unwrap())
        .await?;
    assert!(outbox.list_published(1).await?.is_empty());
    outbox
        .ack("worker-b", 73, reclaimed.claim_token().unwrap())
        .await?;
    assert_eq!(outbox.list_published(1).await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_reclaims_and_completes_a_legacy_owner_only_record() -> CatgaResult<()> {
    let config = redis_config();
    let prefix = format!("{}:outbox-legacy-owner", config.stream);
    let id = 74_u64;
    let payload = MemoryPackCodec::default().encode(&Envelope::new(
        id,
        "order.created",
        vec![1],
        MessageMetadata::new(id, None),
    ))?;
    let client = redis::Client::open(&*config.server)
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let key = format!("{prefix}:{id}");
    let _: () = redis::cmd("HSET")
        .arg(&key)
        .arg("payload")
        .arg(payload)
        .arg("owner")
        .arg("legacy-worker")
        .arg("retry_count")
        .arg(0)
        .arg("max_retries")
        .arg(3)
        .query_async(&mut connection)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let _: () = redis::cmd("ZADD")
        .arg(format!("{prefix}:pending"))
        .arg(0)
        .arg(id)
        .query_async(&mut connection)
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;

    let outbox = RedisOutbox::connect(&config.server, prefix).await?;
    let claimed = outbox.claim("worker-a", 1).await?.pop().unwrap();
    assert_eq!(claimed.id(), id);
    assert!(claimed.claim_token().is_some());
    outbox
        .ack("worker-a", id, claimed.claim_token().unwrap())
        .await?;
    assert_eq!(outbox.list_published(1).await?.len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_rotates_its_bounded_claim_scan_past_active_leases() -> CatgaResult<()> {
    let config = redis_config();
    let prefix = format!("{}:outbox-scan-fairness", config.stream);
    let payload = MemoryPackCodec::default().encode(&Envelope::new(
        1,
        "order.created",
        vec![1],
        MessageMetadata::new(1, None),
    ))?;
    let client = redis::Client::open(&*config.server)
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    for id in 1_u64..=5 {
        let _: () = redis::cmd("HSET")
            .arg(format!("{prefix}:{id}"))
            .arg("payload")
            .arg(payload.clone())
            .arg("owner")
            .arg("busy")
            .arg("claim_token")
            .arg(format!("busy-{id}"))
            .arg("state")
            .arg("claimed")
            .arg("claimed_until")
            .arg(u64::MAX)
            .arg("retry_count")
            .arg(0)
            .arg("max_retries")
            .arg(3)
            .query_async(&mut connection)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
        let _: () = redis::cmd("ZADD")
            .arg(format!("{prefix}:pending"))
            .arg(0)
            .arg(id)
            .query_async(&mut connection)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    }
    let outbox = RedisOutbox::connect(&config.server, prefix).await?;
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            6,
            "order.created",
            vec![6],
            MessageMetadata::new(6, None),
        )))
        .await?;
    assert!(outbox.claim("worker-a", 1).await?.is_empty());
    assert_eq!(outbox.claim("worker-a", 1).await?[0].id(), 6);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_retains_published_records_until_bounded_cleanup() -> CatgaResult<()> {
    let config = redis_config();
    let outbox = RedisOutbox::connect(
        &config.server,
        format!("{}:outbox-retention", config.stream),
    )
    .await?;
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            87,
            "order.published",
            vec![1],
            MessageMetadata::new(87, None),
        )))
        .await?;
    let claimed = outbox.claim("worker-a", 1).await?.pop().unwrap();

    outbox
        .ack("stale-worker", 87, claimed.claim_token().unwrap())
        .await?;
    assert!(outbox.list_published(1).await?.is_empty());
    outbox
        .ack("worker-a", 87, claimed.claim_token().unwrap())
        .await?;
    let published = outbox.list_published(1).await?;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id(), 87);
    assert_eq!(published[0].state(), OutboxState::Published);
    assert!(published[0].published_at_unix_ms().is_some());
    assert!(outbox.claim("worker-b", 1).await?.is_empty());

    assert!(matches!(
        outbox
            .cleanup_published(Duration::ZERO, MAX_RETENTION_CLEANUP_LIMIT + 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 1).await?, 1);
    assert!(outbox.list_published(1).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_stops_reclaiming_after_its_failure_limit() -> CatgaResult<()> {
    let config = redis_config();
    let outbox =
        RedisOutbox::connect(&config.server, format!("{}:outbox-failures", config.stream)).await?;
    let message = OutboxMessage::new(Envelope::new(
        29,
        "order.created",
        vec![1],
        MessageMetadata::new(29, None),
    ))
    .with_max_retries(3)?;
    outbox.enqueue(message).await?;

    for retry_count in 0..3 {
        let claimed = outbox.claim("worker-a", 1).await?;
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].retry_count(), retry_count);
        outbox
            .record_failure("worker-a", 29, claimed[0].claim_token().unwrap(), "offline")
            .await?;
    }

    assert!(outbox.claim("worker-b", 1).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_rejects_claims_above_the_shared_memory_budget() -> CatgaResult<()> {
    let config = redis_config();
    let outbox = RedisOutbox::connect(
        &config.server,
        format!("{}:outbox-claim-bound", config.stream),
    )
    .await?;

    assert!(matches!(
        outbox.claim("worker-a", MAX_OUTBOX_CLAIM_LIMIT + 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_outbox_does_not_claim_a_message_before_its_delivery_time() {
    let config = redis_config();
    let outbox = RedisOutbox::connect(
        &config.server,
        format!("{}:scheduled-outbox", config.stream),
    )
    .await
    .unwrap();
    let message = OutboxMessage::scheduled(
        Envelope::new(19, "order.ship", vec![1], MessageMetadata::new(19, None)),
        SystemTime::now() + Duration::from_secs(60),
    )
    .unwrap();

    outbox.enqueue(message).await.unwrap();
    assert!(outbox.claim("worker-a", 1).await.unwrap().is_empty());
    assert!(outbox.cancel(19).await.unwrap());
    assert!(!outbox.cancel(19).await.unwrap());
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_stream_round_trip_and_ack() {
    let config = redis_config();
    let transport = RedisTransport::connect(config).await.unwrap();
    transport.initialize().await.unwrap();
    assert!(transport.is_healthy());
    assert_eq!(transport.health_status(), Some("Redis transport is ready"));
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![1, 2],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();
    let delivery = redis_delivery(&transport, "Redis stream delivery")
        .await
        .unwrap();
    assert_eq!(delivery.envelope().payload(), [1, 2]);
    assert_eq!(transport.pending_operations(), 1);

    let completion = transport.wait_for_completion(CancellationToken::new());
    tokio::pin!(completion);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), &mut completion)
            .await
            .is_err()
    );
    transport.ack(delivery).await.unwrap();
    completion.await.unwrap();
    assert_eq!(transport.pending_operations(), 0);

    transport.stop_accepting();
    assert!(!transport.is_accepting());
    assert_eq!(
        transport
            .publish(Envelope::new(
                2,
                "order.created",
                vec![3],
                MessageMetadata::new(2, None),
            ))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unavailable
    );
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_idle_receive_does_not_block_publish() -> CatgaResult<()> {
    let config = redis_config();
    let transport = Arc::new(RedisTransport::connect(config).await?);
    let receiver = Arc::clone(&transport);
    let receive =
        tokio::spawn(async move { redis_delivery(receiver.as_ref(), "idle Redis receiver").await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![3, 4],
            MessageMetadata::new(1, None),
        ))
        .await
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Transient,
                format!("Redis publish while idle failed: {error:?}"),
            )
        })?;

    let delivery = tokio::time::timeout(Duration::from_secs(1), receive)
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "idle Redis receive did not complete"))?
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("idle Redis receiver task failed: {error}"),
            )
        })??;
    assert_eq!(delivery.envelope().payload(), [3, 4]);
    transport.ack(delivery).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_dropped_delivery_is_recovered_from_its_pending_entries() {
    let config = redis_config();
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

    let delivery = redis_delivery(&transport, "Redis pending delivery")
        .await
        .unwrap();
    drop(delivery);

    let recovered = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .expect("pending delivery is recovered")
        .expect("receive succeeds");
    assert_eq!(recovered.envelope().payload(), [5, 6]);
    transport.ack(recovered).await.unwrap();
}

/// Redis Streams reports the durable delivery counter for each pending entry.
///
/// Releasing a delivery locally does not acknowledge it in Redis. The recovered entry must carry
/// the broker-maintained count so competing consumers can apply a durable retry limit.
#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_stream_delivery_reports_native_redelivery_attempts() -> CatgaResult<()> {
    let config = redis_config();
    let transport = RedisTransport::connect(config).await?;
    transport
        .publish(Envelope::new(
            81,
            "order.retry",
            vec![8, 1],
            MessageMetadata::new(81, None),
        ))
        .await?;

    let first = redis_delivery(&transport, "Redis first retry delivery").await?;
    assert_eq!(first.attempts(), 1);
    transport.nack(first).await?;

    let redelivery = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "Redis did not redeliver the entry"))??;
    assert!(redelivery.attempts() >= 2);
    transport.ack(redelivery).await
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_live_delivery_does_not_block_the_next_stream_entry() {
    let config = redis_config();
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

    let first = redis_delivery(&transport, "Redis first live delivery")
        .await
        .unwrap();
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_decode_error_does_not_block_pending_entry_recovery() {
    let config = redis_config();
    let server = config.server.to_string();
    let stream = config.stream.to_string();
    let transport = RedisTransport::connect(config).await.unwrap();
    let client = redis::Client::open(server).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let _: Option<String> = connection
        .xadd(&stream, "*", &[("payload", vec![255])])
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_secs(2), transport.receive())
            .await
            .expect("Redis decode-error delivery did not return within two seconds")
            .is_err()
    );
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
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_ack_errors_when_the_pending_entry_was_already_acknowledged() {
    let config = redis_config();
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
    let delivery = redis_delivery(&transport, "Redis acknowledgement delivery")
        .await
        .unwrap();

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
