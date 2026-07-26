//! NATS JetStream integration tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    AsyncInitializable, CatgaError, CatgaResult, DeadLetter, DeadLetterStore, Destination,
    DestinationTransport, EnhancedSnapshotStore, Envelope, EnvelopeCodec, ErrorCode, EventStore,
    HealthCheckable, IdempotencyStore, InboxStore, LeaseStore, MAX_OUTBOX_CLAIM_LIMIT,
    MAX_RETENTION_CLEANUP_LIMIT, MessageMetadata, MessageTransport, OutboxMessage, OutboxState,
    OutboxStore, PersistentSubscription, ProcessingState, ProjectionCheckpoint,
    ProjectionCheckpointStore, QualityOfService, Snapshot, SnapshotStore, Stoppable,
    SubscriptionCheckpoint, SubscriptionStore, Waitable,
};
use catga_flow::{
    DslStepProgress, DslStepProgressStore, FlowContinuation, FlowState, FlowStore,
    SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_nats::{
    NatsConfig, NatsDeadLetters, NatsDestinationConfig, NatsDslStepProgress, NatsEnhancedSnapshots,
    NatsEventStore, NatsFlows, NatsIdempotency, NatsInbox, NatsLeases, NatsOutbox,
    NatsProjectionCheckpoints, NatsPubSubConfig, NatsPubSubTransport, NatsSnapshotStore,
    NatsSubscriptions, NatsSuspendedFlows, NatsTransport,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[path = "flow/dsl_progress_contract.rs"]
mod dsl_progress_contract;
#[path = "flow/timeout_store_contract.rs"]
mod timeout_store_contract;

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_subscriptions_persist_hashed_definitions_checkpoints_and_owner_leases() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsSubscriptions::with_lease_ttl(
        &server,
        format!("CATGA_SUBSCRIPTIONS_{}", std::process::id()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let name = "orders/payment.7";
    store
        .save(PersistentSubscription::new(name, "orders-*").with_event_types(["created"]))
        .await
        .unwrap();
    assert_eq!(
        store
            .list()
            .await
            .unwrap()
            .iter()
            .map(|subscription| subscription.name())
            .collect::<Vec<_>>(),
        [name]
    );
    assert_eq!(
        store
            .load(name)
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
        .save_checkpoint(SubscriptionCheckpoint::new(name, "orders/7", 4))
        .await
        .unwrap();
    assert_eq!(
        store
            .load_checkpoint(name, "orders/7")
            .await
            .unwrap()
            .unwrap()
            .version(),
        4
    );
    assert!(store.try_acquire(name, "worker-a").await.unwrap());
    assert!(!store.try_acquire(name, "worker-b").await.unwrap());
    store.release(name, "worker-b").await.unwrap();
    assert!(!store.try_acquire(name, "worker-b").await.unwrap());
    store.delete(name).await.unwrap();
    assert!(store.load(name).await.unwrap().is_none());
    assert!(
        store
            .load_checkpoint(name, "orders/7")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.try_acquire(name, "worker-b").await.unwrap());
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_projection_checkpoints_are_isolated_by_projection_and_stream() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let checkpoints = NatsProjectionCheckpoints::connect(
        &server,
        format!("CATGA_PROJECTION_CHECKPOINTS_{}", std::process::id()),
    )
    .await
    .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("orders/audit", "order/1", 4))
        .await
        .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("orders/audit", "order/2", 9))
        .await
        .unwrap();
    checkpoints
        .save(ProjectionCheckpoint::new("audit", "order/1", 2))
        .await
        .unwrap();
    assert_eq!(
        checkpoints
            .load("orders/audit", "order/1")
            .await
            .unwrap()
            .unwrap()
            .version(),
        4
    );
    checkpoints.delete("orders/audit", "order/1").await.unwrap();
    assert!(
        checkpoints
            .load("orders/audit", "order/1")
            .await
            .unwrap()
            .is_none()
    );
    checkpoints.delete_all("orders/audit").await.unwrap();
    assert!(
        checkpoints
            .load("orders/audit", "order/2")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        checkpoints
            .load("audit", "order/1")
            .await
            .unwrap()
            .unwrap()
            .version(),
        2
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_enhanced_snapshots_query_history_and_cleanup_versions() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let snapshots = NatsEnhancedSnapshots::<u64>::connect(
        &server,
        format!("CATGA_ENHANCED_SNAPSHOTS_{}", std::process::id()),
    )
    .await
    .unwrap();
    for (version, state) in [(1, 10_u64), (3, 30), (5, 50)] {
        snapshots
            .save(Snapshot::new("account/7", state, version))
            .await
            .unwrap();
    }
    let at_four = snapshots
        .load_at_version::<u64>("account/7", 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((at_four.version(), *at_four.state()), (3, 30));
    assert_eq!(
        snapshots
            .history("account/7")
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.version())
            .collect::<Vec<_>>(),
        [1, 3, 5]
    );
    snapshots
        .delete_before_version("account/7", 3)
        .await
        .unwrap();
    snapshots.cleanup("account/7", 1).await.unwrap();
    assert!(
        snapshots
            .load_at_version::<u64>("account/7", 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        snapshots
            .history("account/7")
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.version())
            .collect::<Vec<_>>(),
        [5]
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_flows_use_hashed_states_and_type_indexes_for_stale_claims() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let flows = NatsFlows::connect(&server, format!("CATGA_FLOWS_{}", std::process::id()))
        .await
        .unwrap();
    let initial = FlowState::new("order/7", "payment", b"input".to_vec(), "node-a");
    assert!(flows.create(initial.clone()).await.unwrap());
    assert!(!flows.create(initial.clone()).await.unwrap());
    assert!(
        flows
            .update(initial.version(), initial.clone().next_version())
            .await
            .unwrap()
    );
    let stale = FlowState::new("order/8", "payment", b"input".to_vec(), "node-a")
        .heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(flows.create(stale).await.unwrap());
    let claimed = flows
        .try_claim("payment", "node-b", Duration::from_secs(86_400))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id(), "order/8");
    assert_eq!(claimed.owner(), Some("node-b"));
    assert_eq!(claimed.version(), 1);
    assert!(flows.heartbeat("order/8", "node-b", 1).await.unwrap());
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_flows_bound_type_index_pages_and_repair_interrupted_creates() {
    const INDEX_PAGE_CAPACITY: usize = 32;

    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let bucket = format!("CATGA_FLOW_INDEX_{}_{}", std::process::id(), suffix);
    let flows = NatsFlows::connect(&server, bucket.clone()).await.unwrap();
    let flow_type = "payment";

    for number in 0..=INDEX_PAGE_CAPACITY {
        let state = FlowState::new(
            format!("payment/{number}"),
            flow_type,
            b"input".to_vec(),
            "node-a",
        );
        assert!(flows.create(state).await.unwrap());
    }
    assert!(
        !flows
            .create(FlowState::new(
                "payment/0",
                flow_type,
                b"input".to_vec(),
                "node-a",
            ))
            .await
            .unwrap()
    );

    let context = jetstream::new(async_nats::connect(&server).await.unwrap());
    let index = context
        .get_key_value(format!("{bucket}_IDX"))
        .await
        .unwrap();
    let type_hash = format!("{:x}", Sha256::digest(flow_type.as_bytes()));
    for (page, expected_entries) in [INDEX_PAGE_CAPACITY, 1].into_iter().enumerate() {
        let entry = index
            .entry(format!("p{type_hash}.{page}"))
            .await
            .unwrap()
            .expect("the type page exists");
        let payload = entry
            .value
            .strip_prefix(b"CNR1")
            .and_then(|value| value.get(16..))
            .expect("the index page has a create envelope");
        let ids = postcard::from_bytes::<Vec<Box<str>>>(payload).unwrap();
        assert_eq!(ids.len(), expected_entries);
    }

    let terminal = flows.get("payment/0").await.unwrap().unwrap();
    assert!(
        flows
            .update(terminal.version(), terminal.clone().done(1).next_version(),)
            .await
            .unwrap()
    );
    let page = index
        .entry(format!("p{type_hash}.0"))
        .await
        .unwrap()
        .expect("the first type page exists");
    let payload = page
        .value
        .strip_prefix(b"CNR1")
        .and_then(|value| value.get(16..))
        .expect("the index page has a create envelope");
    let ids = postcard::from_bytes::<Vec<Box<str>>>(payload).unwrap();
    assert_eq!(ids.len(), INDEX_PAGE_CAPACITY.saturating_sub(1));
    assert!(!ids.iter().any(|id| id.as_ref() == terminal.id()));

    let interrupted = FlowState::new(
        "payment/interrupted",
        flow_type,
        b"input".to_vec(),
        "node-a",
    )
    .heartbeated_at(SystemTime::UNIX_EPOCH);
    let states = context.get_key_value(&bucket).await.unwrap();
    states
        .create(
            format!("f{:x}", Sha256::digest(interrupted.id().as_bytes())),
            postcard::to_allocvec(&interrupted).unwrap().into(),
        )
        .await
        .unwrap();

    assert!(!flows.create(interrupted.clone()).await.unwrap());
    let mut recovered = None;
    for _ in 0..=INDEX_PAGE_CAPACITY.saturating_add(1) {
        let candidate = flows
            .try_claim(flow_type, "node-b", Duration::from_secs(86_400))
            .await
            .unwrap();
        if candidate
            .as_ref()
            .is_some_and(|state| state.id() == interrupted.id())
        {
            recovered = candidate;
            break;
        }
    }
    assert_eq!(
        recovered.as_ref().map(FlowState::id),
        Some(interrupted.id())
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_dsl_step_progress_uses_hashed_keys_and_revision_updates() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsDslStepProgress::connect(
        &server,
        format!("CATGA_DSL_PROGRESS_{}", std::process::id()),
    )
    .await
    .unwrap();
    let initial = DslStepProgress::new("payment/7", 2, b"cursor:3".to_vec());
    assert!(store.create(initial.clone()).await.unwrap());
    assert!(
        store
            .update(
                initial.version(),
                initial.clone().next_version(b"cursor:4".to_vec()),
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.get("payment/7", 2).await.unwrap().unwrap().payload(),
        b"cursor:4"
    );
    assert!(store.delete("payment/7", 2).await.unwrap());
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_dsl_progress_runs_durable_recovery_contract() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let store = NatsDslStepProgress::connect(
        &server,
        format!("CATGA_DSL_RECOVERY_{}_{}", std::process::id(), suffix),
    )
    .await?;

    dsl_progress_contract::run_durable_recovery_contracts(&store, "payment/recovery-contract").await
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_suspended_flows_preserve_wait_results_and_claims() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsSuspendedFlows::connect(&server, format!("CATGA_FLOWS_{}", std::process::id()))
        .await
        .unwrap();
    let continuation = waiting_continuation("nats-flow");
    assert!(store.create(continuation.clone()).await.unwrap());
    assert!(!store.create(continuation).await.unwrap());
    assert!(
        store
            .record_wait_success("nats-flow", 0, "child-a", b"ok".to_vec())
            .await
            .unwrap()
    );
    assert!(
        store
            .record_wait_failure(
                "nats-flow",
                0,
                "child-b",
                catga_core::CatgaError::new(ErrorCode::Transient, "unavailable"),
            )
            .await
            .unwrap()
    );

    let current = store.get("nats-flow").await.unwrap().unwrap();
    assert_eq!(current.wait().unwrap().completed_count(), 2);
    assert!(store.heartbeat("nats-flow", "node-a", 0).await.unwrap());
    let stale_claim = current
        .clone()
        .with_state(current.state().clone().claimed_by("node-b").next_version());
    assert!(!store.claim(&current, stale_claim).await.unwrap());

    let current = store.get("nats-flow").await.unwrap().unwrap();
    let next = current
        .clone()
        .with_state(current.state().clone().claimed_by("node-b").next_version());
    assert!(store.claim(&current, next.clone()).await.unwrap());
    assert!(
        store
            .heartbeat("nats-flow", "node-b", next.state().version())
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_suspended_flows_retry_only_real_revision_conflicts() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = Arc::new(
        NatsSuspendedFlows::connect(&server, format!("CATGA_FLOWS_RACE_{}", std::process::id()))
            .await
            .unwrap(),
    );
    assert!(
        store
            .create(waiting_continuation("nats-flow-race"))
            .await
            .unwrap()
    );

    let first = store.record_wait_success("nats-flow-race", 0, "child-a", b"a".to_vec());
    let second = store.record_wait_success("nats-flow-race", 0, "child-b", b"b".to_vec());
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap());
    assert!(second.unwrap());
    assert_eq!(
        store
            .get("nats-flow-race")
            .await
            .unwrap()
            .unwrap()
            .wait()
            .unwrap()
            .completed_count(),
        2
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_suspended_flows_page_bounded_timeout_queries() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsSuspendedFlows::connect(
        &server,
        format!("CATGA_TIMEOUT_CONTRACT_{}", std::process::id()),
    )
    .await?;
    timeout_store_contract::run_timeout_store_contract(&store, "nats-timeout", true).await
}

fn waiting_continuation(id: &str) -> FlowContinuation {
    FlowContinuation::waiting(
        FlowState::new(id, "payment", b"input".to_vec(), "node-a"),
        "charge",
        WaitCondition::new(
            format!("{id}-wait"),
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    )
}

/// Core NATS broadcasts are ephemeral and do not carry acknowledgement tokens.
#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn core_nats_pubsub_transport_broadcasts_at_most_once() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.into(),
        subject: format!("catga.pubsub.{}", std::process::id()).into(),
    })
    .await?;
    transport
        .publish(Envelope::new(
            61,
            "order.broadcast",
            vec![6, 1],
            MessageMetadata::new(61, None).with_quality_of_service(QualityOfService::AtMostOnce),
        ))
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "Core NATS delivery timed out"))??;
    assert_eq!(delivery.envelope().payload(), [6, 1]);
    assert_eq!(delivery.attempts(), 1);
    transport.ack(delivery).await?;
    assert_eq!(transport.pending_operations(), 0);

    transport.stop_accepting();
    assert!(matches!(
        transport
            .publish(Envelope::new(
                62,
                "order.broadcast",
                Vec::new(),
                MessageMetadata::new(62, None).with_quality_of_service(QualityOfService::AtMostOnce),
            ))
            .await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    Ok(())
}

/// Core NATS must not claim durable guarantees that only JetStream can provide.
#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn core_nats_pubsub_transport_rejects_durable_qos() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.into(),
        subject: format!("catga.pubsub.qos.{}", std::process::id()).into(),
    })
    .await?;
    let Err(error) = transport
        .publish(Envelope::new(
            63,
            "order.broadcast",
            Vec::new(),
            MessageMetadata::new(63, None),
        ))
        .await
    else {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "Core NATS accepted a durable QoS request",
        ));
    };
    assert_eq!(error.code(), ErrorCode::Unsupported);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_leases_compare_owner_with_kv_revisions() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
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
#[ignore = "requires CATGA_NATS_URL"]
async fn jetstream_round_trip_and_ack() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = format!("{}", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_{suffix}").into(),
        subject: format!("catga.{suffix}").into(),
        consumer: format!("catga_{suffix}").into(),
    })
    .await
    .unwrap();

    transport.initialize().await.unwrap();
    assert!(transport.is_healthy());
    assert_eq!(transport.health_status(), Some("NATS transport is ready"));

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

/// JetStream exposes its durable redelivery count through [`catga_core::Delivery`].
///
/// A negative acknowledgement must cause the next delivery of the same message to report a
/// greater attempt count. This is the value the competing consumer uses to decide when an
/// unrecoverable message belongs in the dead-letter store.
#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn jetstream_delivery_reports_native_redelivery_attempts() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = format!("{}_attempts", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_{suffix}").into(),
        subject: format!("catga.{suffix}").into(),
        consumer: format!("catga_{suffix}").into(),
    })
    .await?;

    transport
        .publish(Envelope::new(
            71,
            "order.retry",
            vec![7, 1],
            MessageMetadata::new(71, None),
        ))
        .await?;
    let first = transport.receive().await?;
    assert_eq!(first.attempts(), 1);
    transport.nack(first).await?;

    let redelivery = transport.receive().await?;
    assert!(redelivery.attempts() >= 2);
    transport.ack(redelivery).await
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn jetstream_destination_requires_explicit_resource_provisioning() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = format!("{}", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_DEST_MAIN_{suffix}").into(),
        subject: format!("catga.destination.main.{suffix}").into(),
        consumer: format!("catga_destination_main_{suffix}").into(),
    })
    .await?;
    let destination = Destination::parse(format!("orders:{suffix}"))?;

    assert!(matches!(
        transport
            .send_to(
                &destination,
                Envelope::new(401, "order.queued", vec![4, 0, 1], MessageMetadata::new(401, None)),
            )
            .await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    transport
        .provision_destination(
            destination.clone(),
            NatsDestinationConfig {
                stream: format!("CATGA_DEST_ORDERS_{suffix}").into(),
                subject: format!("catga.destination.orders.{suffix}").into(),
                consumer: format!("catga_destination_orders_{suffix}").into(),
            },
        )
        .await?;
    transport
        .send_to(
            &destination,
            Envelope::new(
                401,
                "order.queued",
                vec![4, 0, 1],
                MessageMetadata::new(401, None),
            ),
        )
        .await?;
    let delivery = transport.receive_from(&destination).await?;
    assert_eq!(delivery.envelope().id(), 401);
    transport.ack(delivery).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn jetstream_exactly_once_deduplicates_repeated_envelope_ids() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let suffix = format!("{}", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_QOS_{suffix}").into(),
        subject: format!("catga.qos.{suffix}").into(),
        consumer: format!("catga_qos_{suffix}").into(),
    })
    .await
    .unwrap();
    let envelope = Envelope::new(
        77,
        "order.created",
        vec![7],
        MessageMetadata::new(77, None).with_quality_of_service(QualityOfService::ExactlyOnce),
    );

    transport.publish(envelope.clone()).await.unwrap();
    transport.publish(envelope).await.unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(1), transport.receive())
        .await
        .expect("one JetStream delivery must be available")
        .expect("JetStream delivery must decode");
    assert_eq!(delivery.envelope().id(), 77);
    transport.ack(delivery).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), transport.receive())
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_event_store_persists_versioned_history_with_subject_cas() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
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
                vec![
                    Envelope::new(1, "order.paid", vec![2], MessageMetadata::new(2, Some(1)),),
                    Envelope::new(
                        1,
                        "order.shipped",
                        vec![3],
                        MessageMetadata::new(3, Some(1)),
                    ),
                ],
                Some(0),
            )
            .await
            .unwrap(),
        2
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
    let page = tokio::time::timeout(Duration::from_secs(1), store.read_page("orders-7", 0, 2))
        .await
        .expect("bounded NATS event read must finish")
        .unwrap();
    assert_eq!(
        page.stream()
            .events()
            .iter()
            .map(|event| event.version())
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(page.stream().events()[1].envelope().payload(), [2]);
    let through_version_zero = tokio::time::timeout(
        Duration::from_secs(1),
        store.read_to_version_page("orders-7", 0, 0, 3),
    )
    .await
    .expect("version-bounded NATS event history read must finish")
    .unwrap();
    assert_eq!(through_version_zero.stream().events().len(), 1);
    let history = tokio::time::timeout(
        Duration::from_secs(1),
        store.read_to_time_page("orders-7", 0, SystemTime::now(), 3),
    )
    .await
    .expect("full NATS event history read must finish")
    .unwrap();
    assert_eq!(
        history
            .stream()
            .events()
            .iter()
            .map(|event| event.version())
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let versions = tokio::time::timeout(
        Duration::from_secs(1),
        store.version_history_page("orders-7", 0, 3),
    )
    .await
    .expect("NATS version history read must finish")
    .unwrap();
    assert_eq!(versions.entries()[2].event_type(), "order.shipped");
    assert_eq!(
        store.stream_ids_page(None, 3).await.unwrap().ids(),
        ["orders-7"]
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_snapshots_round_trip_and_reject_stale_writers_with_kv_revisions() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_idempotency_claims_exclusively_retries_failures_and_caches_results() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_idempotency_concurrent_claims_have_exactly_one_owner() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
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

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_idempotency_honors_configured_retention_and_bounds_cleanup() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsIdempotency::with_retention(
        &server,
        format!("CATGA_IDEMP_RETENTION_{}", std::process::id()),
        Duration::from_millis(100),
    )
    .await?;
    assert!(store.try_claim("retained-key").await?);
    store.complete("retained-key", None).await?;

    assert!(matches!(
        store
            .cleanup_completed(MAX_RETENTION_CLEANUP_LIMIT + 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(store.cleanup_completed(1).await?, 0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(store.state("retained-key").await?, None);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_inbox_claims_exclusively_retries_failures_and_caches_results() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let inbox = NatsInbox::connect(&server, format!("CATGA_INBOX_{}", std::process::id()))
        .await
        .unwrap();
    assert!(inbox.try_claim(7).await.unwrap());
    assert!(!inbox.try_claim(7).await.unwrap());
    inbox.fail(7).await.unwrap();
    assert!(inbox.try_claim(7).await.unwrap());
    inbox.complete(7, Some(Arc::from([1_u8, 2]))).await.unwrap();
    assert_eq!(
        inbox.state(7).await.unwrap(),
        Some(ProcessingState::Completed)
    );
    assert_eq!(inbox.result(7).await.unwrap().as_deref(), Some(&[1, 2][..]));
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_inbox_reclaims_an_expired_processing_lease() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let inbox = NatsInbox::connect(&server, format!("CATGA_INBOX_LEASE_{}", std::process::id()))
        .await
        .unwrap();

    assert!(
        inbox
            .try_claim_for(91, Duration::from_millis(1))
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        inbox
            .try_claim_for(91, Duration::from_secs(1))
            .await
            .unwrap()
    );
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_inbox_removes_completed_records_with_a_bounded_scan() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let inbox = NatsInbox::connect(
        &server,
        format!("CATGA_INBOX_RETENTION_{}", std::process::id()),
    )
    .await?;
    for message_id in [201_u64, 202] {
        assert!(inbox.try_claim(message_id).await?);
        inbox.complete(message_id, None).await?;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_dead_letters_preserve_queue_order_and_envelopes() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let letters = NatsDeadLetters::connect(
        &server,
        format!("CATGA_DLQ_{}", std::process::id()),
        format!("catga.dlq.{}", std::process::id()),
    )
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

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_claims_and_acknowledges_only_the_current_owner() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(&server, format!("CATGA_OUTBOX_{}", std::process::id()))
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
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            10,
            "order.created",
            vec![2],
            MessageMetadata::new(10, None),
        )))
        .await
        .unwrap();
    let claimed = outbox.claim("worker-a", 2).await.unwrap();
    assert_eq!(
        claimed.iter().map(OutboxMessage::id).collect::<Vec<_>>(),
        [7, 10]
    );
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_reclaims_an_expired_claim_without_accepting_a_stale_ack() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(
        &server,
        format!("CATGA_OUTBOX_CLAIM_LEASE_{}", std::process::id()),
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
        .claim_for("worker-a", 1, Duration::from_millis(40))
        .await?
        .pop()
        .unwrap();
    assert!(
        outbox
            .claim_for("worker-b", 1, Duration::from_millis(40))
            .await?
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(60)).await;
    let reclaimed = outbox
        .claim_for("worker-b", 1, Duration::from_millis(40))
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_updates_legacy_keys_and_releases_for_immediate_reclaim() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let bucket = format!("CATGA_OUTBOX_LEGACY_{}", std::process::id());
    let context = jetstream::new(
        async_nats::connect(&server)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?,
    );
    let raw_store = context
        .create_key_value(kv::Config {
            bucket: bucket.clone(),
            history: 1,
            ..Default::default()
        })
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
    let id = 74_u64;
    let payload = PostcardCodec.encode(&Envelope::new(
        id,
        "order.created",
        vec![1],
        MessageMetadata::new(id, None),
    ))?;
    let owner = b"legacy-worker";
    let mut legacy = Vec::with_capacity(2 + owner.len() + payload.len());
    legacy.extend_from_slice(&(owner.len() as u16).to_be_bytes());
    legacy.extend_from_slice(owner);
    legacy.extend_from_slice(&payload);
    raw_store
        .create("legacy-74", legacy.into())
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;

    let outbox = NatsOutbox::connect(&server, bucket).await?;
    assert!(matches!(
        outbox
            .enqueue(OutboxMessage::new(Envelope::new(
                id,
                "duplicate",
                vec![],
                MessageMetadata::new(id, None),
            )))
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    let first = outbox.claim("worker-a", 1).await?.pop().unwrap();
    outbox
        .release("worker-a", id, first.claim_token().unwrap())
        .await?;
    let reclaimed = outbox.claim("worker-a", 1).await?.pop().unwrap();
    assert_ne!(first.claim_token(), reclaimed.claim_token());
    outbox
        .ack("worker-a", id, reclaimed.claim_token().unwrap())
        .await?;
    assert!(
        raw_store
            .entry("legacy-74")
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?
            .is_some()
    );
    assert!(
        raw_store
            .entry("m00000000000000000074")
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_retains_published_records_until_bounded_cleanup() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(
        &server,
        format!("CATGA_OUTBOX_RETENTION_{}", std::process::id()),
    )
    .await?;
    // A pending entry may precede the published one in the KV key stream.
    // Listing published history must not treat its result limit as a key-scan limit.
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            86,
            "order.pending",
            vec![0],
            MessageMetadata::new(86, None),
        )))
        .await?;
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            87,
            "order.published",
            vec![1],
            MessageMetadata::new(87, None),
        )))
        .await?;
    let claimed = outbox.claim("worker-a", 2).await?;
    let published_claim = claimed.iter().find(|message| message.id() == 87).unwrap();

    outbox
        .ack("stale-worker", 87, published_claim.claim_token().unwrap())
        .await?;
    assert!(outbox.list_published(1).await?.is_empty());
    outbox
        .ack("worker-a", 87, published_claim.claim_token().unwrap())
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_stops_reclaiming_after_its_failure_limit() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(
        &server,
        format!("CATGA_OUTBOX_FAILURES_{}", std::process::id()),
    )
    .await?;
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
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_rejects_claims_above_the_shared_memory_budget() -> CatgaResult<()> {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(
        &server,
        format!("CATGA_OUTBOX_CLAIM_BOUND_{}", std::process::id()),
    )
    .await?;

    assert!(matches!(
        outbox.claim("worker-a", MAX_OUTBOX_CLAIM_LIMIT + 1).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_outbox_does_not_claim_a_message_before_its_delivery_time() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let outbox = NatsOutbox::connect(
        &server,
        format!("CATGA_OUTBOX_SCHEDULED_{}", std::process::id()),
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
