//! Public NATS adapter contracts exercised against an isolated JetStream server.

use std::{
    net::TcpListener,
    process::{Child, Command},
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, UNIX_EPOCH},
};

use async_nats::jetstream::{self, consumer::pull, kv};
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore,
    EnhancedSnapshotStore, Envelope, EnvelopeCodec, ErrorCode, EventStore, MessageMetadata,
    MessageTransport, OutboxMessage, OutboxState, OutboxStore, Projection, ProjectionCheckpoint,
    ProjectionCheckpointStore, QualityOfService, Snapshot, SnapshotStore, StoredEvent,
};
use catga_flow::{
    DueFlowScheduler, FlowContinuation, FlowQuery, FlowScheduler, FlowState, FlowStatus, FlowStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition,
    WaitPolicy,
};
use catga_nats::{
    NatsConfig, NatsConsumerOptions, NatsDeadLetters, NatsEnhancedSnapshots, NatsEventStore,
    NatsFlowScheduler, NatsFlows, NatsOutbox, NatsProjectionCheckpoints, NatsProjectionConfig,
    NatsProjectionRunner, NatsPubSubConfig, NatsPubSubTransport, NatsReceiveOptions,
    NatsRequestClient, NatsSuspendedFlows, NatsTransport, NatsTransportOptions,
};
use tempfile::TempDir;

static TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

#[test]
fn configured_nats_url_prefers_the_ci_service_url() {
    assert_eq!(
        configured_nats_url(Some("nats://127.0.0.1:4222")),
        Some("nats://127.0.0.1:4222".to_owned())
    );
}

struct NatsServer {
    child: Option<Child>,
    _data_directory: Option<TempDir>,
    url: String,
}

impl NatsServer {
    async fn start() -> CatgaResult<Self> {
        if let Some(url) = configured_nats_url(std::env::var("CATGA_NATS_URL").ok().as_deref()) {
            return Ok(Self {
                child: None,
                _data_directory: None,
                url,
            });
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|error| test_error("reserve NATS test port", error))?;
        let port = listener
            .local_addr()
            .map_err(|error| test_error("read NATS test port", error))?
            .port();
        drop(listener);

        let data_directory = tempfile::tempdir()
            .map_err(|error| test_error("create NATS test data directory", error))?;
        let mut child = Command::new("nats-server")
            .args(["-js", "-a", "127.0.0.1", "-p"])
            .arg(port.to_string())
            .arg("-sd")
            .arg(data_directory.path())
            .spawn()
            .map_err(|error| test_error("start local NATS JetStream server", error))?;
        let url = format!("nats://127.0.0.1:{port}");

        for _ in 0..50 {
            if let Ok(client) = async_nats::connect(&url).await {
                drop(client);
                return Ok(Self {
                    child: Some(child),
                    _data_directory: Some(data_directory),
                    url,
                });
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let _ = child.kill();
        let _ = child.wait();
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "local NATS JetStream server did not become ready",
        ))
    }

    fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn configured_nats_url(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn test_error(context: &'static str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}

fn unique(prefix: &str) -> String {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), sequence)
}

fn envelope(id: u64, quality_of_service: QualityOfService) -> Envelope {
    Envelope::new(
        id,
        "nats.contract",
        vec![u8::try_from(id).unwrap_or_default()],
        MessageMetadata::new(id, None).with_quality_of_service(quality_of_service),
    )
}

struct ProjectionCounter {
    total: AtomicUsize,
}

impl ProjectionCounter {
    const fn new() -> Self {
        Self {
            total: AtomicUsize::new(0),
        }
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl Projection for ProjectionCounter {
    fn name(&self) -> &str {
        "nats-projection-runner-contract"
    }

    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()> {
        self.total
            .fetch_add(usize::from(event.envelope().payload()[0]), Ordering::AcqRel);
        Ok(())
    }

    async fn reset(&self) -> CatgaResult<()> {
        self.total.store(0, Ordering::Release);
        Ok(())
    }
}

fn assert_validation<T>(result: CatgaResult<T>) {
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
}

#[tokio::test]
async fn public_connectors_reject_invalid_configuration_before_network_io() {
    let base = NatsConfig {
        server: "nats://127.0.0.1:1".into(),
        stream: "orders".into(),
        subject: "orders.created".into(),
        consumer: "workers".into(),
    };
    for invalid in [
        NatsConfig {
            stream: " ".into(),
            ..base.clone()
        },
        NatsConfig {
            subject: " ".into(),
            ..base.clone()
        },
        NatsConfig {
            consumer: " ".into(),
            ..base.clone()
        },
    ] {
        assert_validation(NatsTransport::connect(invalid).await);
    }

    for subject in ["", " \t"] {
        assert_validation(
            NatsPubSubTransport::connect(NatsPubSubConfig {
                server: "nats://127.0.0.1:1".into(),
                subject: subject.into(),
            })
            .await,
        );
        assert_validation(NatsRequestClient::connect("nats://127.0.0.1:1", subject).await);
    }
    for prefix in ["", "catga.*", "catga.>", "catga..events"] {
        assert_validation(NatsEventStore::connect("nats://127.0.0.1:1", "EVENTS", prefix).await);
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn durable_transport_uses_public_qos_contracts_and_round_trips_envelopes() -> CatgaResult<()>
{
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_TRANSPORT").into(),
        subject: unique("catga.transport").into(),
        consumer: unique("CATGA_CONSUMER").into(),
    })
    .await?;

    assert!(matches!(
        transport
            .publish(envelope(1, QualityOfService::AtMostOnce))
            .await,
        Err(error) if error.code() == ErrorCode::Unsupported
    ));

    let at_least_once = envelope(2, QualityOfService::AtLeastOnce);
    transport.publish(at_least_once.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive durable NATS delivery", error))??;
    assert_eq!(delivery.envelope(), &at_least_once);
    delivery.acknowledge().await?;

    let exactly_once = envelope(3, QualityOfService::ExactlyOnce);
    transport.publish(exactly_once.clone()).await?;
    transport.publish(exactly_once.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive deduplicated NATS delivery", error))??;
    assert_eq!(delivery.envelope(), &exactly_once);
    delivery.acknowledge().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), transport.receive())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn durable_transport_retains_every_delivery_from_a_configured_pull_batch() -> CatgaResult<()>
{
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect_with_receive_options(
        NatsConfig {
            server: server.url().into(),
            stream: unique("CATGA_BATCHED_TRANSPORT").into(),
            subject: unique("catga.batched.transport").into(),
            consumer: unique("CATGA_BATCHED_CONSUMER").into(),
        },
        NatsReceiveOptions::default().with_pull_batch_size(2)?,
    )
    .await?;
    let first = envelope(11, QualityOfService::AtLeastOnce);
    let second = envelope(12, QualityOfService::AtLeastOnce);
    transport.publish(first.clone()).await?;
    transport.publish(second.clone()).await?;

    let delivered_first = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive first prefetched NATS delivery", error))??;
    assert_eq!(delivered_first.envelope(), &first);
    delivered_first.acknowledge().await?;

    let delivered_second = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive second prefetched NATS delivery", error))??;
    assert_eq!(delivered_second.envelope(), &second);
    delivered_second.acknowledge().await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn ephemeral_transport_round_trips_without_creating_the_configured_durable_cursor()
-> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let config = NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_EPHEMERAL_TRANSPORT").into(),
        subject: unique("catga.ephemeral.transport").into(),
        consumer: unique("CATGA_MUST_NOT_BE_DURABLE").into(),
    };
    let transport = NatsTransport::connect_with_options(
        config.clone(),
        NatsTransportOptions::default().with_consumer(
            NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(90)),
        ),
    )
    .await?;
    let message = envelope(13, QualityOfService::AtLeastOnce);
    transport.publish(message.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive ephemeral NATS delivery", error))??;
    assert_eq!(delivery.envelope(), &message);
    delivery.acknowledge().await?;

    let context = jetstream::new(async_nats::connect(server.url()).await.map_err(|error| {
        test_error(
            "connect inspection client for ephemeral NATS transport",
            error,
        )
    })?);
    let stream = context
        .get_stream(config.stream.as_ref())
        .await
        .map_err(|error| test_error("inspect ephemeral NATS transport stream", error))?;
    assert!(
        stream
            .get_consumer::<pull::Config>(config.consumer.as_ref())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn projection_runner_replays_incrementally_and_rebuilds_from_nats_event_history()
-> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let config = NatsProjectionConfig {
        event_stream: unique("CATGA_PROJECTION_EVENTS").into(),
        event_subject_prefix: unique("catga.projection.events").into(),
        checkpoint_bucket: unique("CATGA_PROJECTION_CHECKPOINTS").into(),
    };
    let events = NatsEventStore::connect(
        server.url(),
        config.event_stream.clone(),
        config.event_subject_prefix.clone(),
    )
    .await?;
    events
        .append(
            "order-1",
            vec![
                envelope(21, QualityOfService::AtLeastOnce),
                envelope(22, QualityOfService::AtLeastOnce),
            ],
            None,
        )
        .await?;

    let runner =
        NatsProjectionRunner::connect(server.url(), config, ProjectionCounter::new()).await?;
    assert_eq!(runner.run().await?.applied(), 2);
    assert_eq!(runner.projection().total(), 43);

    events
        .append(
            "order-1",
            vec![envelope(23, QualityOfService::AtLeastOnce)],
            Some(1),
        )
        .await?;
    assert_eq!(runner.run().await?.applied(), 1);
    assert_eq!(runner.projection().total(), 66);
    assert_eq!(runner.rebuild().await?.applied(), 3);
    assert_eq!(runner.projection().total(), 66);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn core_pubsub_delivers_at_most_once_envelopes() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub").into(),
    })
    .await?;
    let message = envelope(4, QualityOfService::AtMostOnce);
    transport.publish(message.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive Core NATS delivery", error))??;
    assert_eq!(delivery.envelope(), &message);
    delivery.acknowledge().await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_persists_versioned_pages_through_its_public_trait() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsEventStore::connect(server.url(), unique("CATGA_EVENTS"), unique("catga.events"))
            .await?;
    assert_eq!(store.append("orders", Vec::new(), Some(999)).await?, -1);
    assert_eq!(
        store
            .append(
                "orders",
                vec![
                    envelope(5, QualityOfService::AtLeastOnce),
                    envelope(6, QualityOfService::AtLeastOnce),
                ],
                Some(-1),
            )
            .await?,
        1
    );
    let page = store.read_page("orders", 0, 1).await?;
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(page.stream().events()[0].version(), 0);
    assert_eq!(page.next_version(), Some(1));
    assert_eq!(store.version("orders").await?, 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dead_letters_round_trip_diagnostics_and_read_legacy_records() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let stream = unique("CATGA_DLQ");
    let subject = unique("catga.dlq");
    let letters = NatsDeadLetters::connect(server.url(), stream, subject.clone()).await?;
    let diagnostics = DeadLetterDiagnostics::try_at(123, ErrorCode::Timeout, "consumer.handle")?;
    let current = DeadLetter::try_with_diagnostics(
        envelope(7, QualityOfService::AtLeastOnce),
        "expired",
        2,
        diagnostics,
    )?;
    letters.enqueue(current.clone()).await?;
    assert!(letters.list(0).await?.is_empty());

    let legacy_envelope = envelope(8, QualityOfService::AtLeastOnce);
    let payload = MemoryPackCodec::default().encode(&legacy_envelope)?;
    let reason = b"old failure";
    let mut legacy = Vec::with_capacity(12 + reason.len() + payload.len());
    legacy.extend_from_slice(&4_u32.to_be_bytes());
    legacy.extend_from_slice(
        &u32::try_from(reason.len())
            .map_err(|error| test_error("encode legacy dead-letter reason length", error))?
            .to_be_bytes(),
    );
    legacy.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|error| test_error("encode legacy dead-letter payload length", error))?
            .to_be_bytes(),
    );
    legacy.extend_from_slice(reason);
    legacy.extend_from_slice(&payload);
    let legacy_publisher = jetstream::new(
        async_nats::connect(server.url())
            .await
            .map_err(|error| test_error("connect legacy dead-letter publisher", error))?,
    );
    legacy_publisher
        .publish(format!("{subject}.legacy"), legacy.into())
        .await
        .map_err(|error| test_error("begin legacy dead-letter publish", error))?
        .await
        .map_err(|error| test_error("confirm legacy dead-letter publish", error))?;

    let first = letters.list(1).await?;
    assert_eq!(first, vec![current.clone()]);
    let listed = letters.list(10).await?;
    assert_eq!(listed[0], current);
    assert_eq!(listed[1].envelope(), &legacy_envelope);
    assert_eq!(listed[1].reason(), "old failure");
    assert_eq!(listed[1].diagnostics().stage(), "legacy");
    assert_eq!(listed[1].diagnostics().failed_at_unix_ms(), 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn outbox_exercises_release_failure_cancel_and_published_cleanup() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let outbox = NatsOutbox::connect(server.url(), unique("CATGA_OUTBOX_LIFECYCLE")).await?;

    assert_validation(
        outbox
            .enqueue(OutboxMessage::new(envelope(
                0,
                QualityOfService::AtLeastOnce,
            )))
            .await,
    );
    outbox
        .enqueue(OutboxMessage::new(envelope(
            10,
            QualityOfService::AtLeastOnce,
        )))
        .await?;
    assert!(outbox.cancel(10).await?);
    assert!(!outbox.cancel(10).await?);

    let retrying =
        OutboxMessage::new(envelope(11, QualityOfService::AtLeastOnce)).with_max_retries(2)?;
    outbox.enqueue(retrying).await?;
    let first_claim =
        outbox.claim("worker-a", 1).await?.pop().ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "expected first NATS outbox claim")
        })?;
    let first_token = first_claim
        .claim_token()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "NATS outbox claim has no token"))?;
    outbox.release("worker-a", 11, "stale-token").await?;
    assert!(outbox.claim("worker-a", 1).await?.is_empty());
    outbox.release("worker-a", 11, first_token).await?;

    let second_claim = outbox.claim("worker-a", 1).await?.pop().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "expected released NATS outbox claim")
    })?;
    let second_token = second_claim.claim_token().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "released NATS outbox claim has no token",
        )
    })?;
    outbox
        .record_failure("worker-a", 11, second_token, "first publish failure")
        .await?;

    let final_claim = outbox.claim("worker-a", 1).await?.pop().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "expected retried NATS outbox claim")
    })?;
    assert_eq!(final_claim.retry_count(), 1);
    assert_eq!(final_claim.last_error(), Some("first publish failure"));
    let final_token = final_claim.claim_token().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "retried NATS outbox claim has no token",
        )
    })?;
    outbox
        .record_failure("worker-a", 11, final_token, "terminal publish failure")
        .await?;
    assert!(outbox.claim("worker-a", 1).await?.is_empty());

    outbox
        .enqueue(OutboxMessage::new(envelope(
            12,
            QualityOfService::AtLeastOnce,
        )))
        .await?;
    let published_claim = outbox.claim("worker-b", 1).await?.pop().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "expected publishable NATS outbox claim",
        )
    })?;
    let published_token = published_claim.claim_token().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "publishable NATS outbox claim has no token",
        )
    })?;
    outbox.ack("worker-b", 12, published_token).await?;
    assert!(outbox.list_published(0).await?.is_empty());
    assert_eq!(
        outbox.list_published(1).await?[0].state(),
        OutboxState::Published
    );
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 10).await?, 1);
    assert!(outbox.list_published(10).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn scheduler_fences_owners_and_supports_cancellation() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let scheduler = NatsFlowScheduler::connect(server.url(), unique("CATGA_SCHEDULER")).await?;
    let now = UNIX_EPOCH + Duration::from_secs(1);
    let schedule_id = scheduler
        .schedule_resume("payment-17", "charge", now)
        .await?;
    assert_validation(
        scheduler
            .claim_due("worker-a", now, Duration::ZERO, 1)
            .await,
    );
    assert!(
        scheduler
            .claim_due("worker-a", now, Duration::from_secs(5), 0)
            .await?
            .is_empty()
    );

    let claimed = scheduler
        .claim_due("worker-a", now, Duration::from_secs(5), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), schedule_id.as_ref());
    assert_eq!(claimed[0].due_at(), now);
    assert!(!scheduler.ack_due("worker-b", &schedule_id).await?);
    assert!(!scheduler.release_due("worker-b", &schedule_id).await?);
    assert!(
        scheduler
            .renew_due("worker-a", &schedule_id, now, Duration::from_secs(5))
            .await?
    );
    assert!(scheduler.release_due("worker-a", &schedule_id).await?);
    assert_eq!(
        scheduler
            .claim_due("worker-b", now, Duration::from_secs(5), 1)
            .await?
            .len(),
        1
    );
    assert!(scheduler.ack_due("worker-b", &schedule_id).await?);
    assert!(!scheduler.ack_due("worker-b", "invalid-schedule-id").await?);

    let future = now + Duration::from_secs(60);
    let cancelled = scheduler
        .schedule_resume("payment-18", "charge", future)
        .await?;
    assert!(scheduler.cancel_resume(&cancelled).await?);
    assert!(!scheduler.cancel_resume(&cancelled).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn scheduler_claims_across_full_index_pages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let scheduler =
        NatsFlowScheduler::connect(server.url(), unique("CATGA_SCHEDULER_PAGES")).await?;
    let due = UNIX_EPOCH + Duration::from_secs(200);

    for state_id in 0..33 {
        scheduler
            .schedule_resume("payment-22", &format!("charge-{state_id}"), due)
            .await?;
    }

    let first_page = scheduler
        .claim_due("worker", due, Duration::from_secs(10), 32)
        .await?;
    assert_eq!(first_page.len(), 32);
    assert_eq!(
        scheduler
            .claim_due("worker", due, Duration::from_secs(10), 1)
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn outbox_orders_claims_recovers_expired_leases_and_bounds_cleanup() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let outbox = NatsOutbox::connect(server.url(), unique("CATGA_OUTBOX_ORDERING")).await?;
    let now = std::time::SystemTime::now();

    for id in [24, 22, 23] {
        outbox
            .enqueue(OutboxMessage::new(envelope(
                id,
                QualityOfService::AtLeastOnce,
            )))
            .await?;
    }
    outbox
        .enqueue(OutboxMessage::scheduled(
            envelope(25, QualityOfService::AtLeastOnce),
            now + Duration::from_secs(30),
        )?)
        .await?;

    let first = outbox
        .claim_for("worker", 2, Duration::from_millis(10))
        .await?;
    assert_eq!(
        first.iter().map(OutboxMessage::id).collect::<Vec<_>>(),
        vec![22, 23]
    );
    let stale_token = first[0]
        .claim_token()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed message has no token"))?
        .to_owned();
    tokio::time::sleep(Duration::from_millis(30)).await;

    let recovered = outbox
        .claim_for("worker", 2, Duration::from_secs(10))
        .await?;
    assert_eq!(
        recovered.iter().map(OutboxMessage::id).collect::<Vec<_>>(),
        vec![22, 23]
    );
    let current_token = recovered[0]
        .claim_token()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "recovered message has no token"))?;
    assert_ne!(current_token, stale_token);
    outbox.ack("worker", 22, &stale_token).await?;
    assert!(!outbox.cancel(22).await?);
    outbox.ack("worker", 22, current_token).await?;

    let other_token = recovered[1].claim_token().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "second claimed message has no token")
    })?;
    outbox.ack("worker", 23, other_token).await?;
    assert_eq!(outbox.list_published(10).await?.len(), 2);
    assert!(outbox.cleanup_published(Duration::ZERO, 1).await? <= 1);
    let retained = outbox.list_published(10).await?.len();
    assert_eq!(
        outbox.cleanup_published(Duration::ZERO, 10).await?,
        retained
    );
    assert!(outbox.list_published(10).await?.is_empty());
    assert_eq!(
        outbox
            .claim("worker", 10)
            .await?
            .iter()
            .map(OutboxMessage::id)
            .collect::<Vec<_>>(),
        vec![24]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn flows_create_claim_heartbeat_and_prune_terminal_states() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let flows = NatsFlows::connect(server.url(), unique("CATGA_FLOWS_LIFECYCLE")).await?;
    let stale = FlowState::new("payment-19", "payment", b"input".to_vec(), "worker-a")
        .heartbeated_at(UNIX_EPOCH);
    assert!(flows.create(stale.clone()).await?);
    assert!(!flows.create(stale.clone()).await?);
    assert!(
        flows
            .try_claim("refund", "worker-b", Duration::ZERO)
            .await?
            .is_none()
    );

    let claimed = flows
        .try_claim("payment", "worker-b", Duration::from_secs(1))
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "expected stale NATS flow claim"))?;
    assert_eq!(claimed.id(), stale.id());
    assert_eq!(claimed.owner(), Some("worker-b"));
    assert!(
        !flows
            .heartbeat(claimed.id(), "worker-a", claimed.version())
            .await?
    );
    assert!(
        flows
            .heartbeat(claimed.id(), "worker-b", claimed.version())
            .await?
    );
    assert!(
        !flows
            .update(claimed.version() - 1, claimed.clone().next_version()?)
            .await?
    );

    let current = flows
        .get(claimed.id())
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed NATS flow disappeared"))?;
    let terminal = current.clone().done(1).next_version()?;
    assert!(flows.update(current.version(), terminal).await?);
    assert!(
        flows
            .try_claim("payment", "worker-c", Duration::ZERO)
            .await?
            .is_none()
    );
    assert_eq!(
        flows.get(claimed.id()).await?.map(|state| state.status()),
        Some(FlowStatus::Done)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn suspended_flows_track_wait_correlations_across_lifecycle_changes() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSuspendedFlows::connect(server.url(), unique("CATGA_SUSPENDED_FLOWS")).await?;
    let continuation = FlowContinuation::waiting(
        FlowState::new("payment-20", "payment", b"input".to_vec(), "worker-a").suspended(),
        "charge",
        WaitCondition::new(
            "payment-20-correlation",
            WaitPolicy::All,
            2,
            UNIX_EPOCH,
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(continuation.clone()).await?);
    assert!(!store.create(continuation.clone()).await?);
    assert_eq!(
        store
            .get_by_wait_correlation("payment-20-correlation")
            .await?
            .as_ref()
            .map(|value| value.state().id()),
        Some(continuation.state().id())
    );
    assert_eq!(
        store
            .query(&FlowQuery::new(10, 10)?.with_status(FlowStatus::Suspended))
            .await?
            .iter()
            .map(|summary| summary.id())
            .collect::<Vec<_>>(),
        vec![continuation.state().id()]
    );
    assert!(
        store
            .record_wait_success("payment-20", 0, "child-a", b"accepted".to_vec())
            .await?
    );
    assert!(
        store
            .record_wait_failure(
                "payment-20",
                0,
                "child-b",
                CatgaError::new(ErrorCode::Transient, "unavailable"),
            )
            .await?
    );
    let stale_claim = continuation.clone().with_state(
        continuation
            .state()
            .clone()
            .claimed_by("worker-b")
            .next_version()?,
    );
    assert!(!store.claim(&continuation, stale_claim).await?);
    let current = store
        .get("payment-20")
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "NATS suspended flow disappeared"))?;
    assert_eq!(current.wait().map(|wait| wait.completed_count()), Some(2));
    let claimed = current.clone().with_state(
        current
            .state()
            .clone()
            .claimed_by("worker-b")
            .next_version()?,
    );
    assert!(store.claim(&current, claimed.clone()).await?);
    assert!(!store.heartbeat("payment-20", "worker-a", 1).await?);
    assert!(store.heartbeat("payment-20", "worker-b", 1).await?);

    let ready = store.get("payment-20").await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "claimed NATS suspended flow disappeared",
        )
    })?;
    let running = ready
        .clone()
        .ready()
        .with_state(ready.state().clone().running().next_version()?);
    assert!(store.update(ready.state().version(), running).await?);
    assert!(
        store
            .get_by_wait_correlation("payment-20-correlation")
            .await?
            .is_none()
    );
    assert!(store.delete("payment-20", 2).await?);
    assert!(store.get("payment-20").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn outbox_persists_published_messages_and_recovers_legacy_records() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let bucket = unique("CATGA_OUTBOX");
    let context = jetstream::new(
        async_nats::connect(server.url())
            .await
            .map_err(|error| test_error("connect legacy outbox publisher", error))?,
    );
    let raw_store = context
        .create_key_value(kv::Config {
            bucket: bucket.clone(),
            history: 1,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create legacy outbox bucket", error))?;
    let legacy_envelope = envelope(9, QualityOfService::AtLeastOnce);
    let payload = MemoryPackCodec::default().encode(&legacy_envelope)?;
    let owner = b"legacy-worker";
    let mut legacy = Vec::with_capacity(2 + owner.len() + payload.len());
    legacy.extend_from_slice(
        &u16::try_from(owner.len())
            .map_err(|error| test_error("encode legacy outbox owner length", error))?
            .to_be_bytes(),
    );
    legacy.extend_from_slice(owner);
    legacy.extend_from_slice(&payload);
    raw_store
        .create("legacy-9", legacy.into())
        .await
        .map_err(|error| test_error("store legacy outbox record", error))?;

    let outbox = NatsOutbox::connect(server.url(), bucket).await?;
    assert!(matches!(
        outbox
            .enqueue(OutboxMessage::new(legacy_envelope.clone()))
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    let claimed = outbox.claim("worker-a", 1).await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id(), legacy_envelope.id());
    assert_eq!(claimed[0].state(), OutboxState::Claimed);
    assert_eq!(
        claimed[0].max_retries(),
        catga_core::DEFAULT_OUTBOX_MAX_RETRIES
    );
    let token = claimed[0].claim_token().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "claimed outbox message has no token")
    })?;
    outbox.ack("worker-a", legacy_envelope.id(), token).await?;
    let published = outbox.list_published(1).await?;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].state(), OutboxState::Published);
    assert!(published[0].published_at_unix_ms().is_some());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn projection_checkpoints_update_delete_and_remain_projection_isolated() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let checkpoints =
        NatsProjectionCheckpoints::connect(server.url(), unique("CATGA_PROJECTION_CHECKPOINTS"))
            .await?;

    let first = ProjectionCheckpoint::new("orders/audit", "order-21", 2);
    checkpoints.save(first).await?;
    checkpoints
        .save(ProjectionCheckpoint::new("orders/audit", "order-21", 7))
        .await?;
    checkpoints
        .save(ProjectionCheckpoint::new("orders/audit", "order-22", 3))
        .await?;
    checkpoints
        .save(ProjectionCheckpoint::new("orders/search", "order-21", 5))
        .await?;

    let updated = checkpoints
        .load("orders/audit", "order-21")
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "missing updated NATS checkpoint"))?;
    assert_eq!(updated.version(), 7);
    assert_eq!(updated.projection_name(), "orders/audit");
    assert_eq!(updated.stream_id(), "order-21");
    assert!(updated.updated_at() >= UNIX_EPOCH);
    assert!(checkpoints.load("orders/audit", "missing").await?.is_none());

    checkpoints.delete("orders/audit", "order-21").await?;
    checkpoints.delete("orders/audit", "order-21").await?;
    assert!(
        checkpoints
            .load("orders/audit", "order-21")
            .await?
            .is_none()
    );
    assert_eq!(
        checkpoints
            .load("orders/audit", "order-22")
            .await?
            .map(|checkpoint| checkpoint.version()),
        Some(3)
    );

    checkpoints.delete_all("orders/audit").await?;
    checkpoints.delete_all("orders/audit").await?;
    assert!(
        checkpoints
            .load("orders/audit", "order-22")
            .await?
            .is_none()
    );
    assert_eq!(
        checkpoints
            .load("orders/search", "order-21")
            .await?
            .map(|checkpoint| checkpoint.version()),
        Some(5)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn flow_store_concurrent_create_rejects_stale_cas_and_fences_heartbeats() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let flows = NatsFlows::connect(server.url(), unique("CATGA_FLOWS_BOUNDARY")).await?;
    let flow_type = unique("flow-type");
    let flow_id = unique("flow-id");
    let initial = FlowState::new(flow_id.as_str(), flow_type.as_str(), [], "node-a");
    let (first, second) =
        tokio::join!(flows.create(initial.clone()), flows.create(initial.clone()));
    assert_eq!(usize::from(first?) + usize::from(second?), 1);
    assert!(flows.get("missing-flow").await?.is_none());
    assert!(!flows.update(0, initial.clone()).await?);
    assert!(
        !flows
            .update(
                0,
                FlowState::new("missing-flow", flow_type.as_str(), [], "node-a").next_version()?,
            )
            .await?
    );

    let fresh_id = unique("fresh-flow");
    assert!(
        flows
            .create(FlowState::new(
                fresh_id.as_str(),
                flow_type.as_str(),
                [],
                "node-a",
            ))
            .await?
    );
    assert!(
        flows
            .try_claim(flow_type.as_str(), "worker-a", Duration::from_secs(1))
            .await?
            .is_none()
    );

    let stale_id = unique("stale-flow");
    assert!(
        flows
            .create(
                FlowState::new(stale_id.as_str(), flow_type.as_str(), [], "node-a")
                    .heartbeated_at(UNIX_EPOCH),
            )
            .await?
    );
    let claimed = flows
        .try_claim(flow_type.as_str(), "worker-a", Duration::from_secs(1))
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "expected stale flow claim"))?;
    assert_eq!(claimed.id(), stale_id);
    assert!(
        !flows
            .heartbeat(claimed.id(), "worker-b", claimed.version())
            .await?
    );
    assert!(
        flows
            .heartbeat(claimed.id(), "worker-a", claimed.version())
            .await?
    );
    assert!(
        !flows
            .heartbeat("missing-flow", "worker-a", claimed.version())
            .await?
    );
    let current = flows
        .get(claimed.id())
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed flow disappeared"))?;
    assert_eq!(current.version(), claimed.version());
    assert_eq!(current.owner(), Some("worker-a"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn outbox_and_scheduler_recover_released_or_expired_leases_with_owner_fences()
-> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let outbox = NatsOutbox::connect(server.url(), unique("CATGA_OUTBOX_FENCES")).await?;
    let message = OutboxMessage::new(envelope(101, QualityOfService::AtLeastOnce));
    let (first, second) = tokio::join!(outbox.enqueue(message.clone()), outbox.enqueue(message));
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.err().or_else(|| second.err()),
        Some(error) if error.code() == ErrorCode::Conflict
    ));
    assert_validation(
        outbox
            .claim_for("worker-a", usize::MAX, Duration::from_secs(1))
            .await,
    );

    let first_claim = outbox
        .claim_for("worker-a", 1, Duration::from_secs(1))
        .await?
        .pop()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "expected outbox claim"))?;
    let first_token = first_claim
        .claim_token()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "outbox claim has no token"))?
        .to_owned();
    outbox.ack("worker-b", 101, first_token.as_str()).await?;
    outbox
        .release("worker-b", 101, first_token.as_str())
        .await?;
    assert!(outbox.claim("worker-b", 1).await?.is_empty());
    outbox
        .release("worker-a", 101, first_token.as_str())
        .await?;
    let released = outbox.claim("worker-b", 1).await?.pop().ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "released outbox claim was not recovered",
        )
    })?;
    let released_token = released.claim_token().ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "released outbox claim has no token")
    })?;
    assert_ne!(released_token, first_token);
    outbox.ack("worker-b", 101, released_token).await?;
    assert_eq!(outbox.list_published(1).await?[0].id(), 101);

    let scheduler =
        NatsFlowScheduler::connect(server.url(), unique("CATGA_SCHEDULER_FENCES")).await?;
    let due = UNIX_EPOCH + Duration::from_secs(10);
    let schedule_id = scheduler
        .schedule_resume("payment-fence", "charge", due)
        .await?;
    assert!(matches!(
        scheduler
            .schedule_resume("payment-fence", "charge", due)
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    let first_claim = scheduler
        .claim_due("worker-a", due, Duration::from_secs(1), 1)
        .await?;
    assert_eq!(first_claim.len(), 1);
    let recovered = scheduler
        .claim_due(
            "worker-b",
            due + Duration::from_secs(2),
            Duration::from_secs(5),
            1,
        )
        .await?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].schedule_id(), schedule_id.as_ref());
    assert!(
        !scheduler
            .renew_due(
                "worker-a",
                &schedule_id,
                due + Duration::from_secs(2),
                Duration::from_secs(1),
            )
            .await?
    );
    assert!(!scheduler.release_due("worker-a", &schedule_id).await?);
    assert!(scheduler.release_due("worker-b", &schedule_id).await?);
    assert_eq!(
        scheduler
            .claim_due(
                "worker-c",
                due + Duration::from_secs(2),
                Duration::from_secs(1),
                1
            )
            .await?
            .len(),
        1
    );
    assert!(scheduler.ack_due("worker-c", &schedule_id).await?);
    assert!(!scheduler.cancel_resume(&schedule_id).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn suspended_flows_fence_stale_snapshots_and_recover_timeout_receipts() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSuspendedFlows::connect(server.url(), unique("CATGA_SUSPENDED_FENCES")).await?;
    let plain_id = unique("plain-continuation");
    let plain = FlowContinuation::new(
        FlowState::new(plain_id.as_str(), "continuation-fence", [], "node-a").suspended(),
        "resume",
    );
    assert!(store.create(plain.clone()).await?);
    assert!(
        !store
            .record_wait_success(plain_id.as_str(), 0, "child", vec![])
            .await?
    );
    assert!(
        !store
            .record_wait_failure(
                plain_id.as_str(),
                0,
                "child",
                CatgaError::new(ErrorCode::Transient, "unused"),
            )
            .await?
    );
    assert!(!store.delete(plain_id.as_str(), 1).await?);
    assert!(store.delete(plain_id.as_str(), 0).await?);

    let correlation = unique("shared-correlation");
    let timeout_id = unique("timeout-continuation");
    let timeout = FlowContinuation::waiting(
        FlowState::new(timeout_id.as_str(), "continuation-fence", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            correlation.as_str(),
            WaitPolicy::All,
            1,
            UNIX_EPOCH,
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(timeout.clone()).await?);
    let conflicting_id = unique("conflicting-continuation");
    let conflicting = FlowContinuation::waiting(
        FlowState::new(conflicting_id.as_str(), "continuation-fence", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            correlation.as_str(),
            WaitPolicy::All,
            1,
            UNIX_EPOCH,
            Duration::from_secs(1),
        ),
    );
    assert!(store.create(conflicting.clone()).await?);
    assert_eq!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await
            .expect_err("multiple active waits must be rejected")
            .code(),
        ErrorCode::Conflict
    );
    assert!(store.delete(conflicting_id.as_str(), 0).await?);
    assert_eq!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await?
            .as_ref()
            .map(|value| value.state().id()),
        Some(timeout_id.as_str())
    );
    assert!(store.delete(timeout_id.as_str(), 0).await?);

    let timeout_store =
        NatsSuspendedFlows::connect(server.url(), unique("CATGA_TIMEOUT_RECEIPTS")).await?;
    let poll = TimedOutFlowPoll::new(UNIX_EPOCH + Duration::from_secs(1), 1, 4)?;
    assert!(timeout_store.create(timeout.clone()).await?);
    let receipt = timeout_store
        .poll_timed_out(&poll)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "expected timeout receipt"))?;
    assert_eq!(receipt.flow_id(), timeout_id);
    assert_eq!(
        timeout_store
            .ack_timed_out(&TimedOutFlowReceipt::new(timeout_id.as_str(), [0xff]))
            .await
            .expect_err("non-UTF-8 timeout receipt must be rejected")
            .code(),
        ErrorCode::Validation
    );
    timeout_store.release_timed_out(&receipt).await?;
    let mut recovered = None;
    for _ in 0..20 {
        if let Some(receipt) = timeout_store
            .poll_timed_out(&poll)
            .await?
            .into_iter()
            .next()
        {
            recovered = Some(receipt);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let recovered = recovered
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "released timeout receipt was lost"))?;
    assert_eq!(recovered.flow_id(), timeout_id);
    timeout_store.ack_timed_out(&recovered).await?;
    assert!(timeout_store.delete(timeout_id.as_str(), 0).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_fences_concurrent_appends_and_pages_public_history() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsEventStore::connect(
        server.url(),
        unique("CATGA_EVENT_BOUNDARY"),
        unique("catga.event"),
    )
    .await?;
    assert_validation(store.append("bad.*", Vec::new(), None).await);
    assert_validation(store.read_page("orders", 0, 0).await);
    assert_eq!(store.append("orders-a", Vec::new(), Some(7)).await?, -1);
    assert_eq!(
        store
            .append(
                "orders-a",
                vec![
                    envelope(201, QualityOfService::AtLeastOnce),
                    envelope(202, QualityOfService::AtLeastOnce),
                    envelope(203, QualityOfService::AtLeastOnce),
                ],
                Some(-1),
            )
            .await?,
        2
    );
    assert!(matches!(
        store
            .append(
                "orders-a",
                vec![envelope(204, QualityOfService::AtLeastOnce)],
                Some(-1),
            )
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    let (first, second) = tokio::join!(
        store.append(
            "orders-a",
            vec![envelope(204, QualityOfService::AtLeastOnce)],
            Some(2),
        ),
        store.append(
            "orders-a",
            vec![envelope(205, QualityOfService::AtLeastOnce)],
            Some(2),
        ),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.err().or_else(|| second.err()),
        Some(error) if error.code() == ErrorCode::Conflict
    ));
    assert_eq!(store.version("orders-a").await?, 3);

    let page = store.read_page("orders-a", 1, 1).await?;
    assert_eq!(page.stream().events()[0].version(), 1);
    assert_eq!(page.next_version(), Some(2));
    let bounded = store.read_to_version_page("orders-a", 0, 1, 10).await?;
    assert_eq!(bounded.stream().events().len(), 2);
    assert!(bounded.next_version().is_none());
    let historical = store.version_history_page("orders-a", 0, 2).await?;
    assert_eq!(historical.entries().len(), 2);
    assert_eq!(historical.next_version(), Some(2));
    let future = std::time::SystemTime::now() + Duration::from_secs(1);
    let timed = store.read_to_time_page("orders-a", 0, future, 10).await?;
    assert_eq!(timed.stream().events().len(), 4);

    assert_eq!(
        store
            .append(
                "orders-b",
                vec![envelope(206, QualityOfService::AtLeastOnce)],
                Some(-1),
            )
            .await?,
        0
    );
    let first_ids = store.stream_ids_page(None, 1).await?;
    assert_eq!(first_ids.ids().len(), 1);
    let cursor = first_ids
        .next_stream_id()
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "missing stream-id cursor"))?;
    let second_ids = store.stream_ids_page(Some(cursor), 10).await?;
    assert_eq!(second_ids.ids().len(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn enhanced_snapshots_preserve_history_under_concurrent_writers_and_cleanup()
-> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let snapshots = Arc::new(
        NatsEnhancedSnapshots::<u64>::connect(server.url(), unique("CATGA_ENHANCED_SNAPSHOTS"))
            .await?,
    );

    assert!(snapshots.load::<u64>("missing").await?.is_none());
    assert!(snapshots.history("missing").await?.is_empty());
    snapshots.delete("missing").await?;

    for (version, state) in [(1, 10_u64), (3, 30), (5, 50)] {
        snapshots
            .save(Snapshot::new("account-history", state, version))
            .await?;
    }
    let at_four = snapshots
        .load_at_version::<u64>("account-history", 4)
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "historical snapshot is missing"))?;
    assert_eq!((*at_four.state(), at_four.version()), (30, 3));

    let first = Arc::clone(&snapshots);
    let second = Arc::clone(&snapshots);
    let (first, second) = tokio::join!(
        first.save(Snapshot::new("account-history", 60_u64, 6)),
        second.save(Snapshot::new("account-history", 61_u64, 6)),
    );
    first?;
    second?;
    assert!(matches!(
        snapshots
            .save(Snapshot::new("account-history", 40_u64, 4))
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert_eq!(
        snapshots
            .load::<String>("account-history")
            .await
            .expect_err("a store for u64 snapshots must reject String reads")
            .code(),
        ErrorCode::Validation
    );

    let history = snapshots.history("account-history").await?;
    assert_eq!(
        history
            .iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        vec![1, 3, 5, 6]
    );
    assert_eq!(
        snapshots
            .load::<u64>("account-history")
            .await?
            .map(|snapshot| snapshot.version()),
        Some(6)
    );

    snapshots
        .delete_before_version("account-history", 3)
        .await?;
    snapshots.cleanup("account-history", 1).await?;
    assert_eq!(
        snapshots
            .history("account-history")
            .await?
            .iter()
            .map(|snapshot| snapshot.version())
            .collect::<Vec<_>>(),
        vec![6]
    );
    snapshots.cleanup("account-history", 0).await?;
    assert!(snapshots.load::<u64>("account-history").await?.is_none());
    snapshots.delete("account-history").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn outbox_concurrent_claims_are_exclusive_and_cleanup_validates_boundaries() -> CatgaResult<()>
{
    let server = NatsServer::start().await?;
    let outbox = Arc::new(NatsOutbox::connect(server.url(), unique("CATGA_OUTBOX_RACE")).await?);
    for id in [310, 311, 312, 313] {
        outbox
            .enqueue(OutboxMessage::new(envelope(
                id,
                QualityOfService::AtLeastOnce,
            )))
            .await?;
    }
    assert!(
        outbox
            .claim_for("worker-a", 0, Duration::from_secs(1))
            .await?
            .is_empty()
    );

    let first = Arc::clone(&outbox);
    let second = Arc::clone(&outbox);
    let (first, second) = tokio::join!(
        first.claim_for("worker-a", 4, Duration::from_secs(5)),
        second.claim_for("worker-b", 4, Duration::from_secs(5)),
    );
    let first = first?;
    let second = second?;
    let mut claimed_ids = first
        .iter()
        .chain(&second)
        .map(OutboxMessage::id)
        .collect::<Vec<_>>();
    claimed_ids.sort_unstable();
    assert_eq!(claimed_ids, vec![310, 311, 312, 313]);

    for (owner, claims) in [("worker-a", &first), ("worker-b", &second)] {
        for claim in claims {
            let token = claim.claim_token().ok_or_else(|| {
                CatgaError::new(ErrorCode::Internal, "concurrent outbox claim has no token")
            })?;
            outbox
                .record_failure("wrong-owner", claim.id(), token, "must be fenced")
                .await?;
            outbox.ack(owner, claim.id(), token).await?;
        }
    }
    assert!(outbox.list_published(0).await?.is_empty());
    assert_eq!(outbox.list_published(10).await?.len(), 4);
    assert_validation(outbox.cleanup_published(Duration::MAX, 1).await);
    assert_eq!(outbox.cleanup_published(Duration::ZERO, 10).await?, 4);
    assert!(outbox.list_published(10).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn flow_store_claims_every_stale_flow_across_type_index_pages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let flows = NatsFlows::connect(server.url(), unique("CATGA_FLOWS_PAGED_INDEX")).await?;
    let flow_type = unique("paged-flow-type");
    let mut ids = Vec::new();
    for position in 0..33 {
        let id = unique(&format!("paged-flow-{position}"));
        assert!(
            flows
                .create(
                    FlowState::new(id.as_str(), flow_type.as_str(), [], "node-a")
                        .heartbeated_at(UNIX_EPOCH),
                )
                .await?
        );
        ids.push(id);
    }

    let mut claimed = Vec::new();
    for claim_index in 0..33 {
        let flow = flows
            .try_claim(flow_type.as_str(), "recoverer", Duration::from_secs(1))
            .await?
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Internal,
                    format!("stale indexed flow was skipped at claim {claim_index}"),
                )
            })?;
        claimed.push(flow.id().to_owned());
    }
    claimed.sort_unstable();
    ids.sort_unstable();
    assert_eq!(claimed, ids);
    assert!(
        flows
            .try_claim(
                flow_type.as_str(),
                "recoverer",
                Duration::from_secs(60 * 60),
            )
            .await?
            .is_none()
    );

    let current = flows
        .get(&claimed[0])
        .await?
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "claimed flow is missing"))?;
    assert!(
        flows
            .update(current.version(), current.clone().done(1).next_version()?,)
            .await?
    );
    assert_eq!(
        flows.get(&claimed[0]).await?.map(|state| state.status()),
        Some(FlowStatus::Done)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn scheduler_skips_cancelled_index_entries_before_claiming_live_work() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let scheduler =
        NatsFlowScheduler::connect(server.url(), unique("CATGA_SCHEDULER_CANCELLED")).await?;
    let due = UNIX_EPOCH + Duration::from_secs(500);
    let cancelled = scheduler
        .schedule_resume("flow-cancelled", "state-cancelled", due)
        .await?;
    let live = scheduler
        .schedule_resume("flow-live", "state-live", due)
        .await?;
    assert!(scheduler.cancel_resume(&cancelled).await?);

    let claimed = scheduler
        .claim_due("worker", due, Duration::from_secs(10), 2)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].schedule_id(), live.as_ref());
    assert!(scheduler.ack_due("worker", &live).await?);
    assert!(
        scheduler
            .claim_due("worker", due, Duration::from_secs(10), 2)
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn suspended_flow_wait_correlation_capacity_is_bounded_and_released() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsSuspendedFlows::connect(server.url(), unique("CATGA_SUSPENDED_CAPACITY")).await?;
    let correlation = unique("bounded-correlation");
    let mut ids = Vec::new();
    for position in 0..16 {
        let id = unique(&format!("bounded-wait-{position}"));
        let continuation = FlowContinuation::waiting(
            FlowState::new(id.as_str(), "bounded-wait", [], "node-a").suspended(),
            "resume",
            WaitCondition::new(
                correlation.as_str(),
                WaitPolicy::All,
                1,
                UNIX_EPOCH,
                Duration::from_secs(60),
            ),
        );
        assert!(store.create(continuation).await?);
        ids.push(id);
    }
    let overflow = FlowContinuation::waiting(
        FlowState::new("overflow-wait", "bounded-wait", [], "node-a").suspended(),
        "resume",
        WaitCondition::new(
            correlation.as_str(),
            WaitPolicy::All,
            1,
            UNIX_EPOCH,
            Duration::from_secs(60),
        ),
    );
    assert!(matches!(
        store.create(overflow).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert_eq!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await
            .expect_err("ambiguous correlation must not choose a continuation")
            .code(),
        ErrorCode::Conflict
    );
    for id in ids {
        assert!(store.delete(&id, 0).await?);
    }
    assert!(
        store
            .get_by_wait_correlation(correlation.as_str())
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dead_letters_reject_malformed_jetstream_records() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let stream = unique("CATGA_DLQ_MALFORMED");
    let subject = unique("catga.dlq.malformed");
    let letters = NatsDeadLetters::connect(server.url(), stream, subject.clone()).await?;
    let publisher = jetstream::new(
        async_nats::connect(server.url())
            .await
            .map_err(|error| test_error("connect malformed dead-letter publisher", error))?,
    );
    publisher
        .publish(format!("{subject}.broken"), vec![1, 2, 3].into())
        .await
        .map_err(|error| test_error("publish malformed dead-letter record", error))?
        .await
        .map_err(|error| test_error("confirm malformed dead-letter record", error))?;
    assert_eq!(
        letters
            .list(1)
            .await
            .expect_err("malformed persisted dead-letter data must not decode")
            .code(),
        ErrorCode::Internal
    );
    Ok(())
}
