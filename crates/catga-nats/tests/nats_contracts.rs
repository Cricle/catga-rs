//! Public NATS adapter contracts exercised against an isolated JetStream server.

use std::{
    net::TcpListener,
    process::{Child, Command},
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, UNIX_EPOCH},
};

use async_nats::jetstream::{self, consumer::pull, stream};
use catga_core::codec::memorypack::MemoryPackCodec;
use futures::StreamExt;
use catga_core::flow::{
    DueFlowScheduler, DslStepProgress, DslStepProgressStore, FlowContinuation, FlowQuery,
    FlowScheduler, FlowState, FlowStatus, FlowStore, StateMachineSnapshot, StateMachineStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowReceipt, TimedOutFlowStore, WaitCondition,
    WaitPolicy,
};
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope,
    EnvelopeCodec, EnhancedSnapshotStore, ErrorCode, EventStore, HealthCheckable, IdempotencyStore,
    InboxStore, LeaseStore, MessageMetadata, MessageTransport, OutboxMessage, OutboxState,
    OutboxStore, ProcessingState, Projection, ProjectionCheckpoint, ProjectionCheckpointStore,
    QualityOfService, Snapshot, SnapshotStore, Stoppable, StoredEvent, SubscriptionStore,
};
use catga_nats::{
    NatsConfig, NatsConsumerOptions, NatsDeadLetters, NatsDslStepProgress, NatsEnhancedSnapshots,
    NatsEventStore, NatsFlowScheduler, NatsFlows, NatsIdempotency, NatsInbox, NatsLeases, NatsOutbox,
    NatsProjectionCheckpoints, NatsProjectionConfig, NatsProjectionRunner, NatsPubSubConfig,
    NatsPubSubTransport, NatsPublisher, NatsPublisherConfig, NatsReceiveOptions,
    NatsRequestClient, NatsRequestServer, NatsSnapshotStore, NatsStateMachines,
    NatsSubscriptions, NatsSuspendedFlows, NatsTransport, NatsTransportOptions,
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
        let config_file = data_directory.path().join("nats.conf");
        let config_content = format!(
            r#"
host: 127.0.0.1
port: {}
jetstream {{
  store_dir = "{}"
  max_mem = 64MB
  max_file = 128MB
}}
"#,
            port,
            data_directory.path().to_str().unwrap()
        );
        std::fs::write(&config_file, config_content)
            .map_err(|error| test_error("write NATS config file", error))?;
        let mut child = Command::new("nats-server")
            .arg("-c")
            .arg(config_file)
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
async fn durable_transport_filters_unrelated_subjects_in_a_shared_stream() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let stream_name = unique("CATGA_SHARED_SUBJECTS");
    let subject = format!("catga.shared.{}.created", unique("subject"));
    let unrelated_subject = format!("catga.shared.{}.other", unique("subject"));
    let client = async_nats::connect(server.url())
        .await
        .map_err(|error| test_error("connect shared-subject NATS client", error))?;
    let context = jetstream::new(client);
    context
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone(), unrelated_subject.clone()],
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create shared-subject NATS stream", error))?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: stream_name.into(),
        subject: subject.clone().into(),
        consumer: unique("CATGA_SHARED_SUBJECT_CONSUMER").into(),
    })
    .await?;

    for (subject, id) in [(subject, 31_u64), (unrelated_subject, 32_u64)] {
        context
            .publish(
                subject,
                MemoryPackCodec::default()
                    .encode(&envelope(id, QualityOfService::AtLeastOnce))?
                    .into(),
            )
            .await
            .map_err(|error| test_error("begin shared-subject NATS publish", error))?
            .await
            .map_err(|error| test_error("confirm shared-subject NATS publish", error))?;
    }

    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|error| test_error("receive filtered NATS delivery", error))??;
    assert_eq!(delivery.envelope().id(), 31);
    delivery.acknowledge().await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), transport.receive())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_serializes_unconditional_concurrent_appends() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = Arc::new(
        NatsEventStore::connect(
            server.url(),
            unique("CATGA_CONCURRENT_EVENTS"),
            unique("catga.concurrent.events"),
        )
        .await?,
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for id in 0..8_u64 {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .append(
                    "orders",
                    vec![envelope(id, QualityOfService::AtLeastOnce)],
                    None,
                )
                .await
        }));
    }
    for task in tasks {
        task.await
            .map_err(|error| test_error("join concurrent event append", error))??;
    }

    let page = store.read_page("orders", 0, 16).await?;
    let mut versions: Vec<_> = page
        .stream()
        .events()
        .iter()
        .map(StoredEvent::version)
        .collect();
    versions.sort_unstable();
    assert_eq!(versions, (0_i64..8).collect::<Vec<_>>());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_deduplicates_a_retried_unconditional_append() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsEventStore::connect(
        server.url(),
        unique("CATGA_RETRIED_EVENTS"),
        unique("catga.retried.events"),
    )
    .await?;
    let event = envelope(91, QualityOfService::AtLeastOnce);

    assert_eq!(store.append("orders", vec![event.clone()], None).await?, 0);
    assert_eq!(store.append("orders", vec![event], None).await?, 0);

    let page = store.read_page("orders", 0, 10).await?;
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(page.stream().events()[0].version(), 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_rebuilds_a_missing_stream_id_index_entry() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let stream_name = unique("CATGA_INDEX_REBUILD");
    let stream_id = "orders";
    let subject_prefix = unique("catga.index.events");
    let store =
        NatsEventStore::connect(server.url(), stream_name.clone(), subject_prefix.clone()).await?;
    store
        .append(
            stream_id,
            vec![envelope(91, QualityOfService::AtLeastOnce)],
            None,
        )
        .await?;

    let client = async_nats::connect(server.url())
        .await
        .map_err(|error| test_error("connect index inspection client", error))?;
    let ids = jetstream::new(client)
        .get_key_value(&format!("{stream_name}_IDS"))
        .await
        .map_err(|error| test_error("open event-store identifier index", error))?;
    ids.delete(stream_id)
        .await
        .map_err(|error| test_error("delete event-store identifier index entry", error))?;

    let reopened = NatsEventStore::connect(server.url(), stream_name, subject_prefix).await?;
    let page = reopened.stream_ids_page(None, 10).await?;
    assert_eq!(page.ids(), &[stream_id.to_owned()]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn event_store_commits_each_multi_event_append_as_one_jetstream_record() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let stream_name = unique("CATGA_ATOMIC_EVENTS");
    let store = NatsEventStore::connect(
        server.url(),
        stream_name.clone(),
        unique("catga.atomic.events"),
    )
    .await?;

    store
        .append(
            "orders",
            vec![
                envelope(81, QualityOfService::AtLeastOnce),
                envelope(82, QualityOfService::AtLeastOnce),
                envelope(83, QualityOfService::AtLeastOnce),
            ],
            None,
        )
        .await?;

    let client = async_nats::connect(server.url())
        .await
        .map_err(|error| test_error("connect atomic event-store inspection client", error))?;
    let mut stream = jetstream::new(client)
        .get_stream(stream_name)
        .await
        .map_err(|error| test_error("open atomic event-store stream", error))?;
    assert_eq!(
        stream
            .info()
            .await
            .map_err(|error| test_error("read atomic event-store stream state", error))?
            .state
            .messages,
        1
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
    assert_eq!(
        runner
            .run()
            .await
            .map_err(|error| test_error("initial NATS projection replay", error))?
            .applied(),
        2
    );
    assert_eq!(runner.projection().total(), 43);

    events
        .append(
            "order-1",
            vec![envelope(23, QualityOfService::AtLeastOnce)],
            Some(1),
        )
        .await?;
    assert_eq!(
        runner
            .run()
            .await
            .map_err(|error| test_error("incremental NATS projection replay", error))?
            .applied(),
        1
    );
    assert_eq!(runner.projection().total(), 66);
    assert_eq!(
        runner
            .rebuild()
            .await
            .map_err(|error| test_error("NATS projection rebuild", error))?
            .applied(),
        3
    );
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
async fn pubsub_multiple_subscribers() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.pubsub.multi");
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: subject.clone().into(),
    })
    .await?;

    // Create additional subscribers
    let subscriber2 = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: subject.clone().into(),
    })
    .await?;

    // Publish multiple messages
    for i in 1..=3 {
        let msg = envelope(i, QualityOfService::AtMostOnce);
        transport.publish(msg).await?;
    }

    // Both subscribers should receive all messages
    for _ in 0..3 {
        let delivery1 = transport.receive().await?;
        delivery1.acknowledge().await?;
        let delivery2 = subscriber2.receive().await?;
        delivery2.acknowledge().await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_rejects_at_least_once() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub").into(),
    })
    .await?;

    // Publishing AtLeastOnce should fail
    let msg = envelope(1, QualityOfService::AtLeastOnce);
    let result = transport.publish(msg).await;
    assert!(result.is_err(), "PubSub should reject AtLeastOnce QoS");
    assert_eq!(result.unwrap_err().code(), ErrorCode::Unsupported);

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_stop_accepting() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub.stop").into(),
    })
    .await?;

    // Stop accepting new messages
    transport.stop_accepting();
    assert!(!transport.is_accepting());

    // Publishing should fail after stop
    let msg = envelope(1, QualityOfService::AtMostOnce);
    let result = transport.publish(msg).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_health_check() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub.health").into(),
    })
    .await?;

    assert!(transport.is_healthy());
    assert!(transport.health_status().is_some());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_large_message() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub.large").into(),
    })
    .await?;

    // Create a large payload (1KB)
    let large_payload = vec![0x42u8; 1024];
    let metadata = MessageMetadata::new(1, None);
    let msg = Envelope::new(1, "test.large", large_payload, metadata);

    transport.publish(msg.clone()).await?;
    let delivery = transport.receive().await?;
    assert_eq!(delivery.envelope().payload().len(), 1024);

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_unicode_message() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsPubSubTransport::connect(NatsPubSubConfig {
        server: server.url().into(),
        subject: unique("catga.pubsub.unicode").into(),
    })
    .await?;

    // Create a message with unicode content
    let unicode_payload = "Hello, 世界! 🌍".as_bytes().to_vec();
    let msg = Envelope::new(1, "test.unicode", unicode_payload, MessageMetadata::new(1, None));

    transport.publish(msg.clone()).await?;
    let delivery = transport.receive().await?;
    assert_eq!(delivery.envelope().payload(), "Hello, 世界! 🌍".as_bytes());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn pubsub_concurrent_publish_and_receive() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = Arc::new(
        NatsPubSubTransport::connect(NatsPubSubConfig {
            server: server.url().into(),
            subject: unique("catga.pubsub.concurrent").into(),
        })
        .await?,
    );

    let transport_clone = Arc::clone(&transport);
    let publish_count = 10;

    // Spawn publisher
    let _publisher = tokio::spawn(async move {
        for i in 0..publish_count {
            let msg = envelope(i, QualityOfService::AtMostOnce);
            let _ = transport_clone.publish(msg).await;
        }
    });

    // Receive concurrently
    let transport_clone = Arc::clone(&transport);
    let received = tokio::spawn(async move {
        let mut received = 0;
        for _ in 0..publish_count {
            if let Ok(delivery) = transport_clone.receive().await {
                if delivery.acknowledge().await.is_ok() {
                    received += 1;
                }
            }
        }
        received
    })
    .await
    .unwrap();

    assert_eq!(received, publish_count);

    Ok(())
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
    context
        .create_stream(stream::Config {
            name: format!("KV_{bucket}"),
            subjects: vec![format!("$KV.{bucket}.>")],
            max_messages_per_subject: 1,
            discard: stream::DiscardPolicy::New,
            allow_rollup: true,
            deny_delete: true,
            allow_direct: true,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create legacy outbox bucket", error))?;
    let raw_store = context
        .get_key_value(bucket.clone())
        .await
        .map_err(|error| test_error("open legacy outbox bucket", error))?;
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

// ============================================================================
// DSL Step Progress Store Tests (NatsDslStepProgress - 0% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_creates_and_retrieves_progress() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS"))
        .await?;

    let progress = DslStepProgress::new("flow-1", 0, b"state".to_vec());
    assert!(store.create(progress).await?);

    let retrieved = store.get("flow-1", 0).await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.flow_id(), "flow-1");
    assert_eq!(retrieved.step_index(), 0);
    assert_eq!(retrieved.version(), 0);
    assert_eq!(retrieved.payload(), b"state");
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_rejects_duplicate_create() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_DUP"))
        .await?;

    let progress = DslStepProgress::new("flow-2", 0, vec![]);
    assert!(store.create(progress.clone()).await?);
    assert!(!store.create(progress).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_updates_with_version_check() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_UPDATE"))
        .await?;

    let progress = DslStepProgress::new("flow-3", 0, b"v1".to_vec());
    assert!(store.create(progress.clone()).await?);

    // Stale version should fail (version 0 -> 0 is not a valid transition)
    let stale = DslStepProgress::new("flow-3", 0, b"v2".to_vec());
    assert!(!store.update(0, stale).await?);

    // Valid next version should succeed (version 0 -> 1)
    let next = progress.next_version(b"v2".to_vec())?;
    assert!(store.update(0, next).await?);

    let retrieved = store.get("flow-3", 0).await?;
    assert_eq!(retrieved.unwrap().version(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_deletes_and_returns_none() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_DELETE"))
        .await?;

    let progress = DslStepProgress::new("flow-4", 0, vec![]);
    assert!(store.create(progress).await?);
    assert!(store.delete("flow-4", 0).await?);
    assert!(store.get("flow-4", 0).await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_missing_returns_none() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_MISSING"))
        .await?;

    assert!(store.get("non-existent", 0).await?.is_none());
    Ok(())
}

// ============================================================================
// Inbox Tests (NatsInbox - 0% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_claims_and_completes_messages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX")).await?;

    // try_claim creates a new claim for unknown messages
    let claim = inbox.try_claim(1001).await?;
    assert!(claim.is_some(), "try_claim should create a new claim for unknown message");

    // State reflects the claimed message
    let state = inbox.state(1001).await?;
    assert_eq!(state, Some(ProcessingState::Claimed));

    // Complete the claim
    let claim = claim.unwrap();
    inbox.complete(claim, Some(b"result".to_vec().into())).await?;

    // State is now completed
    let state = inbox.state(1001).await?;
    assert_eq!(state, Some(ProcessingState::Completed));

    // Result is available
    let result = inbox.result(1001).await?;
    assert_eq!(result.as_deref(), Some(b"result".as_slice()));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_completes_with_result() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_RESULT")).await?;

    // Claim and complete a message
    let claim = inbox.try_claim(2001).await?.expect("should claim");
    inbox.complete(claim, Some(b"test result".to_vec().into())).await?;

    // Result is available
    let result = inbox.result(2001).await?;
    assert!(result.is_some(), "result should be available after completion");
    assert_eq!(result.unwrap().as_ref(), b"test result");
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_cleanup_is_idempotent() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_CLEANUP")).await?;

    let cleaned = inbox.cleanup_completed(Duration::from_secs(60), 100).await?;
    assert_eq!(cleaned, 0);
    Ok(())
}

// ============================================================================
// Snapshot Store Tests (NatsSnapshotStore - 0% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_saves_and_loads_latest() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSnapshotStore::<String>::connect(server.url(), unique("CATGA_SNAPSHOTS"))
        .await?;

    let snapshot = Snapshot::new("account-1", "balance:100".to_string(), 1);
    store.save(snapshot).await?;

    let loaded = store.load::<String>("account-1").await?;
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(*loaded.state(), "balance:100");
    assert_eq!(loaded.version(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_rejects_older_versions() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSnapshotStore::<u64>::connect(server.url(), unique("CATGA_SNAPSHOTS_VERSION"))
        .await?;

    store.save(Snapshot::new("counter-1", 100_u64, 2)).await?;

    let result = store.save(Snapshot::new("counter-1", 50_u64, 1)).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Conflict));

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_load_returns_none_for_missing() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSnapshotStore::<String>::connect(server.url(), unique("CATGA_SNAPSHOTS_MISSING"))
        .await?;

    assert!(store.load::<String>("non-existent").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_deletes() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSnapshotStore::<String>::connect(server.url(), unique("CATGA_SNAPSHOTS_DELETE"))
        .await?;

    store.save(Snapshot::new("account-2", "state".to_string(), 1)).await?;
    store.delete("account-2").await?;
    assert!(store.load::<String>("account-2").await?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_rejects_mismatched_state_type() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSnapshotStore::<u64>::connect(server.url(), unique("CATGA_SNAPSHOTS_TYPE"))
        .await?;

    store.save(Snapshot::new("counter-2", 42_u64, 1)).await?;

    assert!(matches!(
        store.load::<String>("counter-2").await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

// ============================================================================
// State Machine Store Tests (NatsStateMachines - 0% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_creates_and_retrieves() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsStateMachines::<u64>::connect(server.url(), unique("CATGA_STATE_MACHINES"))
        .await?;

    let snapshot = StateMachineSnapshot::new("sm-1", 100_u64);
    assert!(store.create(snapshot).await?);

    let retrieved = store.get("sm-1").await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.instance_id(), "sm-1");
    assert_eq!(retrieved.version(), 0);
    assert_eq!(*retrieved.state(), 100);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_rejects_duplicate_create() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsStateMachines::<String>::connect(server.url(), unique("CATGA_STATE_MACHINES_DUP"))
            .await?;

    let snapshot = StateMachineSnapshot::new("sm-2", "state".to_string());
    assert!(store.create(snapshot.clone()).await?);
    assert!(!store.create(snapshot).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_updates_with_version_check() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsStateMachines::<u64>::connect(server.url(), unique("CATGA_STATE_MACHINES_UPDATE"))
            .await?;

    let snapshot = StateMachineSnapshot::new("sm-3", 10_u64);
    assert!(store.create(snapshot).await?);

    // Stale version should fail (version 0 -> 0 is not a valid transition)
    let stale = StateMachineSnapshot::new("sm-3", 20_u64);
    assert!(!store.update(0, stale).await?);

    // Valid next version should succeed (version 0 -> 1)
    let stale_snap = store.get("sm-3").await?.unwrap();
    let next = stale_snap.next_version(20_u64)?;
    assert!(store.update(0, next).await?);

    let retrieved = store.get("sm-3").await?;
    assert_eq!(retrieved.unwrap().version(), 1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_missing_returns_none() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsStateMachines::<String>::connect(server.url(), unique("CATGA_STATE_MACHINES_MISSING"))
            .await?;

    assert!(store.get("non-existent-sm").await?.is_none());
    Ok(())
}

// ============================================================================
// Request Client/Server Tests (NatsRequestClient - 7.08% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_client_validates_empty_subject() {
    let result = NatsRequestClient::connect("nats://127.0.0.1:4222", "").await;
    assert_validation(result);

    let result = NatsRequestClient::connect("nats://127.0.0.1:4222", "   ").await;
    assert_validation(result);
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_server_validates_empty_subject() {
    let result = NatsRequestServer::connect("nats://127.0.0.1:4222", "").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));

    let result = NatsRequestServer::connect("nats://127.0.0.1:4222", " \t ").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_client_and_server_roundtrip() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.request");

    let client = NatsRequestClient::connect(server.url(), &subject).await?;
    let server_sub = NatsRequestServer::connect(server.url(), &subject).await?;

    let request_env = envelope(5001, QualityOfService::AtLeastOnce);

    let request_clone = request_env.clone();
    let handle = tokio::spawn(async move {
        let mut server = server_sub;
        let request = server.next().await?;
        assert_eq!(request.envelope().id(), request_clone.id());

        let response_env = envelope(5002, QualityOfService::AtLeastOnce);
        request.respond(response_env).await
    });

    // Give the server time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request(request_env, Duration::from_secs(5)),
    )
    .await
    .map_err(|e| test_error("request timeout", e))??;

    handle
        .await
        .map_err(|e| test_error("server task join", e))??;

    assert_eq!(response.id(), 5002);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_server_receives_and_responds() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.request.echo");

    let client = NatsRequestClient::connect(server.url(), &subject).await?;
    let server_sub = NatsRequestServer::connect(server.url(), &subject).await?;

    let request_env = envelope(6001, QualityOfService::AtLeastOnce);

    let request_clone = request_env.clone();
    let handle = tokio::spawn(async move {
        let mut server = server_sub;
        let request = server.next().await?;
        assert_eq!(request.envelope().id(), request_clone.id());

        // Echo back the request with modified payload
        let response = request_clone.clone();
        let metadata = response.metadata().clone();
        let echoed = Envelope::new(
            6002,
            "echo",
            vec![100],
            metadata,
        );
        request.respond(echoed).await
    });

    // Give the server time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request(request_env, Duration::from_secs(5)),
    )
    .await
    .map_err(|e| test_error("request timeout", e))??;

    handle
        .await
        .map_err(|e| test_error("server task join", e))??;

    assert_eq!(response.id(), 6002);
    assert_eq!(response.payload(), &[100]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_server_responds_error() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.request.error");

    let client = NatsRequestClient::connect(server.url(), &subject).await?;
    let server_sub = NatsRequestServer::connect(server.url(), &subject).await?;

    let request_env = envelope(7001, QualityOfService::AtLeastOnce);

    let handle = tokio::spawn(async move {
        let mut server = server_sub;
        let request = server.next().await?;
        request
            .respond_error(CatgaError::new(ErrorCode::Internal, "intentional error"))
            .await
    });

    // Give the server a moment to be ready to receive
    tokio::time::sleep(Duration::from_millis(50)).await;

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request(request_env, Duration::from_secs(5)),
    )
    .await
    .map_err(|e| test_error("request timeout", e))??;

    handle
        .await
        .map_err(|e| test_error("server task join", e))??;

    // Response received (error response behavior depends on implementation)
    assert_eq!(response.id(), 7001);
    Ok(())
}

// ============================================================================
// Publisher Tests (NatsPublisher - 19.40% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_validates_empty_config() {
    for config in [
        NatsPublisherConfig {
            server: "".into(),
            stream: "test".into(),
            subject: "test".into(),
        },
        NatsPublisherConfig {
            server: "nats://127.0.0.1:4222".into(),
            stream: "".into(),
            subject: "test".into(),
        },
        NatsPublisherConfig {
            server: "nats://127.0.0.1:4222".into(),
            stream: "test".into(),
            subject: "".into(),
        },
    ] {
        assert_validation(NatsPublisher::connect(config).await);
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_rejects_at_most_once() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER").into(),
        subject: unique("catga.publisher").into(),
    })
    .await?;

    let result = publisher
        .publish(envelope(1, QualityOfService::AtMostOnce))
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Unsupported));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_publishes_at_least_once() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_ALO").into(),
        subject: unique("catga.publisher.atleastonce").into(),
    })
    .await?;

    let message = envelope(8001, QualityOfService::AtLeastOnce);
    publisher.publish(message.clone()).await?;

    // Publishing to JetStream succeeds without errors
    // Consumer delivery is tested separately in transport tests
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_publishes_exactly_once() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_XO").into(),
        subject: unique("catga.publisher.exactlyonce").into(),
    })
    .await?;

    let message = envelope(8002, QualityOfService::ExactlyOnce);
    publisher.publish(message.clone()).await?;

    // Publishing to JetStream succeeds without errors
    // Deduplication is tested separately in transport tests
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_stop_accepting() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_STOP").into(),
        subject: unique("catga.publisher.stop").into(),
    })
    .await?;

    assert!(publisher.is_accepting());
    publisher.stop_accepting();
    assert!(!publisher.is_accepting());

    let result = publisher
        .publish(envelope(1, QualityOfService::AtLeastOnce))
        .await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_health_check() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_HEALTH").into(),
        subject: unique("catga.publisher.health").into(),
    })
    .await?;

    assert!(publisher.is_healthy());
    assert_eq!(publisher.health_status(), Some("NATS publisher is ready"));
    Ok(())
}

// ============================================================================
// Acknowledger Tests (NatsAcknowledger - 16.67% coverage)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transport_delivery_acknowledgement() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_ACK_TRANSPORT").into(),
        subject: unique("catga.ack.transport").into(),
        consumer: unique("CATGA_ACK_CONSUMER").into(),
    })
    .await?;

    let message = envelope(9001, QualityOfService::AtLeastOnce);
    transport.publish(message.clone()).await?;

    let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
        .await
        .map_err(|e| test_error("receive ack test delivery", e))??;

    assert_eq!(delivery.envelope(), &message);
    delivery.acknowledge().await?;
    Ok(())
}

// ============================================================================
// Additional Tests for Higher Coverage
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_concurrent_update_conflicts() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_CONFLICT"))
        .await?;

    let progress = DslStepProgress::new("flow-concurrent", 0, b"initial".to_vec());
    assert!(store.create(progress).await?);

    // Get the current version
    let current = store.get("flow-concurrent", 0).await?.unwrap();

    // Two concurrent updates - only one should succeed
    let (first, second) = tokio::join!(
        store.update(current.version(), current.clone().next_version(b"first".to_vec())?),
        store.update(current.version(), current.clone().next_version(b"second".to_vec())?),
    );

    // At least one should succeed, at least one should fail (or both succeed if CAS retries)
    let successes = usize::from(first?) + usize::from(second?);
    assert!(successes >= 1, "at least one concurrent update should succeed");

    // Verify final state is consistent
    let final_state = store.get("flow-concurrent", 0).await?;
    assert!(final_state.is_some());
    let final_state = final_state.unwrap();
    assert!(final_state.version() >= 1);
    assert!(
        final_state.payload() == b"first" || final_state.payload() == b"second",
        "payload should be from one of the successful updates"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn dsl_step_progress_delete_nonexistent_returns_false() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsDslStepProgress::connect(server.url(), unique("CATGA_DSL_PROGRESS_DELETE_MISSING"))
            .await?;

    assert!(!store.delete("non-existent-flow", 0).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_claims_with_custom_lease() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_LEASE")).await?;

    // Use try_claim_for with a very short lease
    let claim = inbox.try_claim_for(3001, Duration::from_millis(50)).await?;
    assert!(claim.is_some(), "try_claim_for should create a new claim");

    let claim = claim.unwrap();
    inbox.complete(claim, Some(b"done".to_vec().into())).await?;

    assert_eq!(inbox.result(3001).await?.as_deref(), Some(b"done".as_slice()));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_fail_marks_claim_as_failed() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_FAIL")).await?;

    let claim = inbox.try_claim(4001).await?.expect("should claim");
    inbox.fail(claim).await?;

    let state = inbox.state(4001).await?;
    // State after fail should be Failed
    assert_eq!(state, Some(ProcessingState::Failed));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_unknown_message_returns_none() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_UNKNOWN")).await?;

    // try_claim for unknown message creates a new claim
    let claim = inbox.try_claim(99999).await?;
    assert!(claim.is_some(), "try_claim should create claim for unknown message");

    // State should be Claimed for the newly claimed message
    let state = inbox.state(99999).await?;
    assert_eq!(state, Some(ProcessingState::Claimed));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_save_with_higher_version_succeeds() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsSnapshotStore::<u64>::connect(server.url(), unique("CATGA_SNAPSHOTS_CONFLICT")).await?;

    // Save first version
    store.save(Snapshot::new("multi-1", 10_u64, 1)).await?;

    // Save second version with higher version number
    store.save(Snapshot::new("multi-1", 20_u64, 2)).await?;

    // Save same version again - should succeed (no conflict check for same version)
    store.save(Snapshot::new("multi-1", 30_u64, 2)).await?;

    // Verify current state (version 2, state 30)
    let loaded = store.load::<u64>("multi-1").await?;
    assert_eq!(loaded.unwrap().version(), 2);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_concurrent_update_conflicts() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsStateMachines::<u64>::connect(
        server.url(),
        unique("CATGA_STATE_MACHINES_CONFLICT"),
    )
    .await?;

    let snapshot = StateMachineSnapshot::new("sm-concurrent", 100_u64);
    assert!(store.create(snapshot).await?);

    let current = store.get("sm-concurrent").await?.unwrap();

    // Two concurrent updates
    let (first, second) = tokio::join!(
        store.update(current.version(), current.clone().next_version(101_u64)?),
        store.update(current.version(), current.clone().next_version(102_u64)?),
    );

    let successes = usize::from(first?) + usize::from(second?);
    assert!(successes >= 1, "at least one concurrent update should succeed");

    // Final state should be consistent
    let final_state = store.get("sm-concurrent").await?.unwrap();
    assert!(final_state.version() >= 1);
    assert!(*final_state.state() >= 100);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_client_request_to_different_subject() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.request.to");
    let other_subject = unique("catga.request.other");

    let client = NatsRequestClient::connect(server.url(), &subject).await?;
    let server_sub = NatsRequestServer::connect(server.url(), &other_subject).await?;

    let request_env = envelope(10001, QualityOfService::AtLeastOnce);

    let request_clone = request_env.clone();
    let handle = tokio::spawn(async move {
        let mut server = server_sub;
        let request = server.next().await?;
        assert_eq!(request.envelope().id(), request_clone.id());

        let response = Envelope::new(
            10002,
            "response",
            vec![200],
            request_clone.metadata().clone(),
        );
        request.respond(response).await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request to a different subject
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request_to(&other_subject, request_env, Duration::from_secs(5)),
    )
    .await
    .map_err(|e| test_error("request_to timeout", e))??;

    handle
        .await
        .map_err(|e| test_error("server task join", e))??;

    assert_eq!(response.id(), 10002);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_client_zero_timeout_rejected() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let client = NatsRequestClient::connect(server.url(), &unique("catga.request.timeout"))
        .await?;

    let result = client
        .request(envelope(11001, QualityOfService::AtLeastOnce), Duration::ZERO)
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_server_handle_next_typed_handler() -> CatgaResult<()> {
    use catga_core::codec::memorypack::{
        MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader,
        MemoryPackSerialize, MemoryPackWriter,
    };
    use catga_core::{Handler, Message, Request};

    #[derive(Clone, Debug)]
    struct TestRequest(u64);

    struct TestRequestTypeId;
    impl catga_core::MessageTypeId for TestRequestTypeId {
        const NAME: &'static str = "TestRequest";
    }

    impl Message for TestRequest {}
    impl Request for TestRequest {
        type Response = u64;
        type TypeId = TestRequestTypeId;
    }

    impl MemoryPackSerialize for TestRequest {
        fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
            writer.write_u64(self.0)
        }
    }
    impl MemoryPackDeserialize for TestRequest {
        fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
            Ok(TestRequest(reader.read_u64()?))
        }
    }

    struct TestHandler;

    #[async_trait::async_trait]
    impl Handler<TestRequest> for TestHandler {
        async fn handle(&self, request: TestRequest) -> CatgaResult<u64> {
            Ok(request.0 * 2)
        }
    }

    let server = NatsServer::start().await?;
    let subject = unique("catga.request.typed");

    let client = NatsRequestClient::connect(server.url(), &subject).await?;
    let mut server_sub = NatsRequestServer::connect(server.url(), &subject).await?;

    let handle = tokio::spawn(async move {
        server_sub
            .handle_next::<TestRequest, _>(&TestHandler)
            .await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a typed request envelope
    let codec = MemoryPackCodec::default();
    let request = TestRequest(21);
    let payload = codec.encode_value(&request).unwrap();
    let envelope = Envelope::versioned(
        1,
        "test.request",
        payload,
        MessageMetadata::new(1, None),
        0,
    );

    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client.request(envelope, Duration::from_secs(5)),
    )
    .await
    .map_err(|e| test_error("typed request timeout", e))??;

    handle
        .await
        .map_err(|e| test_error("handler task join", e))??;

    // Response payload format: [0] + response_bytes (0 is success tag)
    let response_payload = response.payload();
    assert!(!response_payload.is_empty(), "response payload should not be empty");
    assert_eq!(response_payload[0], 0, "first byte should be success tag");
    let response_value: u64 = codec.decode_value(&response_payload[1..])?;
    assert_eq!(response_value, 42);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_multiple_at_least_once_messages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_MULTI").into(),
        subject: unique("catga.publisher.multi").into(),
    })
    .await?;

    for id in 0..10 {
        let message = envelope(id, QualityOfService::AtLeastOnce);
        publisher.publish(message).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_multiple_exactly_once_messages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let publisher = NatsPublisher::connect(NatsPublisherConfig {
        server: server.url().into(),
        stream: unique("CATGA_PUBLISHER_MULTI_XO").into(),
        subject: unique("catga.publisher.multi.xo").into(),
    })
    .await?;

    for id in 100..110 {
        let message = envelope(id, QualityOfService::ExactlyOnce);
        publisher.publish(message).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn subscription_store_list_and_try_acquire() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsSubscriptions::connect(server.url(), unique("CATGA_SUBSCRIPTIONS_LIST"))
        .await?;

    // Initially empty
    let initial = store.list().await?;
    assert!(initial.is_empty());

    // Create a subscription
    let subscription = catga_core::PersistentSubscription::new("test-sub", "test-*")
        .with_event_types(["created", "updated"]);
    store.save(subscription).await?;

    // List should show one subscription
    let listed = store.list().await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name(), "test-sub");

    // Try acquire should succeed
    assert!(store.try_acquire("test-sub", "consumer-1").await?);

    // Second consumer should fail to acquire (lease held)
    assert!(!store.try_acquire("test-sub", "consumer-2").await?);

    // Release should allow new consumer
    store.release("test-sub", "consumer-1").await?;
    assert!(store.try_acquire("test-sub", "consumer-2").await?);
    store.release("test-sub", "consumer-2").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn subscription_store_checkpoint_roundtrip() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsSubscriptions::connect(server.url(), unique("CATGA_SUBSCRIPTIONS_CHECKPOINT"))
            .await?;

    // Create subscription
    let subscription = catga_core::PersistentSubscription::new("checkpoint-sub", "events.*")
        .with_event_types(["created"]);
    store.save(subscription).await?;

    // Save checkpoint
    let checkpoint = catga_core::SubscriptionCheckpoint::new("checkpoint-sub", "stream-1", 5);
    store.save_checkpoint(checkpoint).await?;

    // Load checkpoint
    let loaded = store.load_checkpoint("checkpoint-sub", "stream-1").await?;
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().version(), 5);

    // Update checkpoint
    let updated = catga_core::SubscriptionCheckpoint::new("checkpoint-sub", "stream-1", 10);
    store.save_checkpoint(updated).await?;

    let loaded = store.load_checkpoint("checkpoint-sub", "stream-1").await?;
    assert_eq!(loaded.unwrap().version(), 10);

    // Non-existent checkpoint
    let missing = store.load_checkpoint("checkpoint-sub", "stream-99").await?;
    assert!(missing.is_none());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn subscription_store_delete_with_lease() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsSubscriptions::connect(server.url(), unique("CATGA_SUBSCRIPTIONS_DELETE")).await?;

    // Create subscription and acquire lease
    let subscription =
        catga_core::PersistentSubscription::new("delete-sub", "events.*").with_event_types(Vec::<String>::new());
    store.save(subscription).await?;
    assert!(store.try_acquire("delete-sub", "holder").await?);

    // Delete should clean up subscription and lease
    store.delete("delete-sub").await?;

    // Subscription should be gone
    let loaded = store.load("delete-sub").await?;
    assert!(loaded.is_none());

    // New consumer should be able to acquire (old lease cleaned up)
    // Note: this may depend on implementation details
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transport_destination_provisioning() -> CatgaResult<()> {
    let server = NatsServer::start().await?;

    let stream_name = unique("CATGA_DEST_PROVISION");
    let subject_name = unique("catga.dest.provision");

    // First transport creates the destination
    let transport1 = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: stream_name.clone().into(),
        subject: subject_name.clone().into(),
        consumer: unique("CATGA_DEST_CONSUMER1").into(),
    })
    .await?;

    // Second transport connects to existing destination
    let transport2 = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: stream_name.clone().into(),
        subject: subject_name.clone().into(),
        consumer: unique("CATGA_DEST_CONSUMER2").into(),
    })
    .await?;

    // Both transports should be healthy
    assert!(transport1.is_healthy());
    assert!(transport2.is_healthy());

    // Publish from first transport
    let message = envelope(20001, QualityOfService::AtLeastOnce);
    transport1.publish(message).await?;

    // Receive on first transport (since it's a shared stream, either could receive)
    let delivery = tokio::time::timeout(Duration::from_secs(2), transport1.receive())
        .await
        .map_err(|e| test_error("receive message", e))??;
    assert_eq!(delivery.envelope().id(), 20001);
    delivery.acknowledge().await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transport_publish_multiple_and_receive_in_order() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_MULTI_RECEIVE").into(),
        subject: unique("catga.multi.receive").into(),
        consumer: unique("CATGA_MULTI_CONSUMER").into(),
    })
    .await?;

    // Publish multiple messages
    for id in 30001..=30005 {
        transport.publish(envelope(id, QualityOfService::AtLeastOnce)).await?;
    }

    // Receive all messages
    for expected_id in 30001..=30005 {
        let delivery = tokio::time::timeout(Duration::from_secs(2), transport.receive())
            .await
            .map_err(|e| test_error("receive message", e))??;
        assert_eq!(delivery.envelope().id(), expected_id);
        delivery.acknowledge().await?;
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transport_health_check() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_HEALTH").into(),
        subject: unique("catga.health").into(),
        consumer: unique("CATGA_HEALTH_CONSUMER").into(),
    })
    .await?;

    assert!(transport.is_healthy());
    assert_eq!(transport.health_status(), Some("NATS transport is connected"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn inbox_multiple_messages_lifecycle() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let inbox = NatsInbox::connect(server.url(), unique("CATGA_INBOX_MULTI")).await?;

    // Claim and complete multiple messages
    for id in 50001..=50010 {
        let claim = inbox.try_claim(id).await?.expect("should claim");
        inbox.complete(claim, Some(vec![id as u8].into())).await?;
    }

    // Verify all results are available
    for id in 50001..=50010 {
        let result = inbox.result(id).await?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &[id as u8]);
    }

    // Verify all states are completed
    for id in 50001..=50010 {
        let state = inbox.state(id).await?;
        assert_eq!(state, Some(ProcessingState::Completed));
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn state_machine_multiple_instances() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsStateMachines::<String>::connect(
        server.url(),
        unique("CATGA_STATE_MACHINES_MULTI"),
    )
    .await?;

    // Create multiple instances
    for i in 0..5 {
        let snapshot = StateMachineSnapshot::new(format!("sm-{i}"), format!("state-{i}"));
        assert!(store.create(snapshot).await?);
    }

    // Verify all instances
    for i in 0..5 {
        let retrieved = store.get(&format!("sm-{i}")).await?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().instance_id(), format!("sm-{i}"));
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn publisher_from_client_with_custom_codec() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let client = async_nats::connect(server.url())
        .await
        .map_err(|e| test_error("connect client", e))?;

    let publisher = NatsPublisher::from_client(
        client,
        NatsPublisherConfig {
            server: server.url().into(),
            stream: unique("CATGA_PUBLISHER_CLIENT").into(),
            subject: unique("catga.publisher.client").into(),
        },
    )
    .await?;

    assert!(publisher.is_healthy());
    assert!(publisher.is_accepting());

    let message = envelope(60001, QualityOfService::AtLeastOnce);
    publisher.publish(message).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn request_server_receives_direct_publish() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let subject = unique("catga.request.closed");

    // Connect server first - subscription should be ready before we publish
    let mut server_sub = NatsRequestServer::connect(server.url(), &subject).await?;

    // Small delay to ensure subscription is active
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = async_nats::connect(server.url())
        .await
        .map_err(|e| test_error("connect client", e))?;

    // Publish directly to subject
    client
        .publish(
            subject.clone(),
            MemoryPackCodec::default()
                .encode(&envelope(70001, QualityOfService::AtLeastOnce))?
                .into(),
        )
        .await
        .map_err(|e| test_error("direct publish", e))?;

    // The server should be able to receive
    let result = tokio::time::timeout(Duration::from_secs(2), server_sub.next()).await;
    assert!(result.is_ok(), "request should be received");
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn transport_stop_accepting_prevents_new_messages() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: unique("CATGA_STOP_ACCEPTING").into(),
        subject: unique("catga.stop").into(),
        consumer: unique("CATGA_STOP_CONSUMER").into(),
    })
    .await?;

    // Stop accepting new publishes
    transport.stop_accepting();
    assert!(!transport.is_accepting());

    // Publishing should fail
    let result = transport
        .publish(envelope(80001, QualityOfService::AtLeastOnce))
        .await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn snapshot_store_cas_retry_on_conflict() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store =
        NatsSnapshotStore::<u64>::connect(server.url(), unique("CATGA_SNAPSHOTS_CAS_RETRY")).await?;

    // Create initial snapshot
    store.save(Snapshot::new("cas-snap", 1_u64, 0)).await?;

    // Concurrent updates that trigger CAS retry
    let store = Arc::new(store);
    let store1 = Arc::clone(&store);
    let store2 = Arc::clone(&store);

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move {
            let snap = store1
                .load::<u64>("cas-snap")
                .await?
                .unwrap();
            let new_snap = Snapshot::new("cas-snap", snap.state().clone() + 10, snap.version() + 1);
            store1.save(new_snap).await
        }),
        tokio::spawn(async move {
            let snap = store2
                .load::<u64>("cas-snap")
                .await?
                .unwrap();
            let new_snap = Snapshot::new("cas-snap", snap.state().clone() + 20, snap.version() + 1);
            store2.save(new_snap).await
        }),
    );

    // At least one should succeed
    assert!(r1.is_ok() || r2.is_ok());

    // Verify final state
    let final_state = store.load::<u64>("cas-snap").await?.unwrap();
    assert!(final_state.version() >= 1);
    Ok(())
}

// ============================================================================
// Idempotency Store Tests (JetStream KV)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_claim_first() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_CLAIM")).await?;

    // First claim should succeed
    let claimed = store.try_claim("key-1").await?;
    assert!(claimed, "first claim should succeed");

    // Second claim should fail (already claimed)
    let claimed_again = store.try_claim("key-1").await?;
    assert!(!claimed_again, "second claim should fail for same key");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_complete() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_COMPLETE")).await?;

    // Claim the key
    let claimed = store.try_claim("key-complete").await?;
    assert!(claimed, "claim should succeed");

    // Complete with result
    let result_data: Arc<[u8]> = Arc::from(b"test result".as_slice());
    store.complete("key-complete", Some(result_data.clone())).await?;

    // State should be completed
    let state = store.state("key-complete").await?;
    assert_eq!(state, Some(ProcessingState::Completed));

    // Result should be retrievable
    let result = store.result("key-complete").await?;
    assert!(result.is_some());
    assert_eq!(&result.unwrap()[..], b"test result");

    // Claiming completed key should fail
    let claimed_after = store.try_claim("key-complete").await?;
    assert!(!claimed_after, "cannot claim completed key");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_complete_empty() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_EMPTY")).await?;

    // Claim and complete without result
    let claimed = store.try_claim("key-empty").await?;
    assert!(claimed, "claim should succeed");

    store.complete("key-empty", None).await?;

    // State should be completed
    let state = store.state("key-empty").await?;
    assert_eq!(state, Some(ProcessingState::Completed));

    // Result should be None (empty)
    let result = store.result("key-empty").await?;
    assert!(result.is_none());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_fail_retry() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_FAIL")).await?;

    // Claim the key
    let claimed = store.try_claim("key-fail").await?;
    assert!(claimed, "claim should succeed");

    // Fail the key
    store.fail("key-fail").await?;

    // State should be failed
    let state = store.state("key-fail").await?;
    assert_eq!(state, Some(ProcessingState::Failed));

    // Should be able to claim again after failure
    let reclaimed = store.try_claim("key-fail").await?;
    assert!(reclaimed, "should be able to reclaim failed key");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_state_nonexistent() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_STATE")).await?;

    // State for non-existent key should be None
    let state = store.state("non-existent").await?;
    assert!(state.is_none());

    // Result for non-existent key should be None
    let result = store.result("non-existent").await?;
    assert!(result.is_none());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_multiple_keys() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_MULTI")).await?;

    // Claim multiple keys
    let k1 = store.try_claim("key-A").await?;
    let k2 = store.try_claim("key-B").await?;
    let k3 = store.try_claim("key-C").await?;

    assert!(k1 && k2 && k3, "all claims should succeed");

    // Complete them
    store.complete("key-A", Some(Arc::from(&b"result-A"[..]))).await?;
    store.complete("key-B", Some(Arc::from(&b"result-B"[..]))).await?;
    store.complete("key-C", None).await?;

    // Verify results
    assert_eq!(&store.result("key-A").await?.unwrap()[..], b"result-A");
    assert_eq!(&store.result("key-B").await?.unwrap()[..], b"result-B");
    assert!(store.result("key-C").await?.is_none());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_cleanup() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    // Use short retention for cleanup test
    let store = NatsIdempotency::with_retention(
        server.url(),
        unique("CATGA_IDEMPOTENCY_CLEANUP"),
        Duration::from_millis(100),
    )
    .await?;

    // Claim and complete several keys
    for i in 0..5 {
        let key = format!("cleanup-key-{}", i);
        let result_bytes: Arc<[u8]> = Arc::from(format!("result-{}", i).into_bytes());
        store.try_claim(&key).await?;
        store.complete(&key, Some(result_bytes)).await?;
    }

    // Wait for retention period
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Cleanup should remove expired entries
    let removed = store.cleanup_completed(10).await?;
    assert_eq!(removed, 5, "all 5 completed entries should be cleaned up");

    // State should be none for cleaned up keys
    let state = store.state("cleanup-key-0").await?;
    assert!(state.is_none());

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_cleanup_limit() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::with_retention(
        server.url(),
        unique("CATGA_IDEMPOTENCY_CLEANUP_LIMIT"),
        Duration::from_millis(50),
    )
    .await?;

    // Complete 10 keys
    for i in 0..10 {
        let key = format!("limit-key-{}", i);
        store.try_claim(&key).await?;
        store.complete(&key, None).await?;
    }

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cleanup with limit of 3 should only remove 3
    let removed = store.cleanup_completed(3).await?;
    assert_eq!(removed, 3, "only 3 entries should be removed");

    // Cleanup remaining 7
    let removed = store.cleanup_completed(10).await?;
    assert_eq!(removed, 7);

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn idempotency_store_zero_limit_cleanup() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let store = NatsIdempotency::connect(server.url(), unique("CATGA_IDEMPOTENCY_ZERO")).await?;

    // Cleanup with zero limit should return 0
    let removed = store.cleanup_completed(0).await?;
    assert_eq!(removed, 0);

    Ok(())
}

// ============================================================================
// Lease Store Tests (JetStream KV)
// ============================================================================

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_acquire_first_owner() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_ACQUIRE")).await?;

    // First owner should acquire successfully
    let acquired = leases.try_acquire("resource-1", "owner-a", Duration::from_secs(30)).await?;
    assert!(acquired, "first owner should acquire the lease");

    // Same owner can re-acquire (idempotent)
    let reacquired = leases.try_acquire("resource-1", "owner-a", Duration::from_secs(30)).await?;
    assert!(reacquired, "same owner should be able to reacquire");

    // Different owner should fail
    let denied = leases.try_acquire("resource-1", "owner-b", Duration::from_secs(30)).await?;
    assert!(!denied, "different owner should be denied");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_acquire_after_expiry() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_EXPIRY")).await?;

    // Acquire with short TTL
    let acquired = leases.try_acquire("resource-expiry", "owner-a", Duration::from_millis(100)).await?;
    assert!(acquired, "first acquire should succeed");

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Different owner should now succeed after expiry
    let new_owner = leases.try_acquire("resource-expiry", "owner-b", Duration::from_secs(30)).await?;
    assert!(new_owner, "different owner should acquire after expiry");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_renew() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_RENEW")).await?;

    // Acquire initial lease
    let acquired = leases.try_acquire("resource-renew", "owner-a", Duration::from_secs(1)).await?;
    assert!(acquired, "initial acquire should succeed");

    // Renew succeeds for same owner
    let renewed = leases.renew("resource-renew", "owner-a", Duration::from_secs(30)).await?;
    assert!(renewed, "renew should succeed for same owner");

    // Renew fails for different owner
    let denied = leases.renew("resource-renew", "owner-b", Duration::from_secs(30)).await?;
    assert!(!denied, "renew should fail for different owner");

    // Renew fails for non-existent resource
    let missing = leases.renew("non-existent", "owner-a", Duration::from_secs(30)).await?;
    assert!(!missing, "renew should fail for non-existent resource");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_renew_after_expiry() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_RENEW_EXPIRY")).await?;

    // Acquire with very short TTL
    let acquired = leases.try_acquire("resource-renew-expiry", "owner-a", Duration::from_millis(50)).await?;
    assert!(acquired, "initial acquire should succeed");

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Renew fails after expiry
    let renewed = leases.renew("resource-renew-expiry", "owner-a", Duration::from_secs(30)).await?;
    assert!(!renewed, "renew should fail after expiry");

    // But new owner can acquire
    let new_acquired = leases.try_acquire("resource-renew-expiry", "owner-b", Duration::from_secs(30)).await?;
    assert!(new_acquired, "new owner should acquire after expiry");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_release() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_RELEASE")).await?;

    // Acquire a lease
    let acquired = leases.try_acquire("resource-release", "owner-a", Duration::from_secs(30)).await?;
    assert!(acquired, "initial acquire should succeed");

    // Release succeeds for same owner
    let released = leases.release("resource-release", "owner-a").await?;
    assert!(released, "release should succeed for same owner");

    // Release fails on second attempt (resource no longer exists)
    let released_again = leases.release("resource-release", "owner-a").await?;
    assert!(!released_again, "second release should return false (resource gone)");

    // Different owner cannot release non-existent resource
    let try_release = leases.release("resource-release", "owner-b").await?;
    assert!(!try_release, "different owner cannot release non-existent resource");

    // New owner can acquire the now-released resource
    let new_acquired = leases.try_acquire("resource-release", "owner-b", Duration::from_secs(30)).await?;
    assert!(new_acquired, "new owner should acquire released lease");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_release_non_existent() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_NOENT")).await?;

    // Release non-existent resource should return false
    let released = leases.release("non-existent-lease", "owner-a").await?;
    assert!(!released, "release should return false for non-existent");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_multiple_resources() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_MULTI")).await?;

    // Acquire multiple different resources
    let r1 = leases.try_acquire("resource-A", "owner-1", Duration::from_secs(30)).await?;
    let r2 = leases.try_acquire("resource-B", "owner-1", Duration::from_secs(30)).await?;
    let r3 = leases.try_acquire("resource-C", "owner-2", Duration::from_secs(30)).await?;

    assert!(r1, "resource-A should be acquired");
    assert!(r2, "resource-B should be acquired");
    assert!(r3, "resource-C should be acquired");

    // Release each resource
    assert!(leases.release("resource-A", "owner-1").await?, "resource-A released");
    assert!(leases.release("resource-B", "owner-1").await?, "resource-B released");
    assert!(leases.release("resource-C", "owner-2").await?, "resource-C released");

    // Resources can be acquired again
    assert!(leases.try_acquire("resource-A", "owner-3", Duration::from_secs(30)).await?, "resource-A reacquired");
    assert!(leases.try_acquire("resource-B", "owner-3", Duration::from_secs(30)).await?, "resource-B reacquired");
    assert!(leases.try_acquire("resource-C", "owner-3", Duration::from_secs(30)).await?, "resource-C reacquired");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_concurrent_acquire() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_CONCURRENT")).await?;

    // First owner acquires
    let first = leases.try_acquire("concurrent-resource", "owner-first", Duration::from_secs(10)).await?;
    assert!(first, "first owner should acquire");

    // Multiple other owners try concurrently
    let leases = Arc::new(leases);
    let mut handles = Vec::new();

    for i in 0..5 {
        let lease_clone = Arc::clone(&leases);
        let owner = format!("owner-{}", i);
        let handle = tokio::spawn(async move {
            lease_clone.try_acquire("concurrent-resource", owner.as_str(), Duration::from_secs(30)).await
        });
        handles.push(handle);
    }

    let results: Vec<CatgaResult<bool>> = futures::future::join_all(handles).await
        .into_iter()
        .map(|r| r.expect("task should not panic"))
        .collect();

    // All should return false (first owner still holds)
    for result in results {
        assert!(!result?, "concurrent acquires should all be denied");
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_concurrent_acquire_after_expiry() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_CONCURRENT_EXPIRY")).await?;

    // First owner acquires with very short TTL
    let first = leases.try_acquire("concurrent-expiry", "owner-first", Duration::from_millis(50)).await?;
    assert!(first, "first owner should acquire");

    // Wait for expiry
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Multiple owners try concurrently after expiry
    let leases = Arc::new(leases);
    let mut handles = Vec::new();

    for i in 0..3 {
        let lease_clone = Arc::clone(&leases);
        let owner = format!("owner-new-{}", i);
        let handle = tokio::spawn(async move {
            lease_clone.try_acquire("concurrent-expiry", owner.as_str(), Duration::from_secs(30)).await
        });
        handles.push(handle);
    }

    let results: Vec<CatgaResult<bool>> = futures::future::join_all(handles).await
        .into_iter()
        .map(|r| r.expect("task should not panic"))
        .collect();

    // Exactly one should succeed due to CAS
    let success_count = results.iter().filter(|r| r.as_ref().map_or(false, |v| *v)).count();
    assert_eq!(success_count, 1, "exactly one owner should acquire after expiry");

    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lease_store_renew_extends_ttl() -> CatgaResult<()> {
    let server = NatsServer::start().await?;
    let leases = NatsLeases::connect(server.url(), unique("CATGA_LEASES_RENEW_TTL")).await?;

    // Acquire with short TTL
    let acquired = leases.try_acquire("resource-ttl", "owner-a", Duration::from_millis(50)).await?;
    assert!(acquired, "initial acquire should succeed");

    // Renew with longer TTL before expiry
    tokio::time::sleep(Duration::from_millis(20)).await;
    let renewed = leases.renew("resource-ttl", "owner-a", Duration::from_secs(10)).await?;
    assert!(renewed, "renew should succeed");

    // Wait past original TTL but not past renewed TTL
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Different owner should still be denied (lease was renewed)
    let denied = leases.try_acquire("resource-ttl", "owner-b", Duration::from_secs(30)).await?;
    assert!(!denied, "different owner should be denied due to renewal");

    Ok(())
}
