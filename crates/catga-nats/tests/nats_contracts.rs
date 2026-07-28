#![allow(missing_docs)]

//! Public NATS adapter contracts exercised against an isolated JetStream server.

use std::{
    net::TcpListener,
    process::{Child, Command},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_nats::jetstream::{self, kv};
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope,
    EnvelopeCodec, ErrorCode, EventStore, MessageMetadata, MessageTransport, OutboxMessage,
    OutboxState, OutboxStore, QualityOfService,
};
use catga_nats::{
    NatsConfig, NatsDeadLetters, NatsEventStore, NatsOutbox, NatsPubSubConfig, NatsPubSubTransport,
    NatsRequestClient, NatsTransport,
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
