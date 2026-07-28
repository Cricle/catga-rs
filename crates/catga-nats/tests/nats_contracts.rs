#![allow(missing_docs)]

//! Public NATS adapter contracts exercised against an isolated JetStream server.

use std::{
    net::TcpListener,
    process::{Child, Command},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope,
    EnvelopeCodec, ErrorCode, EventStore, MessageMetadata, MessageTransport, OutboxMessage,
    OutboxState, OutboxStore, QualityOfService,
};
use catga_flow::{
    DueFlowScheduler, FlowContinuation, FlowQuery, FlowScheduler, FlowState, FlowStatus, FlowStore,
    SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_nats::{
    NatsConfig, NatsDeadLetters, NatsEventStore, NatsFlowScheduler, NatsFlows, NatsOutbox,
    NatsPubSubConfig, NatsPubSubTransport, NatsRequestClient, NatsSuspendedFlows, NatsTransport,
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
