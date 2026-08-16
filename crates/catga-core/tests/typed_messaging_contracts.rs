//! Contract coverage for routing tables, schema versioning, and the composable
//! transport wrappers.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::memory::{MemoryEventStore, MemoryTransport};
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, Destination, DestinationTransport, Envelope,
    EnvelopeHeaders, ErrorCode, EventStore, EventUpgrader, EventVersionRegistry,
    MessageDestinationRouter, MessageMetadata, MessageRouter, MessageTransport,
    ResilienceExecutor, ResilienceOptions, ResilientTransport, TransportBatcher,
    UpgradingEventStore, assert_error_code, assert_failure, assert_success,
};
use tokio_util::sync::CancellationToken;

fn env(id: u64, message_type: &str, schema_version: u32) -> Envelope {
    Envelope::versioned(
        id,
        message_type,
        vec![1, 2, 3],
        MessageMetadata::new(id, None),
        schema_version,
    )
}

// ---------------------------------------------------------------------------
// MessageDestinationRouter
// ---------------------------------------------------------------------------

#[test]
fn destination_router_validates_and_resolves_routes() {
    let mut router = MessageDestinationRouter::new();
    assert!(router.is_empty());
    assert_eq!(router.len(), 0);

    let dest = assert_success(Destination::parse("orders-stream"));
    assert_success(router.add_route("orders::OrderCreated", dest.clone()));

    // Empty and duplicate routes are rejected deterministically.
    assert_error_code(router.add_route("   ", dest.clone()), ErrorCode::Validation);
    assert_error_code(
        router.add_route("orders::OrderCreated", dest.clone()),
        ErrorCode::Validation,
    );

    assert_eq!(router.len(), 1);
    assert!(!router.is_empty());
    assert_eq!(router.resolve("orders::OrderCreated"), Some(&dest));
    assert_eq!(router.resolve("unknown::Type"), None);
    let cloned = router.clone();
    assert_eq!(cloned.resolve("orders::OrderCreated"), Some(&dest));
}

#[test]
fn destination_names_validate_and_round_trip() {
    let error = assert_failure(Destination::parse(""));
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(!error.message().is_empty());

    let dest = assert_success(Destination::parse("queue-a"));
    assert_eq!(dest.as_str(), "queue-a");
    assert_eq!(dest.to_string(), "queue-a");
    let owned: Box<str> = dest.clone().into_boxed_str();
    assert_eq!(&*owned, "queue-a");
    assert_eq!(dest, assert_success(Destination::parse("queue-a")));
    assert!(dest < assert_success(Destination::parse("queue-b")));
}

// ---------------------------------------------------------------------------
// MessageRouter (header routes)
// ---------------------------------------------------------------------------

#[test]
fn header_router_matches_first_rule_and_falls_back() {
    let tenant = assert_success(Destination::parse("tenant-queue"));
    let region = assert_success(Destination::parse("region-queue"));
    let fallback = assert_success(Destination::parse("fallback-queue"));

    let mut router = MessageRouter::new(Some(fallback.clone()));
    assert!(router.is_empty());
    assert_success(router.add_route("x-tenant", "acme", tenant.clone()));
    assert_success(router.add_route("x-region", "eu", region.clone()));
    // A duplicate rule stays valid; the first match wins.
    assert_success(router.add_route("x-tenant", "acme", region.clone()));
    assert_error_code(
        router.add_route("", "acme", tenant.clone()),
        ErrorCode::Validation,
    );
    assert_error_code(
        router.add_route("x-tenant", " ", tenant.clone()),
        ErrorCode::Validation,
    );
    assert_eq!(router.len(), 3);
    assert!(!router.is_empty());

    let matched = router.resolve(&[("other", "1"), ("x-tenant", "acme")]);
    assert_eq!(matched, Some(&tenant));
    assert_eq!(router.resolve(&[("x-region", "eu")]), Some(&region));
    assert_eq!(router.resolve(&[]), Some(&fallback));

    let headers = assert_success(EnvelopeHeaders::try_new([("x-region", "eu")]));
    assert_eq!(router.resolve_envelope_headers(&headers), Some(&region));
    let empty = assert_success(EnvelopeHeaders::try_new([("unrelated", "0")]));
    assert_eq!(router.resolve_envelope_headers(&empty), Some(&fallback));

    let cloned = router.clone();
    assert_eq!(cloned.len(), 3);
    assert!(format!("{cloned:?}").contains("MessageRouter"));

    // A router without a fallback resolves unmatched headers to nothing.
    let bare = MessageRouter::new(None);
    assert_eq!(bare.resolve(&[("x-tenant", "acme")]), None);
    assert_eq!(bare.resolve_envelope_headers(&headers), None);
}

// ---------------------------------------------------------------------------
// EventVersionRegistry
// ---------------------------------------------------------------------------

struct StepUpgrader {
    source: Box<str>,
    target: Box<str>,
    from: u32,
    to: u32,
    mode: UpgradeMode,
}

#[derive(Clone, Copy)]
enum UpgradeMode {
    Honest,
    MismatchedOutput,
    Failing,
}

impl StepUpgrader {
    fn new(source: &str, target: &str, from: u32, to: u32) -> Arc<Self> {
        Arc::new(Self {
            source: source.into(),
            target: target.into(),
            from,
            to,
            mode: UpgradeMode::Honest,
        })
    }
}

impl EventUpgrader for StepUpgrader {
    fn source_type(&self) -> &str {
        &self.source
    }

    fn target_type(&self) -> &str {
        &self.target
    }

    fn source_version(&self) -> u32 {
        self.from
    }

    fn target_version(&self) -> u32 {
        self.to
    }

    fn upgrade(&self, source: Envelope) -> CatgaResult<Envelope> {
        match self.mode {
            UpgradeMode::Honest => Ok(Envelope::versioned(
                source.id(),
                self.target.clone(),
                source.payload().to_vec(),
                source.metadata(),
                self.to,
            )),
            UpgradeMode::MismatchedOutput => Ok(Envelope::versioned(
                source.id(),
                self.target.clone(),
                source.payload().to_vec(),
                source.metadata(),
                self.to + 5,
            )),
            UpgradeMode::Failing => Err(CatgaError::new(
                ErrorCode::Internal,
                "upgrader refuses to run",
            )),
        }
    }
}

#[test]
fn event_version_registry_validates_registrations() {
    let registry = EventVersionRegistry::default();
    assert_eq!(registry.current_version("orders::OrderCreated"), 1);
    assert!(!registry.has_upgraders("orders::OrderCreated"));

    // Empty types and non-advancing versions are rejected.
    let empty_source = Arc::new(StepUpgrader {
        source: "".into(),
        target: "b".into(),
        from: 1,
        to: 2,
        mode: UpgradeMode::Honest,
    });
    assert_error_code(registry.register(empty_source), ErrorCode::Validation);
    let empty_target = Arc::new(StepUpgrader {
        source: "a".into(),
        target: "".into(),
        from: 1,
        to: 2,
        mode: UpgradeMode::Honest,
    });
    assert_error_code(registry.register(empty_target), ErrorCode::Validation);
    let backwards = StepUpgrader::new("a", "b", 3, 3);
    assert_error_code(registry.register(backwards), ErrorCode::Validation);

    let first = StepUpgrader::new("a", "b", 1, 2);
    assert_success(registry.register(first));
    assert!(registry.has_upgraders("a"));
    assert!(!registry.has_upgraders("b"));
    assert_eq!(registry.current_version("b"), 2);

    // A second upgrader for the same source type and version conflicts.
    let duplicate = StepUpgrader::new("a", "c", 1, 4);
    assert_error_code(registry.register(duplicate), ErrorCode::Conflict);

    // Higher targets raise the tracked current version.
    let later = StepUpgrader::new("a", "b", 2, 5);
    assert_success(registry.register(later));
    assert_eq!(registry.current_version("b"), 5);
}

#[test]
fn event_version_registry_upgrades_chains_to_latest() {
    let registry = EventVersionRegistry::default();
    assert_success(registry.register(StepUpgrader::new("orders::v1", "orders::v2", 1, 2)));
    assert_success(registry.register(StepUpgrader::new("orders::v2", "orders::v3", 2, 3)));

    let upgraded = assert_success(registry.upgrade_to_latest(env(7, "orders::v1", 1)));
    assert_eq!(upgraded.message_type(), "orders::v3");
    assert_eq!(upgraded.schema_version(), 3);
    assert_eq!(upgraded.payload(), &[1, 2, 3]);
    assert_eq!(upgraded.id(), 7);

    // Unknown types and unmatched versions pass through unchanged.
    let untouched = assert_success(registry.upgrade_to_latest(env(8, "other::Type", 1)));
    assert_eq!(untouched.message_type(), "other::Type");
    let stalled = assert_success(registry.upgrade_to_latest(env(9, "orders::v1", 99)));
    assert_eq!(stalled.schema_version(), 99);
}

#[test]
fn event_version_registry_rejects_bad_upgrader_output_and_cycles() {
    let registry = EventVersionRegistry::default();
    let mismatched = Arc::new(StepUpgrader {
        source: "mismatched".into(),
        target: "mismatched".into(),
        from: 1,
        to: 2,
        mode: UpgradeMode::MismatchedOutput,
    });
    assert_success(registry.register(mismatched));
    assert_error_code(
        registry.upgrade_to_latest(env(1, "mismatched", 1)),
        ErrorCode::Validation,
    );

    let failing = Arc::new(StepUpgrader {
        source: "failing".into(),
        target: "failing".into(),
        from: 1,
        to: 2,
        mode: UpgradeMode::Failing,
    });
    assert_success(registry.register(failing));
    assert_error_code(
        registry.upgrade_to_latest(env(1, "failing", 1)),
        ErrorCode::Internal,
    );

    // A ping-pong chain longer than the upgrade budget is rejected.
    let chained = EventVersionRegistry::default();
    for version in 1..=101_u32 {
        let source = if version % 2 == 1 { "ping" } else { "pong" };
        let target = if version % 2 == 1 { "pong" } else { "ping" };
        assert_success(chained.register(StepUpgrader::new(source, target, version, version + 1)));
    }
    assert_error_code(
        chained.upgrade_to_latest(env(1, "ping", 1)),
        ErrorCode::Validation,
    );
}

// ---------------------------------------------------------------------------
// UpgradingEventStore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upgrading_event_store_upgrades_read_pages() {
    let store = MemoryEventStore::default();
    assert_success(
        store
            .append("orders-1", vec![env(10, "orders::Created", 1)], None)
            .await,
    );
    assert_success(
        store
            .append("plain-1", vec![env(11, "plain::Type", 1)], None)
            .await,
    );

    let versions = EventVersionRegistry::default();
    assert_success(versions.register(StepUpgrader::new(
        "orders::Created",
        "orders::CreatedV2",
        1,
        2,
    )));
    let view = UpgradingEventStore::new(&store, &versions);

    // Append passes through unchanged.
    assert_success(
        view.append("orders-2", vec![env(12, "orders::Created", 1)], None)
            .await,
    );
    assert_eq!(assert_success(view.version("orders-2").await), 0);

    let page = assert_success(view.read_page("orders-1", 0, 10).await);
    let events = page.stream().events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].envelope().message_type(), "orders::CreatedV2");
    assert_eq!(events[0].envelope().schema_version(), 2);

    // Streams without registered upgraders pass through untouched.
    let plain = assert_success(view.read_page("plain-1", 0, 10).await);
    assert_eq!(
        plain.stream().events()[0].envelope().message_type(),
        "plain::Type"
    );

    let to_version = assert_success(view.read_to_version_page("orders-1", 0, 5, 10).await);
    assert_eq!(
        to_version.stream().events()[0].envelope().message_type(),
        "orders::CreatedV2"
    );

    let to_time = assert_success(
        view.read_to_time_page(
            "orders-1",
            0,
            SystemTime::now() + Duration::from_secs(60),
            10,
        )
        .await,
    );
    assert_eq!(
        to_time.stream().events()[0].envelope().message_type(),
        "orders::CreatedV2"
    );

    let history = assert_success(view.version_history_page("orders-1", 0, 10).await);
    assert_eq!(history.entries().len(), 1);
    let ids = assert_success(view.stream_ids_page(None, 10).await);
    assert!(ids.ids().iter().any(|id| id == "orders-1"));

    // A failing upgrader surfaces its error on read.
    let failing = EventVersionRegistry::default();
    let broken = Arc::new(StepUpgrader {
        source: "orders::Created".into(),
        target: "orders::CreatedV2".into(),
        from: 1,
        to: 2,
        mode: UpgradeMode::Failing,
    });
    assert_success(failing.register(broken));
    let broken_view = UpgradingEventStore::new(&store, &failing);
    assert_error_code(
        broken_view.read_page("orders-1", 0, 10).await,
        ErrorCode::Internal,
    );
}

// ---------------------------------------------------------------------------
// ResilientTransport
// ---------------------------------------------------------------------------

struct FlakyTransport {
    publish_failures_left: AtomicU32,
    receive_failures_left: AtomicU32,
    send_failures_left: AtomicU32,
    publishes: AtomicU32,
}

impl FlakyTransport {
    fn new(publish_flakes: u32, receive_flakes: u32, send_flakes: u32) -> Arc<Self> {
        Arc::new(Self {
            publish_failures_left: AtomicU32::new(publish_flakes),
            receive_failures_left: AtomicU32::new(receive_flakes),
            send_failures_left: AtomicU32::new(send_flakes),
            publishes: AtomicU32::new(0),
        })
    }

    fn flake(counter: &AtomicU32) -> CatgaResult<()> {
        let mut current = counter.load(Ordering::SeqCst);
        loop {
            if current == 0 {
                return Ok(());
            }
            match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    return Err(CatgaError::new(
                        ErrorCode::Unavailable,
                        "transient backend failure",
                    ));
                }
                Err(observed) => current = observed,
            }
        }
    }
}

#[async_trait]
impl MessageTransport for FlakyTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        Self::flake(&self.publish_failures_left)?;
        self.publishes.fetch_add(1, Ordering::SeqCst);
        let _ = envelope;
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Self::flake(&self.receive_failures_left)?;
        Ok(Delivery::new(env(1, "flaky::Type", 1)))
    }
}

#[async_trait]
impl DestinationTransport for FlakyTransport {
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        Self::flake(&self.send_failures_left)?;
        let _ = (destination, envelope);
        Ok(())
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        Self::flake(&self.receive_failures_left)?;
        let _ = destination;
        Ok(Delivery::new(env(2, "flaky::Destination", 1)))
    }
}

fn retrying_executor() -> Arc<ResilienceExecutor> {
    let options = ResilienceOptions {
        max_retries: 3,
        retry_delay: Duration::from_millis(1),
        ..ResilienceOptions::default()
    };
    Arc::new(assert_success(ResilienceExecutor::new(options)))
}

#[tokio::test]
async fn resilient_transport_retries_transient_failures() {
    let inner = FlakyTransport::new(2, 1, 1);
    let resilient =
        ResilientTransport::new(inner.clone(), retrying_executor(), retrying_executor());
    assert!(Arc::ptr_eq(resilient.inner(), &inner));

    // Two transient publish failures are retried until success.
    assert_success(MessageTransport::publish(&resilient, env(1, "flaky::Type", 1)).await);
    assert_eq!(inner.publishes.load(Ordering::SeqCst), 1);

    let delivery = assert_success(resilient.receive().await);
    assert_eq!(delivery.envelope().message_type(), "flaky::Type");
    assert_success(resilient.ack(delivery).await);

    let destination = assert_success(Destination::parse("flaky-queue"));
    assert_success(
        resilient
            .send_to(&destination, env(2, "flaky::Type", 1))
            .await,
    );
    let from_destination = assert_success(resilient.receive_from(&destination).await);
    assert_eq!(
        from_destination.envelope().message_type(),
        "flaky::Destination"
    );
}

#[tokio::test]
async fn resilient_transport_propagates_exhausted_retries() {
    let inner = FlakyTransport::new(20, 20, 20);
    let resilient = ResilientTransport::new(inner, retrying_executor(), retrying_executor());
    assert_error_code(
        MessageTransport::publish(&resilient, env(1, "flaky::Type", 1)).await,
        ErrorCode::Unavailable,
    );
    assert_error_code(resilient.receive().await, ErrorCode::Unavailable);
    let destination = assert_success(Destination::parse("flaky-queue"));
    assert_error_code(
        resilient
            .send_to(&destination, env(1, "flaky::Type", 1))
            .await,
        ErrorCode::Unavailable,
    );
    assert_error_code(
        resilient.receive_from(&destination).await,
        ErrorCode::Unavailable,
    );
}

// ---------------------------------------------------------------------------
// Delivery and acknowledgement contracts
// ---------------------------------------------------------------------------

struct RecordingAcknowledger {
    acked: Arc<AtomicU32>,
    fail_ack: bool,
    support_nack: bool,
}

#[async_trait]
impl Acknowledger for RecordingAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        if self.fail_ack {
            return Err(CatgaError::new(ErrorCode::Internal, "ack failed"));
        }
        self.acked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        if self.support_nack {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Unsupported,
                "nack not supported by this recorder",
            ))
        }
    }
}

struct DefaultNackAcknowledger;

#[async_trait]
impl Acknowledger for DefaultNackAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn delivery_acknowledgement_and_mapping_contracts() {
    // A delivery without an acknowledger acks and nacks as a no-op.
    let plain = Delivery::new(env(1, "plain::Type", 1));
    assert_eq!(plain.attempts(), 1);
    assert!(format!("{plain:?}").contains("requires_ack"));
    let plain = Delivery::new(env(1, "plain::Type", 1));
    assert_success(plain.acknowledge().await);
    assert_success(Delivery::new(env(1, "plain::Type", 1)).nack().await);

    // Zero attempts normalize to one; larger values pass through.
    assert_eq!(Delivery::new(env(1, "t", 1)).with_attempts(0).attempts(), 1);
    assert_eq!(Delivery::new(env(1, "t", 1)).with_attempts(4).attempts(), 4);

    // map_envelope keeps the acknowledgement token and attempt count.
    let acked = Arc::new(AtomicU32::new(0));
    let recorded = Delivery::with_acknowledger(
        env(1, "old::Type", 1),
        Box::new(RecordingAcknowledger {
            acked: acked.clone(),
            fail_ack: false,
            support_nack: true,
        }),
    )
    .with_attempts(3);
    let mapped = assert_success(recorded.map_envelope(|envelope| {
        let id = envelope.id();
        Ok(envelope.with_metadata(MessageMetadata::new(id, Some(42))))
    }));
    assert_eq!(mapped.attempts(), 3);
    assert_eq!(mapped.envelope().metadata().correlation_id(), Some(42));
    assert_success(mapped.acknowledge().await);
    assert_eq!(acked.load(Ordering::SeqCst), 1);

    // A failing mapper propagates its error without consuming the delivery token.
    let rejected = Delivery::with_acknowledger(
        env(2, "old::Type", 1),
        Box::new(RecordingAcknowledger {
            acked: acked.clone(),
            fail_ack: false,
            support_nack: true,
        }),
    );
    assert_error_code(
        rejected.map_envelope(|_| Err(CatgaError::new(ErrorCode::Validation, "bad mapping"))),
        ErrorCode::Validation,
    );

    // Acknowledger failures surface through the delivery and transport helpers.
    let failing = Delivery::with_acknowledger(
        env(3, "t", 1),
        Box::new(RecordingAcknowledger {
            acked: acked.clone(),
            fail_ack: true,
            support_nack: true,
        }),
    );
    assert_error_code(failing.acknowledge().await, ErrorCode::Internal);

    let nack_supported = Delivery::with_acknowledger(
        env(4, "t", 1),
        Box::new(RecordingAcknowledger {
            acked: acked.clone(),
            fail_ack: false,
            support_nack: true,
        }),
    );
    assert_success(nack_supported.negative_acknowledge().await);

    // Backends without native nack report Unsupported.
    let default_nack =
        Delivery::with_acknowledger(env(5, "t", 1), Box::new(DefaultNackAcknowledger));
    assert_error_code(default_nack.nack().await, ErrorCode::Unsupported);
}

// ---------------------------------------------------------------------------
// Batch publication contracts
// ---------------------------------------------------------------------------

struct CountingTransport {
    publishes: AtomicU32,
    fail_ids_below: AtomicU64,
}

#[async_trait]
impl MessageTransport for CountingTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        if envelope.id() < self.fail_ids_below.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "rejected envelope"));
        }
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Ok(Delivery::new(env(1, "counting::Type", 1)))
    }
}

#[async_trait]
impl DestinationTransport for CountingTransport {
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        let _ = destination;
        MessageTransport::publish(self, envelope).await
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        let _ = destination;
        self.receive().await
    }
}

#[tokio::test]
async fn batch_publication_attempts_every_envelope_and_reports_first_error() {
    let transport = CountingTransport {
        publishes: AtomicU32::new(0),
        fail_ids_below: AtomicU64::new(3),
    };

    // Zero concurrency is rejected for both default and explicit batches.
    assert_error_code(
        transport
            .publish_batch_with_concurrency(vec![env(1, "t", 1)], 0)
            .await,
        ErrorCode::Validation,
    );
    let destination = assert_success(Destination::parse("bulk"));
    assert_error_code(
        transport
            .send_batch_to_with_concurrency(&destination, vec![env(1, "t", 1)], 0)
            .await,
        ErrorCode::Validation,
    );

    // Every envelope is attempted; the first observed error is returned.
    let envelopes = vec![
        env(5, "t", 1),
        env(1, "t", 1),
        env(6, "t", 1),
        env(2, "t", 1),
    ];
    assert_error_code(
        transport.publish_batch(envelopes).await,
        ErrorCode::Unavailable,
    );
    assert_eq!(transport.publishes.load(Ordering::SeqCst), 4);

    // A clean batch succeeds through the default concurrency path.
    let clean = vec![env(9, "t", 1), env(10, "t", 1)];
    assert_success(transport.publish_batch(clean).await);

    // Destination batches share the same semantics.
    let send = vec![env(1, "t", 1), env(9, "t", 1)];
    assert_error_code(
        transport.send_batch_to(&destination, send).await,
        ErrorCode::Unavailable,
    );
    let clean_send = vec![env(9, "t", 1)];
    assert_success(transport.send_batch_to(&destination, clean_send).await);

    // The default destination declaration is a no-op.
    assert_success(transport.declare_destination(&destination));
}

// ---------------------------------------------------------------------------
// TransportBatcher and runner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transport_batch_options_validate_bounds() {
    let options = catga_core::TransportBatchOptions::default();
    assert_eq!(options.max_batch_size, 100);
    assert_eq!(options.max_queue_length, 10_000);

    let mut zero_batch = options.clone();
    zero_batch.max_batch_size = 0;
    assert_error_code(
        TransportBatcher::new(
            Arc::new(assert_success(MemoryTransport::new(1))),
            zero_batch,
        )
        .map(|_| ()),
        ErrorCode::Validation,
    );

    let mut zero_timeout = options.clone();
    zero_timeout.batch_timeout = Duration::ZERO;
    assert_error_code(
        TransportBatcher::new(
            Arc::new(assert_success(MemoryTransport::new(1))),
            zero_timeout,
        )
        .map(|_| ()),
        ErrorCode::Validation,
    );

    let mut zero_queue = options.clone();
    zero_queue.max_queue_length = 0;
    assert_error_code(
        TransportBatcher::new(
            Arc::new(assert_success(MemoryTransport::new(1))),
            zero_queue,
        )
        .map(|_| ()),
        ErrorCode::Validation,
    );

    let mut zero_concurrency = options;
    zero_concurrency.publish_concurrency = 0;
    assert_error_code(
        TransportBatcher::new(
            Arc::new(assert_success(MemoryTransport::new(1))),
            zero_concurrency,
        )
        .map(|_| ()),
        ErrorCode::Validation,
    );
}

#[tokio::test]
async fn transport_batcher_flushes_by_size_and_timeout_and_rejects_on_shutdown() {
    let transport = Arc::new(assert_success(MemoryTransport::new(64)));
    let options = catga_core::TransportBatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_millis(25),
        max_queue_length: 8,
        publish_concurrency: 2,
    };
    let (batcher, runner) = assert_success(TransportBatcher::new(transport.clone(), options));

    let shutdown = CancellationToken::new();
    let runner_task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));

    // Two queued envelopes hit the batch size and publish together.
    let first_batcher = batcher.clone();
    let first = tokio::spawn(async move { first_batcher.publish(env(1, "batch::Type", 1)).await });
    let second_batcher = batcher.clone();
    let second =
        tokio::spawn(async move { second_batcher.publish(env(2, "batch::Type", 1)).await });
    assert_success(first.await.expect("first publish task joins"));
    assert_success(second.await.expect("second publish task joins"));

    // A lone envelope flushes by timeout instead of batch size.
    let lone = batcher.publish(env(3, "batch::Type", 1)).await;
    assert_success(lone);
    // The size-triggered batch publishes concurrently, so only its membership
    // is ordered before the timeout-triggered singleton.
    let mut batch_ids = Vec::new();
    for _ in 0..2 {
        let delivery = assert_success(transport.receive().await);
        batch_ids.push(delivery.envelope().id());
        assert_success(delivery.acknowledge().await);
    }
    batch_ids.sort_unstable();
    assert_eq!(batch_ids, vec![1, 2]);
    let last = assert_success(transport.receive().await);
    assert_eq!(last.envelope().id(), 3);
    assert_success(last.acknowledge().await);

    // Cancellation rejects queued but unstarted envelopes.
    let queued_batcher = batcher.clone();
    let queued =
        tokio::spawn(async move { queued_batcher.publish(env(4, "batch::Type", 1)).await });
    shutdown.cancel();
    let queued_result = queued.await.expect("queued publish task joins");
    let error = assert_failure(queued_result);
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_success(runner_task.await.expect("runner task joins"));

    // A stopped runner refuses new work.
    assert_error_code(
        batcher.publish(env(5, "batch::Type", 1)).await,
        ErrorCode::Unavailable,
    );
}
