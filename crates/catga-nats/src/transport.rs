use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{self, pull},
    stream,
};
use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Destination,
    DestinationTransport, Envelope, EnvelopeCodec, ErrorCode, HealthCheckable, MessageTransport,
    OperationTracker, QualityOfService, Stoppable, Waitable, telemetry,
};
use dashmap::{DashMap, mapref::entry::Entry};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{NatsConfig, NatsDestinationConfig, acknowledgement::NatsAcknowledger};

/// Counter incremented when JetStream confirms that it suppressed an ExactlyOnce duplicate.
const NATS_DEDUP_DROPS: &str = "catga.nats.dedup.drops";

/// NATS transport that maps durable Catga QoS to JetStream delivery semantics.
///
/// `AtLeastOnce` waits for a JetStream publish acknowledgement, and `ExactlyOnce` additionally
/// supplies the envelope message ID to JetStream's deduplication window. `AtMostOnce` is
/// rejected because Core NATS publication to a subject captured by this transport's JetStream
/// stream would be retained and redeliverable. Use [`crate::NatsPubSubTransport`] for genuinely
/// ephemeral publication. Received JetStream deliveries always expose explicit acknowledgement through
/// [`MessageTransport::ack`]. The default [`MemoryPackCodec`] preserves the original wire format;
/// applications with another envelope format can select it through a `*_with_codec` constructor.
pub struct NatsTransport<C = MemoryPackCodec>
where
    C: EnvelopeCodec,
{
    context: jetstream::Context,
    subject: Box<str>,
    codec: C,
    consumer: consumer::PullConsumer,
    destinations: DashMap<Destination, NatsDestination>,
    operations: OperationTracker,
    acceptance: AcceptanceGate,
}

/// Provisioned JetStream handles for one named Catga destination.
#[derive(Clone)]
struct NatsDestination {
    subject: Box<str>,
    consumer: consumer::PullConsumer,
}

/// Native NATS publication mechanism chosen from an envelope's delivery guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NatsPublishMode {
    /// Core NATS with no JetStream persistence acknowledgement.
    Core,
    /// JetStream with durable at-least-once delivery.
    JetStream,
    /// JetStream with a stable NATS message identity for broker-side deduplication.
    JetStreamDeduplicated,
}

impl NatsTransport<MemoryPackCodec> {
    /// Connects and idempotently provisions the configured stream and durable consumer.
    pub async fn connect(config: NatsConfig) -> CatgaResult<Self> {
        Self::connect_with_codec(config, MemoryPackCodec::default()).await
    }

    /// Builds a transport from an application-owned NATS client.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior while idempotently provisioning the stream and durable consumer in
    /// `config`. `config.server` is not opened by this constructor; it remains part of
    /// [`NatsConfig`] for compatibility with [`Self::connect`].
    pub async fn from_client(client: async_nats::Client, config: NatsConfig) -> CatgaResult<Self> {
        Self::from_client_with_codec(client, config, MemoryPackCodec::default()).await
    }

    /// Builds a transport from an application-owned NATS client.
    ///
    /// This is equivalent to [`Self::from_client`] and is available for applications that use
    /// `connect_*` naming for their transport factories. The supplied client is reused without
    /// opening `config.server`.
    pub async fn connect_with_client(
        client: async_nats::Client,
        config: NatsConfig,
    ) -> CatgaResult<Self> {
        Self::connect_with_client_with_codec(client, config, MemoryPackCodec::default()).await
    }
}

impl<C> NatsTransport<C>
where
    C: EnvelopeCodec,
{
    /// Connects with a caller-provided envelope codec and provisions the configured resources.
    ///
    /// The selected codec defines the envelope bytes written to and read from NATS. It must be
    /// compatible with every producer and consumer that shares the configured subjects.
    pub async fn connect_with_codec(config: NatsConfig, codec: C) -> CatgaResult<Self> {
        validate_config(&config)?;
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(map_error)?;
        Self::from_client_with_codec(client, config, codec).await
    }

    /// Builds a transport from an application-owned NATS client and caller-provided codec.
    ///
    /// This preserves the client's TLS, authentication, reconnection, and observability behavior
    /// while idempotently provisioning the stream and durable consumer in `config`. The supplied
    /// codec encodes published envelopes and decodes received envelopes; `config.server` is not
    /// opened by this constructor.
    pub async fn from_client_with_codec(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config, codec).await
    }

    /// Builds a transport from an application-owned NATS client and caller-provided codec.
    ///
    /// This is equivalent to [`Self::from_client_with_codec`] and is available for applications
    /// that use `connect_*` naming for transport factories. The supplied client is reused without
    /// opening `config.server`.
    pub async fn connect_with_client_with_codec(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config, codec).await
    }

    async fn initialize(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
    ) -> CatgaResult<Self> {
        validate_config(&config)?;
        let context = jetstream::new(client.clone());
        let stream = context
            .get_or_create_stream(stream::Config {
                name: config.stream.to_string(),
                subjects: vec![config.subject.to_string()],
                ..Default::default()
            })
            .await
            .map_err(map_error)?;
        let consumer = stream
            .get_or_create_consumer(
                config.consumer.as_ref(),
                pull::Config {
                    durable_name: Some(config.consumer.to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            context,
            subject: config.subject,
            codec,
            consumer,
            destinations: DashMap::new(),
            operations: OperationTracker::default(),
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Idempotently provisions and registers the JetStream resources for one destination.
    ///
    /// The destination is unavailable to [`DestinationTransport::send_to`] until this method
    /// succeeds.  A second registration of the same name returns [`ErrorCode::Conflict`], even
    /// when its resource configuration would be identical.
    pub async fn provision_destination(
        &self,
        destination: Destination,
        config: NatsDestinationConfig,
    ) -> CatgaResult<()> {
        validate_destination_config(&config)?;
        if self.destinations.contains_key(&destination) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "NATS destination is already provisioned",
            ));
        }
        let stream = self
            .context
            .get_or_create_stream(stream::Config {
                name: config.stream.to_string(),
                subjects: vec![config.subject.to_string()],
                ..Default::default()
            })
            .await
            .map_err(map_error)?;
        let consumer = stream
            .get_or_create_consumer(
                config.consumer.as_ref(),
                pull::Config {
                    durable_name: Some(config.consumer.to_string()),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_error)?;
        let resource = NatsDestination {
            subject: config.subject,
            consumer,
        };
        match self.destinations.entry(destination) {
            Entry::Vacant(entry) => {
                entry.insert(resource);
                Ok(())
            }
            Entry::Occupied(_) => Err(CatgaError::new(
                ErrorCode::Conflict,
                "NATS destination is already provisioned",
            )),
        }
    }

    fn destination(&self, destination: &Destination) -> CatgaResult<NatsDestination> {
        self.destinations
            .get(destination)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::NotFound, "NATS destination is not provisioned")
            })
    }

    async fn publish_durable(&self, subject: &str, envelope: Envelope) -> CatgaResult<()> {
        let payload = encode_envelope(&self.codec, &envelope)?;
        if envelope.metadata().quality_of_service() == QualityOfService::ExactlyOnce {
            let acknowledgement = self
                .context
                .send_publish(
                    subject.to_owned(),
                    jetstream::context::Publish::build()
                        .payload(payload.into())
                        .message_id(envelope.metadata().message_id().to_string()),
                )
                .await
                .map_err(map_error)?
                .await
                .map_err(map_error)?;
            record_broker_duplicate(acknowledgement.duplicate);
            Ok(())
        } else {
            self.context
                .publish(subject.to_owned(), payload.into())
                .await
                .map_err(map_error)?
                .await
                .map_err(map_error)
                .map(|_| ())
        }
    }

    async fn receive_consumer(&self, consumer: &consumer::PullConsumer) -> CatgaResult<Delivery> {
        loop {
            let mut batch = consumer
                .batch()
                .max_messages(1)
                .expires(Duration::from_secs(30))
                .messages()
                .await
                .map_err(map_error)?;
            let Some(message) = batch.next().await else {
                continue;
            };
            let message = message.map_err(map_error)?;
            let envelope = decode_envelope(&self.codec, &message.payload)?;
            // JetStream embeds delivery metadata in the acknowledgement subject. `info` parses
            // that borrowed subject without allocating, so capture it before moving `message`
            // into its acknowledger. Core or malformed broker metadata safely falls back to the
            // invariant for every received value: this is at least its first delivery.
            let attempts = message
                .info()
                .ok()
                .and_then(|info| u32::try_from(info.delivered).ok())
                .filter(|attempts| *attempts > 0)
                .unwrap_or(1);
            return Ok(Delivery::with_acknowledger(
                envelope,
                Box::new(NatsAcknowledger {
                    message,
                    _operation: self.operations.begin_operation(),
                }),
            )
            .with_attempts(attempts));
        }
    }
}

#[async_trait]
impl<C> MessageTransport for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let mode = publish_mode(envelope.metadata().quality_of_service());
        match mode {
            NatsPublishMode::Core => {
                let _ = envelope;
                Err(CatgaError::new(
                    ErrorCode::Unsupported,
                    "durable NATS transport does not support AtMostOnce; use NatsPubSubTransport",
                ))
            }
            NatsPublishMode::JetStream => {
                telemetry::record_message_publish("nats", "jetstream", async {
                    self.acceptance.ensure_accepting()?;
                    self.context
                        .publish(
                            self.subject.to_string(),
                            encode_envelope(&self.codec, &envelope)?.into(),
                        )
                        .await
                        .map_err(map_error)?
                        .await
                        .map_err(map_error)
                        .map(|_| ())
                })
                .await
            }
            NatsPublishMode::JetStreamDeduplicated => {
                telemetry::record_message_publish("nats", "jetstream_deduplicated", async {
                    self.acceptance.ensure_accepting()?;
                    let acknowledgement = self
                        .context
                        .send_publish(
                            self.subject.to_string(),
                            jetstream::context::Publish::build()
                                .payload(encode_envelope(&self.codec, &envelope)?.into())
                                .message_id(envelope.metadata().message_id().to_string()),
                        )
                        .await
                        .map_err(map_error)?
                        .await
                        .map_err(map_error)?;
                    record_broker_duplicate(acknowledgement.duplicate);
                    Ok(())
                })
                .await
            }
        }
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("nats", "jetstream", async {
            self.receive_consumer(&self.consumer).await
        })
        .await
    }
}

#[async_trait]
impl<C> DestinationTransport for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    async fn send_to(&self, destination: &Destination, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("nats", "jetstream_destination", async {
            self.acceptance.ensure_accepting()?;
            let resource = self.destination(destination)?;
            self.publish_durable(resource.subject.as_ref(), envelope)
                .await
        })
        .await
    }

    async fn receive_from(&self, destination: &Destination) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("nats", "jetstream_destination", async {
            let resource = self.destination(destination)?;
            self.receive_consumer(&resource.consumer).await
        })
        .await
    }
}

impl<C> Stoppable for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl<C> AsyncInitializable for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl<C> HealthCheckable for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("NATS transport is ready")
    }
}

#[async_trait]
impl<C> Waitable for NatsTransport<C>
where
    C: EnvelopeCodec,
{
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        self.operations.wait_for_completion(cancellation).await
    }

    fn pending_operations(&self) -> usize {
        self.operations.pending_operations()
    }
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

/// Delegates envelope encoding to the codec selected when the transport was constructed.
fn encode_envelope<C: EnvelopeCodec>(codec: &C, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
    codec.encode(envelope)
}

/// Delegates envelope decoding to the codec selected when the transport was constructed.
fn decode_envelope<C: EnvelopeCodec>(codec: &C, bytes: &[u8]) -> CatgaResult<Envelope> {
    codec.decode(bytes)
}

/// Records the broker-owned duplicate decision for one ExactlyOnce publication.
///
/// JetStream retains the deduplication window, so this adapter stores neither a local identity
/// cache nor an eviction counter. A duplicate acknowledgement is a successful publication: the
/// broker has already retained the original message under the caller's stable message ID.
fn record_broker_duplicate(duplicate: bool) {
    if duplicate {
        metrics::counter!(NATS_DEDUP_DROPS).increment(1);
        tracing::debug!(
            target: catga_core::TRACING_TARGET,
            "JetStream suppressed an ExactlyOnce duplicate publication"
        );
    }
}

fn validate_destination_config(config: &NatsDestinationConfig) -> CatgaResult<()> {
    if config.stream.trim().is_empty()
        || config.subject.trim().is_empty()
        || config.consumer.trim().is_empty()
    {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "NATS destination stream, subject, and consumer must not be empty",
        ));
    }
    Ok(())
}

fn validate_config(config: &NatsConfig) -> CatgaResult<()> {
    if config.stream.trim().is_empty()
        || config.subject.trim().is_empty()
        || config.consumer.trim().is_empty()
    {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "NATS stream, subject, and consumer must not be empty",
        ));
    }
    Ok(())
}

const fn publish_mode(quality_of_service: QualityOfService) -> NatsPublishMode {
    match quality_of_service {
        QualityOfService::AtMostOnce => NatsPublishMode::Core,
        QualityOfService::AtLeastOnce => NatsPublishMode::JetStream,
        QualityOfService::ExactlyOnce => NatsPublishMode::JetStreamDeduplicated,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    };

    use catga_core::{Envelope, EnvelopeCodec, MessageMetadata, QualityOfService};
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use crate::{NatsConfig, NatsTransport};

    use super::{
        NatsPublishMode, decode_envelope, encode_envelope, publish_mode, record_broker_duplicate,
        validate_config,
    };

    #[derive(Default)]
    struct RecordingCodec {
        encoded: AtomicUsize,
        decoded: AtomicUsize,
        envelope: Mutex<Option<Envelope>>,
    }

    impl EnvelopeCodec for RecordingCodec {
        fn encode(&self, envelope: &Envelope) -> catga_core::CatgaResult<Vec<u8>> {
            self.encoded.fetch_add(1, Ordering::Relaxed);
            let mut stored = self.envelope.lock().map_err(|_| {
                catga_core::CatgaError::new(
                    catga_core::ErrorCode::Internal,
                    "test codec lock poisoned",
                )
            })?;
            *stored = Some(envelope.clone());
            Ok(b"recording-codec".to_vec())
        }

        fn decode(&self, bytes: &[u8]) -> catga_core::CatgaResult<Envelope> {
            self.decoded.fetch_add(1, Ordering::Relaxed);
            if bytes != b"recording-codec" {
                return Err(catga_core::CatgaError::new(
                    catga_core::ErrorCode::Internal,
                    "unexpected test codec bytes",
                ));
            }
            self.envelope
                .lock()
                .map_err(|_| {
                    catga_core::CatgaError::new(
                        catga_core::ErrorCode::Internal,
                        "test codec lock poisoned",
                    )
                })?
                .clone()
                .ok_or_else(|| {
                    catga_core::CatgaError::new(
                        catga_core::ErrorCode::Internal,
                        "missing test envelope",
                    )
                })
        }
    }

    #[derive(Default)]
    struct CounterValue(AtomicU64);

    #[derive(Default)]
    struct CounterRecorder(Arc<CounterValue>);

    impl Recorder for CounterRecorder {
        fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

        fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::clone(&self.0))
        }

        fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    impl CounterFn for CounterValue {
        fn increment(&self, value: u64) {
            self.0.fetch_add(value, Ordering::Relaxed);
        }

        fn absolute(&self, value: u64) {
            self.0.store(value, Ordering::Relaxed);
        }
    }

    #[test]
    fn quality_of_service_selects_the_native_nats_delivery_path() {
        assert_eq!(
            publish_mode(QualityOfService::AtMostOnce),
            NatsPublishMode::Core
        );
        assert_eq!(
            publish_mode(QualityOfService::AtLeastOnce),
            NatsPublishMode::JetStream
        );
        assert_eq!(
            publish_mode(QualityOfService::ExactlyOnce),
            NatsPublishMode::JetStreamDeduplicated
        );
    }

    #[test]
    fn broker_duplicate_acknowledgements_increment_only_duplicate_drops() {
        let recorder = CounterRecorder::default();
        let guard = metrics::set_default_local_recorder(&recorder);
        record_broker_duplicate(false);
        record_broker_duplicate(true);
        drop(guard);

        assert_eq!(recorder.0.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn transport_config_requires_nonblank_jetstream_resource_names() {
        let valid = NatsConfig {
            server: "nats://unused.invalid".into(),
            stream: "orders".into(),
            subject: "orders.created".into(),
            consumer: "workers".into(),
        };

        assert!(validate_config(&valid).is_ok());

        for config in [
            NatsConfig {
                stream: " ".into(),
                ..valid.clone()
            },
            NatsConfig {
                subject: " ".into(),
                ..valid.clone()
            },
            NatsConfig {
                consumer: " ".into(),
                ..valid.clone()
            },
        ] {
            assert!(validate_config(&config).is_err());
        }
    }

    #[test]
    fn transport_codec_helpers_use_the_injected_envelope_codec_for_encode_and_decode()
    -> catga_core::CatgaResult<()> {
        let codec = RecordingCodec::default();
        let envelope = Envelope::new(
            42,
            "tests.custom-codec",
            vec![1, 2, 3],
            MessageMetadata::new(42, None),
        );

        let encoded = encode_envelope(&codec, &envelope)?;
        let decoded = decode_envelope(&codec, &encoded)?;

        assert_eq!(encoded, b"recording-codec");
        assert_eq!(decoded, envelope);
        assert_eq!(codec.encoded.load(Ordering::Relaxed), 1);
        assert_eq!(codec.decoded.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[test]
    fn connect_validates_transport_config_before_opening_a_connection() {
        let result = futures::executor::block_on(NatsTransport::connect(NatsConfig {
            server: "nats://127.0.0.1:1".into(),
            stream: " ".into(),
            subject: "orders.created".into(),
            consumer: "workers".into(),
        }));

        assert!(matches!(
            result,
            Err(error) if error.code() == catga_core::ErrorCode::Validation
        ));
    }
}
