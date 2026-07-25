use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{self, pull},
    stream,
};
use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
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

/// NATS transport that maps Catga QoS to Core NATS or JetStream delivery semantics.
///
/// `AtMostOnce` uses Core NATS, `AtLeastOnce` waits for a JetStream publish acknowledgement, and
/// `ExactlyOnce` additionally supplies the envelope message ID to JetStream's deduplication
/// window. Received JetStream deliveries always expose explicit acknowledgement through
/// [`MessageTransport::ack`].
pub struct NatsTransport {
    client: async_nats::Client,
    context: jetstream::Context,
    subject: Box<str>,
    codec: PostcardCodec,
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

impl NatsTransport {
    /// Connects and idempotently provisions the configured stream and durable consumer.
    pub async fn connect(config: NatsConfig) -> CatgaResult<Self> {
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(map_error)?;
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
            client,
            context,
            subject: config.subject,
            codec: PostcardCodec,
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
        let payload = self.codec.encode(&envelope)?;
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
            let envelope = self.codec.decode(&message.payload)?;
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
impl MessageTransport for NatsTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let mode = publish_mode(envelope.metadata().quality_of_service());
        match mode {
            NatsPublishMode::Core => {
                telemetry::record_message_publish("nats", "core", async {
                    self.acceptance.ensure_accepting()?;
                    self.client
                        .publish(
                            self.subject.to_string(),
                            self.codec.encode(&envelope)?.into(),
                        )
                        .await
                        .map_err(map_error)
                })
                .await
            }
            NatsPublishMode::JetStream => {
                telemetry::record_message_publish("nats", "jetstream", async {
                    self.acceptance.ensure_accepting()?;
                    self.context
                        .publish(
                            self.subject.to_string(),
                            self.codec.encode(&envelope)?.into(),
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
                                .payload(self.codec.encode(&envelope)?.into())
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
impl DestinationTransport for NatsTransport {
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

impl Stoppable for NatsTransport {
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl AsyncInitializable for NatsTransport {
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for NatsTransport {
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("NATS transport is ready")
    }
}

#[async_trait]
impl Waitable for NatsTransport {
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
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use catga_core::QualityOfService;
    use metrics::{
        Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
    };

    use super::{NatsPublishMode, publish_mode, record_broker_duplicate};

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
}
