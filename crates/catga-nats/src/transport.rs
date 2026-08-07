use std::{sync::Arc, time::Duration};

use async_nats::jetstream::{
    self,
    consumer::{self, pull},
    stream,
};
use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Destination,
    DestinationTransport, Envelope, EnvelopeCodec, ErrorCode, HealthCheckable, MessageTransport,
    OperationTracker, QualityOfService, Stoppable, Waitable, telemetry,
};
use dashmap::{DashMap, mapref::entry::Entry};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    NatsConfig, NatsConsumerMode, NatsConsumerOptions, NatsDestinationConfig, NatsReceiveOptions,
    NatsTransportOptions, acknowledgement::NatsAcknowledger,
};

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
/// Receives pull batches of 64 deliveries by default; use `*_with_receive_options` constructors
/// and [`NatsReceiveOptions`] to select another bounded prefetch size.
pub struct NatsTransport<C = MemoryPackCodec>
where
    C: EnvelopeCodec,
{
    context: jetstream::Context,
    subject: Box<str>,
    codec: C,
    consumer: consumer::PullConsumer,
    receive_options: NatsReceiveOptions,
    consumer_batch: Mutex<Option<pull::Batch>>,
    destinations: DashMap<Destination, NatsDestination>,
    operations: OperationTracker,
    acceptance: AcceptanceGate,
}

/// Provisioned JetStream handles for one named Catga destination.
#[derive(Clone)]
struct NatsDestination {
    subject: Box<str>,
    consumer: consumer::PullConsumer,
    batch: Arc<Mutex<Option<pull::Batch>>>,
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
    /// Connects and idempotently provisions the configured stream and default durable consumer.
    pub async fn connect(config: NatsConfig) -> CatgaResult<Self> {
        Self::connect_with_options(config, NatsTransportOptions::default()).await
    }

    /// Connects with caller-selected pull buffering and consumer lifecycle settings.
    ///
    /// Use [`NatsConsumerOptions::ephemeral`] for a one-off replay that must not preserve a
    /// JetStream cursor. The configured consumer name is ignored in that mode; JetStream assigns
    /// a transient name and removes it after the optional inactivity threshold.
    pub async fn connect_with_options(
        config: NatsConfig,
        options: NatsTransportOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_codec_and_options(config, MemoryPackCodec::default(), options).await
    }

    /// Connects with caller-selected bounded JetStream pull buffering.
    pub async fn connect_with_receive_options(
        config: NatsConfig,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_options(
            config,
            NatsTransportOptions::default().with_receive(receive_options),
        )
        .await
    }

    /// Connects with caller-selected JetStream consumer lifecycle settings.
    pub async fn connect_with_consumer_options(
        config: NatsConfig,
        consumer_options: NatsConsumerOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_options(
            config,
            NatsTransportOptions::default().with_consumer(consumer_options),
        )
        .await
    }

    /// Builds a transport from an application-owned NATS client.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior while idempotently provisioning the stream and durable consumer in
    /// `config`. `config.server` is not opened by this constructor; it remains part of
    /// [`NatsConfig`] for compatibility with [`Self::connect`].
    pub async fn from_client(client: async_nats::Client, config: NatsConfig) -> CatgaResult<Self> {
        Self::from_client_with_options(client, config, NatsTransportOptions::default()).await
    }

    /// Builds a transport from an application-owned client with caller-selected options.
    pub async fn from_client_with_options(
        client: async_nats::Client,
        config: NatsConfig,
        options: NatsTransportOptions,
    ) -> CatgaResult<Self> {
        Self::from_client_with_codec_and_options(
            client,
            config,
            MemoryPackCodec::default(),
            options,
        )
        .await
    }

    /// Builds a transport from an application-owned client with caller-selected pull buffering.
    pub async fn from_client_with_receive_options(
        client: async_nats::Client,
        config: NatsConfig,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::from_client_with_options(
            client,
            config,
            NatsTransportOptions::default().with_receive(receive_options),
        )
        .await
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
        Self::from_client(client, config).await
    }

    /// Alias for [`Self::from_client_with_receive_options`].
    pub async fn connect_with_client_and_receive_options(
        client: async_nats::Client,
        config: NatsConfig,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::from_client_with_receive_options(client, config, receive_options).await
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
        Self::connect_with_codec_and_receive_options(config, codec, NatsReceiveOptions::default())
            .await
    }

    /// Connects with a caller-provided codec and bounded JetStream pull buffering.
    pub async fn connect_with_codec_and_receive_options(
        config: NatsConfig,
        codec: C,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::connect_with_codec_and_options(
            config,
            codec,
            NatsTransportOptions::default().with_receive(receive_options),
        )
        .await
    }

    /// Connects with a caller-provided codec plus transport and consumer options.
    pub async fn connect_with_codec_and_options(
        config: NatsConfig,
        codec: C,
        options: NatsTransportOptions,
    ) -> CatgaResult<Self> {
        validate_config(&config)?;
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(map_error)?;
        Self::from_client_with_codec_and_options(client, config, codec, options).await
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
        Self::from_client_with_codec_and_options(
            client,
            config,
            codec,
            NatsTransportOptions::default(),
        )
        .await
    }

    /// Builds a transport from an application-owned client with a codec and pull buffering.
    pub async fn from_client_with_codec_and_receive_options(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::from_client_with_codec_and_options(
            client,
            config,
            codec,
            NatsTransportOptions::default().with_receive(receive_options),
        )
        .await
    }

    /// Builds a transport from an application-owned client with a codec and full options.
    pub async fn from_client_with_codec_and_options(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
        options: NatsTransportOptions,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config, codec, options).await
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
        Self::connect_with_client_with_codec_and_receive_options(
            client,
            config,
            codec,
            NatsReceiveOptions::default(),
        )
        .await
    }

    /// Alias for [`Self::from_client_with_codec_and_receive_options`].
    pub async fn connect_with_client_with_codec_and_receive_options(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
        receive_options: NatsReceiveOptions,
    ) -> CatgaResult<Self> {
        Self::from_client_with_codec_and_receive_options(client, config, codec, receive_options)
            .await
    }

    async fn initialize(
        client: async_nats::Client,
        config: NatsConfig,
        codec: C,
        options: NatsTransportOptions,
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
        let consumer = provision_consumer(
            &stream,
            config.subject.as_ref(),
            config.consumer.as_ref(),
            options.consumer(),
        )
        .await?;
        Ok(Self {
            context,
            subject: config.subject,
            codec,
            consumer,
            receive_options: options.receive(),
            consumer_batch: Mutex::new(None),
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
                    filter_subject: config.subject.to_string(),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_error)?;
        let resource = NatsDestination {
            subject: config.subject,
            consumer,
            batch: Arc::new(Mutex::new(None)),
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
                    jetstream::message::PublishMessage::build()
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

    async fn receive_consumer(
        &self,
        consumer: &consumer::PullConsumer,
        batch_slot: &Mutex<Option<pull::Batch>>,
    ) -> CatgaResult<Delivery> {
        loop {
            let mut batch = batch_slot.lock().await;
            if batch.is_none() {
                *batch = Some(
                    consumer
                        .batch()
                        .max_messages(self.receive_options.pull_batch_size().get())
                        .expires(Duration::from_secs(30))
                        .messages()
                        .await
                        .map_err(map_error)?,
                );
            }
            let Some(active_batch) = batch.as_mut() else {
                continue;
            };
            let Some(message) = active_batch.next().await else {
                *batch = None;
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
                            jetstream::message::PublishMessage::build()
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
            self.receive_consumer(&self.consumer, &self.consumer_batch)
                .await
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
            self.receive_consumer(&resource.consumer, resource.batch.as_ref())
                .await
        })
        .await
    }

    fn declare_destination(&self, destination: &Destination) -> CatgaResult<()> {
        if self.destinations.contains_key(destination) {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::NotFound,
                "NATS destination is not provisioned; call provision_destination before building routed endpoints",
            ))
        }
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
        // Check if the underlying NATS client is connected
        matches!(
            self.context.client().connection_state(),
            async_nats::connection::State::Connected
        )
    }

    fn health_status(&self) -> Option<&str> {
        if self.is_healthy() {
            Some("NATS transport is connected")
        } else {
            Some("NATS transport is disconnected")
        }
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

pub(crate) fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

/// Delegates envelope encoding to the codec selected when the transport was constructed.
pub(crate) fn encode_envelope<C: EnvelopeCodec>(
    codec: &C,
    envelope: &Envelope,
) -> CatgaResult<Vec<u8>> {
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
pub(crate) fn record_broker_duplicate(duplicate: bool) {
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

async fn provision_consumer(
    stream: &stream::Stream,
    subject: &str,
    name: &str,
    options: NatsConsumerOptions,
) -> CatgaResult<consumer::PullConsumer> {
    let mut config = pull::Config {
        filter_subject: subject.to_owned(),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ..Default::default()
    };
    if let Some(inactive_threshold) = options.inactive_threshold() {
        config.inactive_threshold = inactive_threshold;
    }
    match options.mode() {
        NatsConsumerMode::Durable => {
            config.durable_name = Some(name.to_owned());
            stream
                .get_or_create_consumer(name, config)
                .await
                .map_err(map_error)
        }
        NatsConsumerMode::Ephemeral => stream.create_consumer(config).await.map_err(map_error),
    }
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
