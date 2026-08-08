//! Publish-only JetStream transport composition.

use async_nats::jetstream::{self, stream};
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Envelope, EnvelopeCodec,
    ErrorCode, HealthCheckable, QualityOfService, Stoppable, telemetry,
};

use crate::{NatsPublisherConfig, transport};

/// A JetStream envelope publisher that provisions no durable consumer.
///
/// Use this in API processes and other publish-only deployments. It provisions the configured
/// stream and publishes with the same AtLeastOnce and ExactlyOnce semantics as
/// [`crate::NatsTransport`], without exposing a receive method or allocating a consumer cursor.
pub struct NatsPublisher<C = MemoryPackCodec>
where
    C: EnvelopeCodec,
{
    context: jetstream::Context,
    subject: Box<str>,
    codec: C,
    acceptance: AcceptanceGate,
}

impl NatsPublisher<MemoryPackCodec> {
    /// Connects and idempotently provisions only the configured JetStream stream.
    pub async fn connect(config: NatsPublisherConfig) -> CatgaResult<Self> {
        Self::connect_with_codec(config, MemoryPackCodec::default()).await
    }

    /// Builds a publisher from an application-owned NATS client.
    pub async fn from_client(
        client: async_nats::Client,
        config: NatsPublisherConfig,
    ) -> CatgaResult<Self> {
        Self::from_client_with_codec(client, config, MemoryPackCodec::default()).await
    }
}

impl<C> NatsPublisher<C>
where
    C: EnvelopeCodec,
{
    /// Connects with a caller-provided envelope codec and provisions only the configured stream.
    pub async fn connect_with_codec(config: NatsPublisherConfig, codec: C) -> CatgaResult<Self> {
        validate_config(&config)?;
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(transport::map_error)?;
        Self::from_client_with_codec(client, config, codec).await
    }

    /// Builds a publisher from an application-owned client and caller-provided envelope codec.
    pub async fn from_client_with_codec(
        client: async_nats::Client,
        config: NatsPublisherConfig,
        codec: C,
    ) -> CatgaResult<Self> {
        validate_config(&config)?;
        let context = jetstream::new(client);
        context
            .get_or_create_stream(stream::Config {
                name: config.stream.to_string(),
                subjects: vec![config.subject.to_string()],
                ..Default::default()
            })
            .await
            .map_err(transport::map_error)?;
        Ok(Self {
            context,
            subject: config.subject,
            codec,
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Publishes one durable envelope without creating or using a consumer.
    pub async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("nats", "jetstream_publisher", async {
            self.acceptance.ensure_accepting()?;
            match envelope.metadata().quality_of_service() {
                QualityOfService::AtMostOnce => Err(CatgaError::new(
                    ErrorCode::Unsupported,
                    "durable NATS publisher does not support AtMostOnce; use NatsPubSubTransport",
                )),
                QualityOfService::AtLeastOnce => self
                    .context
                    .publish(
                        self.subject.to_string(),
                        transport::encode_envelope(&self.codec, &envelope)?.into(),
                    )
                    .await
                    .map_err(transport::map_error)?
                    .await
                    .map_err(transport::map_error)
                    .map(|_| ()),
                QualityOfService::ExactlyOnce => {
                    let acknowledgement = self
                        .context
                        .send_publish(
                            self.subject.to_string(),
                            jetstream::message::PublishMessage::build()
                                .payload(transport::encode_envelope(&self.codec, &envelope)?.into())
                                .message_id(envelope.metadata().message_id().to_string()),
                        )
                        .await
                        .map_err(transport::map_error)?
                        .await
                        .map_err(transport::map_error)?;
                    transport::record_broker_duplicate(acknowledgement.duplicate);
                    Ok(())
                }
            }
        })
        .await
    }
}

impl<C> Stoppable for NatsPublisher<C>
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

#[async_trait::async_trait]
impl<C> AsyncInitializable for NatsPublisher<C>
where
    C: EnvelopeCodec,
{
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl<C> HealthCheckable for NatsPublisher<C>
where
    C: EnvelopeCodec,
{
    fn is_healthy(&self) -> bool {
        true
    }

    fn health_status(&self) -> Option<&str> {
        Some("NATS publisher is ready")
    }
}

fn validate_config(config: &NatsPublisherConfig) -> CatgaResult<()> {
    for (name, value) in [
        ("NATS server", config.server.as_ref()),
        ("NATS stream", config.stream.as_ref()),
        ("NATS subject", config.subject.as_ref()),
    ] {
        if value.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!("{name} must not be empty"),
            ));
        }
    }
    Ok(())
}

