//! Ephemeral Core NATS Pub/Sub transport.

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Envelope, EnvelopeCodec,
    ErrorCode, HealthCheckable, MessageTransport, QualityOfService, Stoppable, Waitable, telemetry,
};
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::NatsPubSubConfig;

/// A Core NATS transport for low-latency, non-durable AtMostOnce broadcasts.
///
/// The subscription is established by [`Self::connect`]. [`MessageTransport::receive`] polls the
/// subscribed socket directly, so the adapter has no background forwarding task or unbounded
/// application queue. Core NATS cannot recover disconnected messages, acknowledge deliveries, or
/// provide deduplication; use [`crate::NatsTransport`] for JetStream-backed guarantees instead.
pub struct NatsPubSubTransport {
    client: async_nats::Client,
    subject: async_nats::Subject,
    subscription: Mutex<async_nats::Subscriber>,
    codec: PostcardCodec,
    acceptance: AcceptanceGate,
}

impl NatsPubSubTransport {
    /// Connects and subscribes to the configured nonblank Core NATS subject.
    pub async fn connect(config: NatsPubSubConfig) -> CatgaResult<Self> {
        if config.subject.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Core NATS subject must not be empty or whitespace-only",
            ));
        }
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(map_error)?;
        let subject: async_nats::Subject = config.subject.to_string().into();
        let subscription = client.subscribe(subject.clone()).await.map_err(map_error)?;
        Ok(Self {
            client,
            subject,
            subscription: Mutex::new(subscription),
            codec: PostcardCodec,
            acceptance: AcceptanceGate::default(),
        })
    }
}

#[async_trait]
impl MessageTransport for NatsPubSubTransport {
    /// Publishes an AtMostOnce envelope to the configured Core NATS subject.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("nats", "core_pubsub", async {
            self.acceptance.ensure_accepting()?;
            if envelope.metadata().quality_of_service() != QualityOfService::AtMostOnce {
                return Err(CatgaError::new(
                    ErrorCode::Unsupported,
                    "Core NATS Pub/Sub supports AtMostOnce only; use NatsTransport for JetStream delivery guarantees",
                ));
            }
            self.client
                .publish(self.subject.clone(), self.codec.encode(&envelope)?.into())
                .await
                .map_err(map_error)
        })
        .await
    }

    /// Waits for and decodes the next live Core NATS message.
    ///
    /// Returned values have no acknowledger because Core NATS offers no acknowledgement or
    /// redelivery protocol; callers may still use [`MessageTransport::ack`] uniformly.
    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("nats", "core_pubsub", async {
            let mut subscription = self.subscription.lock().await;
            let message = subscription.next().await.ok_or_else(|| {
                CatgaError::new(ErrorCode::Transient, "Core NATS subscription closed")
            })?;
            Ok(Delivery::new(self.codec.decode(&message.payload)?))
        })
        .await
    }
}

impl Stoppable for NatsPubSubTransport {
    /// Rejects later publications while keeping the existing subscription available to drain.
    fn stop_accepting(&self) {
        self.acceptance.stop_accepting();
    }

    /// Returns whether publication is currently accepted.
    fn is_accepting(&self) -> bool {
        self.acceptance.is_accepting()
    }
}

#[async_trait]
impl AsyncInitializable for NatsPubSubTransport {
    /// Subscription setup already completed during [`Self::connect`].
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for NatsPubSubTransport {
    /// A connected transport considers its owned Core NATS subscription ready.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Returns a concise readiness description.
    fn health_status(&self) -> Option<&str> {
        Some("Core NATS Pub/Sub transport is ready")
    }
}

#[async_trait]
impl Waitable for NatsPubSubTransport {
    /// Core NATS deliveries do not retain acknowledgement work, so completion is immediate.
    async fn wait_for_completion(&self, _: CancellationToken) -> CatgaResult<()> {
        Ok(())
    }

    /// Returns zero because Core NATS deliveries are not acknowledgement-backed operations.
    fn pending_operations(&self) -> usize {
        0
    }
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
