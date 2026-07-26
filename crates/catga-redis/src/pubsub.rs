//! Ephemeral Redis Pub/Sub transport.

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaError, CatgaResult, Delivery, Envelope, EnvelopeCodec,
    ErrorCode, HealthCheckable, MessageTransport, Stoppable, Waitable, telemetry,
};
use futures::StreamExt;
use redis::{AsyncCommands, Script};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{RedisPubSubConfig, transport::map_error};

const DEDUPLICATION_TTL_MILLISECONDS: u64 = 300_000;

// Setting the key and publishing in one Lua invocation closes the race between two concurrent
// exactly-once publishers. A duplicate returns -1 and does not emit a second broadcast.
const PUBLISH_EXACTLY_ONCE: &str = r#"
if redis.call('SET', KEYS[1], '1', 'PX', ARGV[1], 'NX') then
    return redis.call('PUBLISH', KEYS[2], ARGV[2])
end
return -1
"#;

// Receiver keys are instance-specific, so every active subscriber can observe its first copy
// while repeated copies for that subscriber are discarded. TTL bounds Redis memory retention.
const CLAIM_RECEIVED_EXACTLY_ONCE: &str = r#"
if redis.call('SET', KEYS[1], '1', 'PX', ARGV[1], 'NX') then return 1 end
return 0
"#;

/// A Redis Pub/Sub transport for low-latency, non-durable broadcasts.
///
/// The transport subscribes during [`Self::connect`], before it becomes available to callers.
/// [`MessageTransport::receive`] holds the subscription only while awaiting its next network
/// message, so no background task or unbounded in-process queue is required. Redis does not
/// retain Pub/Sub messages: a disconnected subscriber does not receive historical publications.
/// Exactly-once envelopes are deduplicated through bounded Redis keys for five minutes; this
/// suppresses concurrent repeated publications without retaining an unbounded in-process cache.
pub struct RedisPubSubTransport {
    client: redis::Client,
    channel: Box<str>,
    subscription: Mutex<redis::aio::PubSub>,
    receiver_id: Uuid,
    codec: PostcardCodec,
    acceptance: AcceptanceGate,
}

impl RedisPubSubTransport {
    /// Connects and subscribes to the configured nonblank Redis channel.
    ///
    /// The subscription is created before this method returns, preventing a race in which a
    /// caller publishes the first broadcast before its local receiver is registered.
    pub async fn connect(config: RedisPubSubConfig) -> CatgaResult<Self> {
        let client = redis::Client::open(config.server.as_ref()).map_err(map_error)?;
        Self::from_client(client, config).await
    }

    /// Builds a Pub/Sub transport from an application-owned Redis client.
    ///
    /// This retains the supplied client's TLS, authentication, reconnection, and observability
    /// configuration. `config.server` is not opened by this constructor; it remains part of
    /// [`RedisPubSubConfig`] for compatibility with [`Self::connect`]. The configured channel is
    /// validated and subscribed before this method returns.
    pub async fn from_client(
        client: redis::Client,
        config: RedisPubSubConfig,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config).await
    }

    /// Builds a Pub/Sub transport from an application-owned Redis client.
    ///
    /// This is equivalent to [`Self::from_client`] and is available for applications that use
    /// `connect_*` naming for their transport factories. The configured channel is validated and
    /// subscribed before the returned transport can receive messages.
    pub async fn connect_with_client(
        client: redis::Client,
        config: RedisPubSubConfig,
    ) -> CatgaResult<Self> {
        Self::initialize(client, config).await
    }

    async fn initialize(client: redis::Client, config: RedisPubSubConfig) -> CatgaResult<Self> {
        if config.channel.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis Pub/Sub channel must not be empty or whitespace-only",
            ));
        }
        let mut subscription = client.get_async_pubsub().await.map_err(map_error)?;
        subscription
            .subscribe(config.channel.as_ref())
            .await
            .map_err(map_error)?;
        Ok(Self {
            client,
            channel: config.channel,
            subscription: Mutex::new(subscription),
            receiver_id: Uuid::new_v4(),
            codec: PostcardCodec,
            acceptance: AcceptanceGate::default(),
        })
    }

    /// Derives a fixed-size Redis key without exposing a business channel or message identity.
    fn deduplication_key(&self, scope: &[u8], message_id: u64) -> Box<str> {
        let mut digest = Sha256::new();
        digest.update(scope);
        digest.update(self.channel.len().to_be_bytes());
        digest.update(self.channel.as_bytes());
        digest.update(message_id.to_be_bytes());
        if scope == b"receive" {
            digest.update(self.receiver_id.as_bytes());
        }
        format!("catga:pubsub:dedup:{:x}", digest.finalize()).into_boxed_str()
    }

    /// Atomically reserves one received exactly-once identity for this subscriber instance.
    async fn claim_received(&self, message_id: u64) -> CatgaResult<bool> {
        let key = self.deduplication_key(b"receive", message_id);
        let mut connection = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_error)?;
        let claimed: i64 = Script::new(CLAIM_RECEIVED_EXACTLY_ONCE)
            .key(key.as_ref())
            .arg(DEDUPLICATION_TTL_MILLISECONDS)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(claimed == 1)
    }
}

#[async_trait]
impl MessageTransport for RedisPubSubTransport {
    /// Publishes one envelope to the configured ephemeral Redis channel.
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        telemetry::record_message_publish("redis", "pubsub", async {
            self.acceptance.ensure_accepting()?;
            let payload = self.codec.encode(&envelope)?;
            let mut connection = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(map_error)?;
            if envelope
                .metadata()
                .quality_of_service()
                .requires_deduplication()
            {
                let key = self.deduplication_key(b"publish", envelope.metadata().message_id());
                let _: i64 = Script::new(PUBLISH_EXACTLY_ONCE)
                    .key(key.as_ref())
                    .key(self.channel.as_ref())
                    .arg(DEDUPLICATION_TTL_MILLISECONDS)
                    .arg(payload)
                    .invoke_async(&mut connection)
                    .await
                    .map_err(map_error)?;
            } else {
                let _: usize = connection
                    .publish(self.channel.as_ref(), payload)
                    .await
                    .map_err(map_error)?;
            }
            Ok(())
        })
        .await
    }

    /// Waits for and decodes the next live broadcast.
    ///
    /// Returned values have no acknowledger because Redis Pub/Sub has no acknowledgement or
    /// redelivery protocol; callers may still use [`MessageTransport::ack`] uniformly.
    async fn receive(&self) -> CatgaResult<Delivery> {
        telemetry::record_message_receive("redis", "pubsub", async {
            let mut subscription = self.subscription.lock().await;
            loop {
                let message = subscription.on_message().next().await.ok_or_else(|| {
                    CatgaError::new(ErrorCode::Transient, "Redis Pub/Sub subscription closed")
                })?;
                let envelope = self.codec.decode(message.get_payload_bytes())?;
                if envelope
                    .metadata()
                    .quality_of_service()
                    .requires_deduplication()
                    && !self
                        .claim_received(envelope.metadata().message_id())
                        .await?
                {
                    continue;
                }
                return Ok(Delivery::new(envelope));
            }
        })
        .await
    }
}

impl Stoppable for RedisPubSubTransport {
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
impl AsyncInitializable for RedisPubSubTransport {
    /// Pub/Sub subscription setup already completed during [`Self::connect`].
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl HealthCheckable for RedisPubSubTransport {
    /// A connected transport considers its owned subscription ready.
    fn is_healthy(&self) -> bool {
        true
    }

    /// Returns a concise readiness description.
    fn health_status(&self) -> Option<&str> {
        Some("Redis Pub/Sub transport is ready")
    }
}

#[async_trait]
impl Waitable for RedisPubSubTransport {
    /// Pub/Sub deliveries do not retain acknowledgement work, so completion is immediate.
    async fn wait_for_completion(&self, _: CancellationToken) -> CatgaResult<()> {
        Ok(())
    }

    /// Returns zero because Pub/Sub deliveries are not acknowledgement-backed operations.
    fn pending_operations(&self) -> usize {
        0
    }
}
