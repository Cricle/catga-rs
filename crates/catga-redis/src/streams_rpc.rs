//! Durable Redis Streams request ingress with per-request Pub/Sub reply inboxes.
//!
//! The request client writes each request to the supplied [`DestinationTransport`], which is
//! normally [`crate::RedisTransport`] and therefore a Redis Stream. Every call subscribes to a
//! fresh, random Redis Pub/Sub reply channel before writing the request. The reply channel has no
//! retained backlog and consumes only one subscription per in-flight request.
//!
//! A request server does not acknowledge its source [`Delivery`] until publishing the response
//! succeeds. Consequently, a process, decode, handler, or response-publication failure before
//! acknowledgement leaves the stream entry pending for the transport's normal redelivery path.

use std::{future::Future, sync::Arc, time::Duration};

use catga_core::codec::memorypack::{MemoryPackCodec, MemoryPackDeserialize, MemoryPackSerialize};
use catga_core::{
    CatgaError, CatgaResult, Delivery, Destination, DestinationTransport, Envelope, EnvelopeCodec,
    ErrorCode, Handler, Request, RequestTransport,
};
use futures::StreamExt;
use redis::AsyncCommands;

use crate::transport::map_error;

/// A request client that durably sends ingress through a [`DestinationTransport`].
///
/// Each request uses a distinct, temporary Redis Pub/Sub reply inbox. The client retains no
/// process-wide reply registry, and its per-call state is bounded to one subscription and one
/// encoded envelope. Replies published after the caller times out are intentionally discarded by
/// Redis Pub/Sub because the inbox subscription has already been dropped. Its timeout is one
/// end-to-end budget covering reply-inbox connection, subscription, durable ingress, and reply.
#[derive(Clone)]
pub struct RedisStreamsRequestClient<T: ?Sized> {
    transport: Arc<T>,
    client: redis::Client,
    codec: MemoryPackCodec,
}

impl<T> RedisStreamsRequestClient<T>
where
    T: DestinationTransport + ?Sized,
{
    /// Creates a client over a durable destination transport and a Redis reply connection.
    ///
    /// `server` must address the same Redis deployment used by the request server's reply
    /// publisher. Opening the Redis client validates the URL without performing network I/O.
    pub fn new(transport: Arc<T>, server: &str) -> CatgaResult<Self> {
        Ok(Self {
            transport,
            client: redis::Client::open(server).map_err(map_error)?,
            codec: MemoryPackCodec::default(),
        })
    }

    /// Durably sends `request` to `destination` and waits for one correlated reply.
    ///
    /// `timeout` is one end-to-end budget for connecting the reply inbox, subscribing it, writing
    /// the durable ingress entry, and waiting for the reply. A timeout does not reliably cancel a
    /// Redis command that reached the broker, so the server can still complete and acknowledge a
    /// request later. An error before the server acknowledges a delivery leaves it eligible for
    /// normal Streams redelivery.
    pub async fn request_to(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        let destination = Destination::parse(destination)?;
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis Streams request timeout must be greater than zero",
            ));
        }

        let reply_to: Box<str> =
            format!("catga.reply.{}", uuid::Uuid::new_v4()).into_boxed_str();
        let mut subscription = self.client.get_async_pubsub().await.map_err(|e| {
            CatgaError::new(
                ErrorCode::Connection,
                format!("Redis connection failed: {e}"),
            )
        })?;
        subscription
            .subscribe(reply_to.as_ref())
            .await
            .map_err(|e| {
                CatgaError::new(
                    ErrorCode::Connection,
                    format!("Redis subscription failed: {e}"),
                )
            })?;
        self.transport
            .send_to(&destination, request.with_reply_to(reply_to))
            .await?;

        run_request_with_timeout(timeout, reply_to, &mut subscription, async {
            let reply = subscription.on_message().next().await.ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Transient,
                    "Redis Streams reply subscription closed",
                )
            })?;
            self.codec.decode(reply.get_payload_bytes())
        })
        .await
    }
}

#[async_trait::async_trait]
impl<T> RequestTransport for RedisStreamsRequestClient<T>
where
    T: DestinationTransport + ?Sized,
{
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        self.request_to(destination, request, timeout).await
    }
}

/// A server that receives durable requests from one [`Destination`].
///
/// Cloning the surrounding [`Arc`] lets multiple tasks call [`Self::next`] concurrently. Their
/// `DestinationTransport` remains responsible for coordinating consumer-group delivery and
/// recovery; this server adds only request decoding, reply publication, and ordered acknowledgement.
pub struct RedisStreamsRequestServer<T: ?Sized> {
    transport: Arc<T>,
    destination: Destination,
    client: redis::Client,
    codec: MemoryPackCodec,
}

impl<T> RedisStreamsRequestServer<T>
where
    T: DestinationTransport + ?Sized,
{
    /// Creates a server for one validated durable request destination.
    ///
    /// `server` must address the Redis deployment to which clients subscribed for temporary reply
    /// inboxes. Opening this client validates its URL without performing network I/O.
    pub fn new(transport: Arc<T>, destination: Destination, server: &str) -> CatgaResult<Self> {
        Ok(Self {
            transport,
            destination,
            client: redis::Client::open(server).map_err(map_error)?,
            codec: MemoryPackCodec::default(),
        })
    }

    /// Returns the durable ingress destination used by this server.
    pub fn destination(&self) -> &Destination {
        &self.destination
    }

    /// Receives one durable request without acknowledging it.
    ///
    /// Dropping the returned request, returning an error while decoding it, or failing before
    /// [`RedisStreamsRequest::respond`] completes leaves the delivery available for redelivery.
    pub async fn next(&self) -> CatgaResult<RedisStreamsRequest> {
        let delivery = self.transport.receive_from(&self.destination).await?;
        Ok(RedisStreamsRequest {
            delivery,
            client: self.client.clone(),
            codec: self.codec,
        })
    }

    /// Receives one typed request, then publishes either its successful response or its remote
    /// error before acknowledging the durable ingress delivery.
    ///
    /// Any failure before a response is successfully published is returned and deliberately does
    /// not acknowledge the request, preserving at-least-once redelivery semantics.
    pub async fn handle_next<M, H>(&self, handler: &H) -> CatgaResult<()>
    where
        M: Request + MemoryPackDeserialize,
        M::Response: MemoryPackSerialize,
        H: Handler<M>,
    {
        let request = self.next().await?;
        match request.decode::<M>() {
            Ok(message) => match handler.handle(message).await {
                Ok(response) => request.respond_value(&response).await,
                Err(error) => request.respond_error(error).await,
            },
            Err(error) => request.respond_error(error).await,
        }
    }
}

/// One durable Redis Streams request awaiting a reply and acknowledgement.
///
/// Calling [`Self::respond`] publishes first and acknowledges second. Thus a failed response
/// publication, a missing reply destination, or a dropped request never silently acknowledges the
/// durable ingress entry. Applications that decide not to respond can call [`Self::nack`] to make
/// the entry immediately eligible for the transport's next receive attempt.
///
/// ## Lifetime semantics
///
/// The underlying delivery is held by value and released only when [`Self::respond`],
/// [`Self::nack`], or [`Drop`] runs. A successful response publication followed by acknowledgement
/// failure still leaves the entry unacknowledged so the transport's redelivery path can recover it.
pub struct RedisStreamsRequest {
    delivery: Delivery,
    client: redis::Client,
    codec: MemoryPackCodec,
}

impl RedisStreamsRequest {
    /// Returns the received Catga envelope.
    pub const fn envelope(&self) -> &Envelope {
        self.delivery.envelope()
    }

    /// Returns the total Redis Streams delivery attempts observed for this request.
    pub const fn attempts(&self) -> u32 {
        self.delivery.attempts()
    }

    /// Deserializes a typed request payload without copying it first.
    pub fn decode<M: MemoryPackDeserialize>(&self) -> CatgaResult<M> {
        self.codec.decode_value(self.delivery.envelope().payload())
    }

    /// Publishes `response` to the request inbox and then acknowledges the durable ingress entry.
    ///
    /// Redis's Pub/Sub publish count may be zero when a client has timed out and unsubscribed. A
    /// successful publish command still commits the request because the client reply inbox is
    /// intentionally ephemeral; retrying in that case could duplicate completed handler work.
    pub async fn respond(self, response: Envelope) -> CatgaResult<()> {
        let reply_to: Box<str> = self
            .delivery
            .envelope()
            .reply_to()
            .map(Into::into)
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "Redis Streams request is missing reply_to",
                )
            })?;
        let payload = self.codec.encode(&response)?;
        let mut commands = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_error)?;
        let _: usize = commands
            .publish(reply_to.as_ref(), payload)
            .await
            .map_err(map_error)?;
        self.delivery.acknowledge().await
    }

    /// Serializes and sends a typed successful response with propagated correlation metadata.
    pub async fn respond_value<T: MemoryPackSerialize>(self, response: &T) -> CatgaResult<()> {
        let envelope = self
            .codec
            .typed_success(self.delivery.envelope(), response)?;
        self.respond(envelope).await
    }

    /// Sends a structured typed remote failure and then acknowledges the durable ingress entry.
    pub async fn respond_error(self, error: CatgaError) -> CatgaResult<()> {
        let envelope = self.codec.typed_failure(self.delivery.envelope(), error)?;
        self.respond(envelope).await
    }

    /// Requests redelivery without publishing a response.
    pub async fn nack(self) -> CatgaResult<()> {
        self.delivery.nack().await
    }
}

/// Runs every phase of a request/reply operation against one caller-provided timeout budget.
///
/// Keeping the timer outside the supplied future prevents connection, subscription, durable-send,
/// and reply-wait phases from each receiving a fresh full timeout.
///
/// On timeout, the reply inbox subscription is unsubscribed before returning to prevent leaking
/// the subscription until the connection is dropped. The subscription channel is dropped as part of
/// the unsubscription, which allows Redis to discard any late replies.
async fn run_request_with_timeout<T>(
    timeout: Duration,
    reply_to: Box<str>,
    subscription: &mut redis::aio::PubSub,
    operation: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| {
            // Explicitly unsubscribe before returning to release the subscription.
            // Dropping the PubSub connection without unsubscribing would leave the subscription
            // active on the Redis server until the connection closes, leaking resources.
            let _ = subscription.unsubscribe(reply_to.as_ref());
            CatgaError::new(
                ErrorCode::Timeout,
                "Redis Streams request timed out waiting for reply",
            )
        })?
}
