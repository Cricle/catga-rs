//! Redis Pub/Sub request/reply over Catga envelopes.

use std::time::Duration;

use catga_codec_memorypack::{MemoryPackCodec, MemoryPackDeserialize, MemoryPackSerialize};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, Handler, Request, RequestTransport,
};
use futures::StreamExt;
use redis::AsyncCommands;

use crate::transport::map_error;

/// A Redis Pub/Sub request client using one temporary reply subscription per request.
#[derive(Clone)]
pub struct RedisRequestClient {
    client: redis::Client,
    codec: MemoryPackCodec,
}

/// A Redis Pub/Sub request subscription for one destination.
pub struct RedisRequestServer {
    client: redis::Client,
    subscription: redis::aio::PubSub,
    codec: MemoryPackCodec,
}

impl RedisRequestServer {
    /// Connects and subscribes to one request destination.
    pub async fn connect(server: &str, destination: &str) -> CatgaResult<Self> {
        if destination.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis request destination must not be empty",
            ));
        }
        let client = redis::Client::open(server).map_err(map_error)?;
        let mut subscription = client.get_async_pubsub().await.map_err(map_error)?;
        subscription
            .subscribe(destination)
            .await
            .map_err(map_error)?;
        Ok(Self {
            client,
            subscription,
            codec: MemoryPackCodec::default(),
        })
    }

    /// Receives one request and returns its reply handle.
    pub async fn next(&mut self) -> CatgaResult<RedisRequest> {
        let message = self.subscription.on_message().next().await.ok_or_else(|| {
            CatgaError::new(ErrorCode::Transient, "Redis request subscription closed")
        })?;
        Ok(RedisRequest {
            client: self.client.clone(),
            envelope: self.codec.decode(message.get_payload_bytes())?,
            codec: self.codec,
        })
    }

    /// Receives one typed request and sends its handler result to the private reply channel.
    pub async fn handle_next<M, H>(&mut self, handler: &H) -> CatgaResult<()>
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

/// One Redis request with its private reply channel.
pub struct RedisRequest {
    client: redis::Client,
    envelope: Envelope,
    codec: MemoryPackCodec,
}

impl RedisRequest {
    /// Returns the decoded request envelope.
    pub fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Deserializes the typed request payload without copying it first.
    pub fn decode<M: MemoryPackDeserialize>(&self) -> CatgaResult<M> {
        self.codec.decode_value(self.envelope.payload())
    }

    /// Publishes a correlated reply to the request's private channel.
    pub async fn respond(self, response: Envelope) -> CatgaResult<()> {
        let reply_to = self.envelope.reply_to().ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "Redis request is missing reply_to")
        })?;
        let payload = self.codec.encode(&response)?;
        let mut commands = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_error)?;
        let _: usize = commands
            .publish(reply_to, payload)
            .await
            .map_err(map_error)?;
        Ok(())
    }

    /// Serializes and sends a typed successful response with propagated correlation metadata.
    pub async fn respond_value<T: MemoryPackSerialize>(self, response: &T) -> CatgaResult<()> {
        let envelope = self.codec.typed_success(&self.envelope, response)?;
        self.respond(envelope).await
    }

    /// Sends a structured typed failure to the request's private reply channel.
    pub async fn respond_error(self, error: CatgaError) -> CatgaResult<()> {
        let envelope = self.codec.typed_failure(&self.envelope, error)?;
        self.respond(envelope).await
    }
}

impl RedisRequestClient {
    /// Connects a client to Redis.
    pub fn connect(server: &str) -> CatgaResult<Self> {
        Ok(Self {
            client: redis::Client::open(server).map_err(map_error)?,
            codec: MemoryPackCodec::default(),
        })
    }

    /// Sends a request to a Pub/Sub destination.
    pub async fn request_to(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        if destination.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis request destination must not be empty",
            ));
        }
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Redis request timeout must be greater than zero",
            ));
        }
        let reply_to: Box<str> = format!("catga.reply.{}", uuid::Uuid::new_v4()).into_boxed_str();
        let mut subscription = self.client.get_async_pubsub().await.map_err(map_error)?;
        subscription
            .subscribe(reply_to.as_ref())
            .await
            .map_err(map_error)?;
        let request = request.with_reply_to(reply_to);
        let payload = self.codec.encode(&request)?;
        let mut commands = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_error)?;
        let _: usize = commands
            .publish(destination, payload)
            .await
            .map_err(map_error)?;
        let reply = tokio::time::timeout(timeout, subscription.on_message().next())
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Timeout, "Redis request timed out"))?
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Transient, "Redis reply subscription closed")
            })?;
        self.codec.decode(reply.get_payload_bytes())
    }
}

#[async_trait::async_trait]
impl RequestTransport for RedisRequestClient {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        self.request_to(destination, request, timeout).await
    }
}
