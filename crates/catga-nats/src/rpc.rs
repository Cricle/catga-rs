//! Native NATS request/reply over Catga envelopes.

use std::time::Duration;

use catga_codec_postcard::{PostcardCodec, PostcardRequestClient};
use catga_core::{
    CatgaError, CatgaResult, DistributedIdGenerator, Envelope, EnvelopeCodec, ErrorCode, Handler,
    Request, RequestTransport,
};
use futures::StreamExt;
use serde::{Serialize, de::DeserializeOwned};

/// A native NATS request client using a private inbox for every request.
///
/// Each call delegates reply routing to NATS, so concurrent callers do not
/// contend on a shared reply map or a receiver lock.
#[derive(Clone)]
pub struct NatsRequestClient {
    client: async_nats::Client,
    subject: async_nats::Subject,
    codec: PostcardCodec,
}

impl NatsRequestClient {
    /// Connects a request client to one NATS service subject.
    pub async fn connect(server: &str, subject: &str) -> CatgaResult<Self> {
        if subject.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "NATS request subject must not be empty",
            ));
        }
        let client = async_nats::connect(server).await.map_err(map_error)?;
        Ok(Self {
            client,
            subject: subject.into(),
            codec: PostcardCodec,
        })
    }

    /// Sends an envelope and awaits exactly one reply through a NATS inbox.
    pub async fn request(&self, request: Envelope, timeout: Duration) -> CatgaResult<Envelope> {
        self.request_to(&self.subject, request, timeout).await
    }

    /// Sends an envelope to `subject` and awaits exactly one native NATS reply.
    pub async fn request_to(
        &self,
        subject: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        if subject.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "NATS request subject must not be empty",
            ));
        }
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "NATS request timeout must be greater than zero",
            ));
        }
        let payload = self.codec.encode(&request)?;
        let reply = tokio::time::timeout(
            timeout,
            self.client.request(subject.to_owned(), payload.into()),
        )
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "NATS request timed out"))?
        .map_err(map_error)?;
        self.codec.decode(&reply.payload)
    }

    /// Adds typed message serialization backed by the supplied ID generator.
    pub fn typed(
        self,
        id_generator: std::sync::Arc<dyn DistributedIdGenerator>,
    ) -> CatgaResult<NatsTypedRequestClient> {
        let destination = self.subject.to_string();
        PostcardRequestClient::new(
            std::sync::Arc::new(self),
            destination,
            Duration::from_secs(30),
            id_generator,
        )
    }
}

#[async_trait::async_trait]
impl RequestTransport for NatsRequestClient {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        self.request_to(destination, request, timeout).await
    }
}

/// A typed Postcard request client backed by native NATS request/reply.
pub type NatsTypedRequestClient = PostcardRequestClient<NatsRequestClient>;

/// A subscription accepting native NATS requests for one service subject.
pub struct NatsRequestServer {
    client: async_nats::Client,
    subscription: async_nats::Subscriber,
    codec: PostcardCodec,
}

impl NatsRequestServer {
    /// Connects a request server to one NATS service subject.
    pub async fn connect(server: &str, subject: &str) -> CatgaResult<Self> {
        if subject.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "NATS request subject must not be empty",
            ));
        }
        let client = async_nats::connect(server).await.map_err(map_error)?;
        let subscription = client
            .subscribe(async_nats::Subject::from(subject))
            .await
            .map_err(map_error)?;
        Ok(Self {
            client,
            subscription,
            codec: PostcardCodec,
        })
    }

    /// Receives the next request and returns its one-shot response handle.
    pub async fn next(&mut self) -> CatgaResult<NatsRequest> {
        let message = self.subscription.next().await.ok_or_else(|| {
            CatgaError::new(ErrorCode::Transient, "NATS request subscription closed")
        })?;
        let reply = message.reply.ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "NATS request does not include a reply subject",
            )
        })?;
        let envelope = self.codec.decode(&message.payload)?;
        Ok(NatsRequest {
            client: self.client.clone(),
            reply,
            envelope,
            codec: self.codec,
        })
    }

    /// Receives one typed request and routes its result through the private reply inbox.
    pub async fn handle_next<M, H>(&mut self, handler: &H) -> CatgaResult<()>
    where
        M: Request + DeserializeOwned,
        M::Response: Serialize,
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

/// One decoded request and its private NATS reply subject.
pub struct NatsRequest {
    client: async_nats::Client,
    reply: async_nats::Subject,
    envelope: Envelope,
    codec: PostcardCodec,
}

impl NatsRequest {
    /// Returns the received Catga envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Deserializes the typed request payload without copying it.
    pub fn decode<M: DeserializeOwned>(&self) -> CatgaResult<M> {
        self.codec.decode_value(self.envelope.payload())
    }

    /// Sends the sole response to the originating request inbox.
    pub async fn respond(self, response: Envelope) -> CatgaResult<()> {
        let payload = self.codec.encode(&response)?;
        self.client
            .publish(self.reply, payload.into())
            .await
            .map_err(map_error)
    }

    /// Serializes and sends a typed successful response with propagated correlation metadata.
    pub async fn respond_value<T: Serialize>(self, response: &T) -> CatgaResult<()> {
        let envelope = self.codec.typed_success(&self.envelope, response)?;
        self.respond(envelope).await
    }

    /// Sends a structured remote failure to the originating request inbox.
    pub async fn respond_error(self, error: CatgaError) -> CatgaResult<()> {
        let envelope = self.codec.typed_failure(&self.envelope, error)?;
        self.respond(envelope).await
    }
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
