//! Native NATS request/reply over Catga envelopes.

use std::time::Duration;

use catga_codec_memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackRequestClient, MemoryPackSerialize,
};
use catga_core::{
    CatgaError, CatgaResult, DistributedIdGenerator, Envelope, EnvelopeCodec, ErrorCode, Handler,
    Request, RequestTransport,
};
use futures::StreamExt;

/// A native NATS request client using a private inbox for every request.
///
/// Each call delegates reply routing to NATS, so concurrent callers do not
/// contend on a shared reply map or a receiver lock.
#[derive(Clone)]
pub struct NatsRequestClient {
    client: async_nats::Client,
    subject: async_nats::Subject,
    codec: MemoryPackCodec,
}

impl NatsRequestClient {
    /// Connects a request client to one NATS service subject.
    pub async fn connect(server: &str, subject: &str) -> CatgaResult<Self> {
        validate_subject(subject)?;
        let client = async_nats::connect(server).await.map_err(map_error)?;
        Self::from_client(client, subject)
    }

    /// Builds a request client from an application-owned NATS client.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior. The supplied client is used directly and no server is opened.
    pub fn from_client(client: async_nats::Client, subject: &str) -> CatgaResult<Self> {
        Self::initialize(client, subject)
    }

    /// Builds a request client from an application-owned NATS client.
    ///
    /// This is equivalent to [`Self::from_client`] and is available for applications that use
    /// `connect_*` naming for their transport factories.
    pub fn connect_with_client(client: async_nats::Client, subject: &str) -> CatgaResult<Self> {
        Self::initialize(client, subject)
    }

    fn initialize(client: async_nats::Client, subject: &str) -> CatgaResult<Self> {
        validate_subject(subject)?;
        Ok(Self {
            client,
            subject: subject.into(),
            codec: MemoryPackCodec::default(),
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
        validate_subject(subject)?;
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
        MemoryPackRequestClient::new(
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

/// A typed MemoryPack request client backed by native NATS request/reply.
pub type NatsTypedRequestClient = MemoryPackRequestClient<NatsRequestClient>;

/// A subscription accepting native NATS requests for one service subject.
pub struct NatsRequestServer {
    client: async_nats::Client,
    subscription: async_nats::Subscriber,
    codec: MemoryPackCodec,
}

impl NatsRequestServer {
    /// Connects a request server to one NATS service subject.
    pub async fn connect(server: &str, subject: &str) -> CatgaResult<Self> {
        validate_subject(subject)?;
        let client = async_nats::connect(server).await.map_err(map_error)?;
        Self::from_client(client, subject).await
    }

    /// Builds a request server from an application-owned NATS client.
    ///
    /// This preserves the client's configured TLS, authentication, reconnection, and
    /// observability behavior. The configured subject is validated and subscribed before this
    /// method returns.
    pub async fn from_client(client: async_nats::Client, subject: &str) -> CatgaResult<Self> {
        Self::initialize(client, subject).await
    }

    /// Builds a request server from an application-owned NATS client.
    ///
    /// This is equivalent to [`Self::from_client`] and is available for applications that use
    /// `connect_*` naming for their transport factories. The configured subject is validated and
    /// subscribed before the returned server can receive requests.
    pub async fn connect_with_client(
        client: async_nats::Client,
        subject: &str,
    ) -> CatgaResult<Self> {
        Self::initialize(client, subject).await
    }

    async fn initialize(client: async_nats::Client, subject: &str) -> CatgaResult<Self> {
        validate_subject(subject)?;
        let subscription = client
            .subscribe(async_nats::Subject::from(subject))
            .await
            .map_err(map_error)?;
        Ok(Self {
            client,
            subscription,
            codec: MemoryPackCodec::default(),
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

/// One decoded request and its private NATS reply subject.
pub struct NatsRequest {
    client: async_nats::Client,
    reply: async_nats::Subject,
    envelope: Envelope,
    codec: MemoryPackCodec,
}

impl NatsRequest {
    /// Returns the received Catga envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Deserializes the typed request payload without copying it.
    pub fn decode<M: MemoryPackDeserialize>(&self) -> CatgaResult<M> {
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
    pub async fn respond_value<T: MemoryPackSerialize>(self, response: &T) -> CatgaResult<()> {
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

fn validate_subject(subject: &str) -> CatgaResult<()> {
    if subject.trim().is_empty() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "NATS request subject must not be empty or whitespace-only",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::NatsRequestClient;

    use super::validate_subject;

    #[test]
    fn request_subject_must_not_be_blank() {
        assert!(validate_subject("orders.create").is_ok());
        assert!(validate_subject("").is_err());
        assert!(validate_subject(" \t").is_err());
    }

    #[test]
    fn connect_validates_request_subject_before_opening_a_connection() {
        let result =
            futures::executor::block_on(NatsRequestClient::connect("nats://127.0.0.1:1", " "));

        assert!(matches!(
            result,
            Err(error) if error.code() == catga_core::ErrorCode::Validation
        ));
    }
}
