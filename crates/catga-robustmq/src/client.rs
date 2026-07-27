use std::{sync::Arc, time::Duration};

use catga_codec_memorypack::{MemoryPackCodec, MemoryPackDeserialize, MemoryPackSerialize};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, Handler, Request, RequestTransport,
};
use robustmq::{MQ9Client, Mailbox, Subscription};
use tokio::sync::mpsc;

use crate::MailboxPriority;

/// Configuration for a single RobustMQ mq9 mailbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MailboxConfig {
    /// Server URL accepted by the RobustMQ SDK.
    pub server: Box<str>,
    /// Mailbox lifetime in seconds.
    pub ttl_seconds: u64,
    /// Whether the mailbox is publicly discoverable.
    pub public: bool,
    /// Optional discoverable mailbox name.
    pub name: Box<str>,
    /// Optional mailbox description.
    pub description: Box<str>,
}

#[async_trait::async_trait]
impl RequestTransport for MailboxClient {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: std::time::Duration,
    ) -> CatgaResult<Envelope> {
        self.request_to(destination, request, timeout).await
    }
}

/// A typed wrapper around the RobustMQ mq9 mailbox client.
#[derive(Clone)]
pub struct MailboxClient {
    client: Arc<MQ9Client>,
}

impl MailboxClient {
    /// Connects to a RobustMQ NATS-compatible endpoint.
    pub async fn connect(server: &str) -> CatgaResult<Self> {
        MQ9Client::connect(server)
            .await
            .map(|client| Self {
                client: Arc::new(client),
            })
            .map_err(map_error)
    }

    /// Creates a mailbox using the configured retention and visibility.
    pub async fn create(&self, config: &MailboxConfig) -> CatgaResult<Mailbox> {
        self.client
            .create(
                config.ttl_seconds,
                config.public,
                &config.name,
                &config.description,
            )
            .await
            .map_err(map_error)
    }

    /// Sends an envelope payload to a mailbox with explicit priority.
    pub async fn send(
        &self,
        mailbox_id: &str,
        envelope: &Envelope,
        priority: MailboxPriority,
    ) -> CatgaResult<()> {
        self.client
            .send(mailbox_id, envelope.payload(), priority.as_sdk())
            .await
            .map_err(map_error)
    }

    /// Sends a complete Catga envelope without losing its metadata or schema version.
    pub async fn send_envelope(
        &self,
        mailbox_id: &str,
        envelope: &Envelope,
        priority: MailboxPriority,
    ) -> CatgaResult<()> {
        let payload = MemoryPackCodec::default().encode(envelope)?;
        self.client
            .send(mailbox_id, &payload, priority.as_sdk())
            .await
            .map_err(map_error)
    }

    /// Subscribes to push delivery for a mailbox.
    pub async fn subscribe<F, Fut>(
        &self,
        mailbox_id: &str,
        callback: F,
        priority: Option<MailboxPriority>,
        queue_group: &str,
    ) -> CatgaResult<Subscription>
    where
        F: Fn(robustmq::Message) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.client
            .subscribe(
                mailbox_id,
                callback,
                priority.map(MailboxPriority::as_sdk),
                queue_group,
            )
            .await
            .map_err(map_error)
    }

    /// Subscribes to complete Catga envelopes, surfacing decode failures to the callback.
    pub async fn subscribe_envelopes<F, Fut>(
        &self,
        mailbox_id: &str,
        callback: F,
        priority: Option<MailboxPriority>,
        queue_group: &str,
    ) -> CatgaResult<Subscription>
    where
        F: Fn(CatgaResult<Envelope>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let codec = MemoryPackCodec::default();
        self.subscribe(
            mailbox_id,
            move |message| callback(codec.decode(&message.payload)),
            priority,
            queue_group,
        )
        .await
    }

    /// Sends a request at its envelope priority and awaits one reply through a private mailbox.
    pub async fn request_to(
        &self,
        mailbox_id: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        if mailbox_id.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request mailbox must not be empty",
            ));
        }
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request timeout must be greater than zero",
            ));
        }
        let reply = self
            .client
            .create(60, false, "", "")
            .await
            .map_err(map_error)?;
        let (sender, mut receiver) = mpsc::channel(1);
        let codec = MemoryPackCodec::default();
        let subscription = self
            .client
            .subscribe(
                &reply.mail_id,
                move |message| {
                    let decoded = codec.decode(&message.payload);
                    let sender = sender.clone();
                    async move {
                        let _ = sender.send(decoded).await;
                    }
                },
                None,
                "",
            )
            .await
            .map_err(map_error)?;
        let priority = MailboxPriority::from_envelope(&request).as_sdk();
        let payload = MemoryPackCodec::default().encode(&request.with_reply_to(reply.mail_id))?;
        let result = async {
            self.client
                .send(mailbox_id, &payload, priority)
                .await
                .map_err(map_error)?;
            tokio::time::timeout(timeout, receiver.recv())
                .await
                .map_err(|_| CatgaError::new(ErrorCode::Timeout, "RobustMQ request timed out"))?
                .ok_or_else(|| {
                    CatgaError::new(ErrorCode::Transient, "RobustMQ reply subscription closed")
                })?
        }
        .await;
        subscription.unsubscribe();
        result
    }
}

/// A RobustMQ request server with bounded inbound backpressure.
pub struct MailboxRequestServer {
    subscription: Option<Subscription>,
    requests: mpsc::Receiver<CatgaResult<MailboxRequest>>,
}

impl MailboxRequestServer {
    /// Subscribes to one mailbox and buffers at most `capacity` decoded requests.
    pub async fn subscribe(
        client: MailboxClient,
        mailbox_id: &str,
        capacity: usize,
    ) -> CatgaResult<Self> {
        if mailbox_id.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request mailbox must not be empty",
            ));
        }
        if capacity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request server capacity must be greater than zero",
            ));
        }
        let (sender, requests) = mpsc::channel(capacity);
        let request_client = Arc::clone(&client.client);
        let subscription = client
            .client
            .subscribe(
                mailbox_id,
                move |message| {
                    let sender = sender.clone();
                    let client = Arc::clone(&request_client);
                    async move {
                        let request = MemoryPackCodec::default()
                            .decode(&message.payload)
                            .map(|envelope| MailboxRequest { client, envelope });
                        let _ = sender.send(request).await;
                    }
                },
                None,
                "",
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            subscription: Some(subscription),
            requests,
        })
    }

    /// Receives the next request or reports that its mailbox subscription closed.
    pub async fn next(&mut self) -> CatgaResult<MailboxRequest> {
        self.requests.recv().await.ok_or_else(|| {
            CatgaError::new(ErrorCode::Transient, "RobustMQ request subscription closed")
        })?
    }

    /// Receives one typed request and sends its handler result to the private reply mailbox.
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

impl Drop for MailboxRequestServer {
    fn drop(&mut self) {
        if let Some(subscription) = self.subscription.take() {
            subscription.unsubscribe();
        }
    }
}

/// One received RobustMQ request and its private reply route.
pub struct MailboxRequest {
    client: Arc<MQ9Client>,
    envelope: Envelope,
}

impl MailboxRequest {
    /// Returns the decoded request envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Deserializes the typed request payload without copying it first.
    pub fn decode<M: MemoryPackDeserialize>(&self) -> CatgaResult<M> {
        MemoryPackCodec::default().decode_value(self.envelope.payload())
    }

    /// Sends one response at its envelope priority to the request's private reply mailbox.
    pub async fn respond(self, response: Envelope) -> CatgaResult<()> {
        let reply_to = self.envelope.reply_to().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request is missing reply_to",
            )
        })?;
        let priority = MailboxPriority::from_envelope(&response).as_sdk();
        let payload = MemoryPackCodec::default().encode(&response)?;
        self.client
            .send(reply_to, &payload, priority)
            .await
            .map_err(map_error)
    }

    /// Serializes and sends a typed response with propagated correlation and priority metadata.
    pub async fn respond_value<T: MemoryPackSerialize>(self, response: &T) -> CatgaResult<()> {
        let envelope = MemoryPackCodec::default().typed_success(&self.envelope, response)?;
        self.respond(envelope).await
    }

    /// Sends a structured typed failure to the request's private reply mailbox.
    pub async fn respond_error(self, error: CatgaError) -> CatgaResult<()> {
        let envelope = MemoryPackCodec::default().typed_failure(&self.envelope, error)?;
        self.respond(envelope).await
    }
}

fn map_error(error: robustmq::MQ9Error) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
