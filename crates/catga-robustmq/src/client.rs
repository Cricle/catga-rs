use std::{sync::Arc, time::Duration};

use catga_core::codec::memorypack::{MemoryPackCodec, MemoryPackDeserialize, MemoryPackSerialize};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, Handler, Request, RequestTransport,
};
use robustmq::{MQ9Client, Mailbox, Subscription};
use tokio::sync::mpsc;

use crate::MailboxPriority;

/// Configuration for a single RobustMQ mq9 mailbox.
///
/// ```
/// use catga_robustmq::MailboxConfig;
///
/// let config = MailboxConfig {
///     server: "nats://127.0.0.1:4222".into(),
///     ttl_seconds: 60,
///     public: false,
///     name: "order-replies".into(),
///     description: "private request replies".into(),
/// };
/// assert!(!config.public);
/// ```
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
impl<C> RequestTransport for MailboxClient<C>
where
    C: EnvelopeCodec + 'static,
{
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: std::time::Duration,
    ) -> CatgaResult<Envelope> {
        self.request_to(destination, request, timeout).await
    }
}

/// A RobustMQ mq9 mailbox client using `C` to frame complete Catga envelopes.
///
/// `C` defaults to [`MemoryPackCodec`], preserving Catga's standard bounded MemoryPack wire
/// format. Construct a client with [`Self::connect_with_codec`] when communicating with a peer
/// that uses a different [`EnvelopeCodec`]. Raw [`Self::send`] and [`Self::subscribe`] calls
/// remain format-agnostic because they operate on caller-provided bytes.
#[derive(Clone)]
pub struct MailboxClient<C = MemoryPackCodec> {
    client: Arc<MQ9Client>,
    codec: Arc<C>,
}

/// Owns a private reply subscription and unsubscribes when a request completes or times out.
struct ReplySubscription(Option<Subscription>);

impl ReplySubscription {
    fn new(subscription: Subscription) -> Self {
        Self(Some(subscription))
    }
}

impl Drop for ReplySubscription {
    fn drop(&mut self) {
        if let Some(subscription) = self.0.take() {
            subscription.unsubscribe();
        }
    }
}

impl MailboxClient<MemoryPackCodec> {
    /// Connects to a RobustMQ NATS-compatible endpoint.
    pub async fn connect(server: &str) -> CatgaResult<Self> {
        Self::connect_with_codec(server, MemoryPackCodec::default()).await
    }
}

impl<C> MailboxClient<C>
where
    C: EnvelopeCodec + 'static,
{
    /// Connects to a RobustMQ NATS-compatible endpoint using `codec` for complete envelopes.
    ///
    /// Both request/reply directions, envelope subscriptions, and request-server responses use
    /// the supplied codec. The codec is shared through an [`Arc`] so cloning the client does not
    /// clone codec state or allocate a second codec instance.
    pub async fn connect_with_codec(server: &str, codec: C) -> CatgaResult<Self> {
        MQ9Client::connect(server)
            .await
            .map(|client| Self {
                client: Arc::new(client),
                codec: Arc::new(codec),
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

    /// Sends a complete Catga envelope with the client's configured [`EnvelopeCodec`].
    ///
    /// Unlike [`Self::send`], this preserves Catga envelope metadata, schema version, headers,
    /// and reply routing. The bytes are compatible with [`Self::subscribe_envelopes`],
    /// [`Self::request_to`], and [`MailboxRequestServer::subscribe`] created from a client using
    /// the same codec type and configuration.
    pub async fn send_envelope(
        &self,
        mailbox_id: &str,
        envelope: &Envelope,
        priority: MailboxPriority,
    ) -> CatgaResult<()> {
        let payload = encode_envelope(self.codec.as_ref(), envelope)?;
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

    /// Subscribes to complete Catga envelopes with the client's configured codec.
    ///
    /// Decode failures are surfaced to `callback`; malformed remote data never panics the
    /// subscription task.
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
        let codec = Arc::clone(&self.codec);
        self.subscribe(
            mailbox_id,
            move |message| callback(decode_envelope(codec.as_ref(), &message.payload)),
            priority,
            queue_group,
        )
        .await
    }

    /// Sends a request with the client's codec and awaits one reply through a private mailbox.
    ///
    /// The reply is decoded with the same codec instance, ensuring a custom wire format is used
    /// consistently in both request directions.
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
        tokio::time::timeout(timeout, async {
            let reply = self
                .client
                .create(60, false, "", "")
                .await
                .map_err(map_error)?;
            let (sender, mut receiver) = mpsc::channel(1);
            let codec = Arc::clone(&self.codec);
            let _subscription = ReplySubscription::new(
                self.client
                    .subscribe(
                        &reply.mail_id,
                        move |message| {
                            let decoded = decode_envelope(codec.as_ref(), &message.payload);
                            let sender = sender.clone();
                            async move {
                                let _ = sender.send(decoded).await;
                            }
                        },
                        None,
                        "",
                    )
                    .await
                    .map_err(map_error)?,
            );
            let priority = MailboxPriority::from_envelope(&request).as_sdk();
            let payload =
                encode_envelope(self.codec.as_ref(), &request.with_reply_to(reply.mail_id))?;
            self.client
                .send(mailbox_id, &payload, priority)
                .await
                .map_err(map_error)?;
            receiver.recv().await.ok_or_else(|| {
                CatgaError::new(ErrorCode::Transient, "RobustMQ reply subscription closed")
            })?
        })
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "RobustMQ request timed out"))?
    }
}

/// A RobustMQ request server with bounded inbound backpressure.
///
/// `C` defaults to [`MemoryPackCodec`]. Use a [`MailboxClient`] constructed with
/// [`MailboxClient::connect_with_codec`] to select another envelope format.
pub struct MailboxRequestServer<C = MemoryPackCodec> {
    subscription: Option<Subscription>,
    requests: mpsc::Receiver<CatgaResult<MailboxRequest<C>>>,
}

impl<C> MailboxRequestServer<C>
where
    C: EnvelopeCodec + 'static,
{
    /// Subscribes to one mailbox and buffers at most `capacity` decoded requests.
    ///
    /// Requests are decoded with the codec configured on `client`. Each returned
    /// [`MailboxRequest`] retains that codec so its envelope response uses the matching wire
    /// format.
    pub async fn subscribe(
        client: MailboxClient<C>,
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
        let codec = Arc::clone(&client.codec);
        let subscription = client
            .client
            .subscribe(
                mailbox_id,
                move |message| {
                    let sender = sender.clone();
                    let client = Arc::clone(&request_client);
                    let codec = Arc::clone(&codec);
                    async move {
                        let request =
                            decode_envelope(codec.as_ref(), &message.payload).map(|envelope| {
                                MailboxRequest {
                                    client,
                                    codec,
                                    envelope,
                                }
                            });
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
    ///
    /// The returned request retains the envelope codec configured on the subscribing client.
    pub async fn next(&mut self) -> CatgaResult<MailboxRequest<C>> {
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

impl<C> Drop for MailboxRequestServer<C> {
    fn drop(&mut self) {
        if let Some(subscription) = self.subscription.take() {
            subscription.unsubscribe();
        }
    }
}

/// One received RobustMQ request, its private reply route, and its envelope codec.
///
/// The type parameter defaults to [`MemoryPackCodec`]. Custom-codec request servers produce a
/// matching `MailboxRequest<C>` so [`Self::respond`] encodes replies with the same codec that
/// decoded the request.
pub struct MailboxRequest<C = MemoryPackCodec> {
    client: Arc<MQ9Client>,
    codec: Arc<C>,
    envelope: Envelope,
}

impl<C> MailboxRequest<C>
where
    C: EnvelopeCodec + 'static,
{
    /// Returns the decoded request envelope.
    pub const fn envelope(&self) -> &Envelope {
        &self.envelope
    }

    /// Deserializes the typed request payload without copying it first.
    pub fn decode<M: MemoryPackDeserialize>(&self) -> CatgaResult<M> {
        MemoryPackCodec::default().decode_value(self.envelope.payload())
    }

    /// Sends one response at its envelope priority to the private reply mailbox.
    ///
    /// The complete envelope is encoded with the codec retained from the request server; this
    /// avoids accidentally replying in MemoryPack to a custom-codec peer.
    pub async fn respond(self, response: Envelope) -> CatgaResult<()> {
        let reply_to = self.envelope.reply_to().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "RobustMQ request is missing reply_to",
            )
        })?;
        let priority = MailboxPriority::from_envelope(&response).as_sdk();
        let payload = encode_envelope(self.codec.as_ref(), &response)?;
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

/// Encodes an envelope at the RobustMQ boundary without making a transport choice in Core.
fn encode_envelope<C: EnvelopeCodec + ?Sized>(
    codec: &C,
    envelope: &Envelope,
) -> CatgaResult<Vec<u8>> {
    codec.encode(envelope)
}

/// Decodes an envelope at the RobustMQ boundary without making a transport choice in Core.
fn decode_envelope<C: EnvelopeCodec + ?Sized>(codec: &C, bytes: &[u8]) -> CatgaResult<Envelope> {
    codec.decode(bytes)
}

