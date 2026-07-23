use std::sync::Arc;

use async_nats::jetstream::{self, consumer::pull, stream};
use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, EnvelopeCodec, ErrorCode, MessageTransport,
};
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::{NatsConfig, acknowledgement::NatsAcknowledger};

/// JetStream-backed at-least-once transport with explicit acknowledgement.
pub struct NatsTransport {
    context: jetstream::Context,
    subject: Box<str>,
    codec: PostcardCodec,
    messages: Arc<Mutex<pull::Stream>>,
}

impl NatsTransport {
    /// Connects and idempotently provisions the configured stream and durable consumer.
    pub async fn connect(config: NatsConfig) -> CatgaResult<Self> {
        let client = async_nats::connect(config.server.as_ref())
            .await
            .map_err(map_error)?;
        let context = jetstream::new(client);
        let stream = context
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
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_error)?;
        let messages = consumer.messages().await.map_err(map_error)?;
        Ok(Self {
            context,
            subject: config.subject,
            codec: PostcardCodec,
            messages: Arc::new(Mutex::new(messages)),
        })
    }
}

#[async_trait]
impl MessageTransport for NatsTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let payload = self.codec.encode(&envelope)?;
        self.context
            .publish(self.subject.to_string(), payload.into())
            .await
            .map_err(map_error)?
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let message = self
            .messages
            .lock()
            .await
            .next()
            .await
            .ok_or_else(|| CatgaError::new(ErrorCode::Transient, "JetStream consumer closed"))
            .and_then(|message| message.map_err(map_error))?;
        let envelope = self.codec.decode(&message.payload)?;
        Ok(Delivery::with_acknowledger(
            envelope,
            Box::new(NatsAcknowledger(message)),
        ))
    }
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
