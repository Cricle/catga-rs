//! JetStream-backed ordered dead-letter queue.

use async_nats::jetstream::{
    self,
    stream::{self, Stream},
};
use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{CatgaError, CatgaResult, DeadLetter, DeadLetterStore, EnvelopeCodec, ErrorCode};

/// JetStream append-only dead-letter store.
pub struct NatsDeadLetters {
    context: jetstream::Context,
    stream: Stream,
    subject: Box<str>,
    codec: PostcardCodec,
}
impl NatsDeadLetters {
    /// Connects and provisions an append-only dead-letter stream.
    pub async fn connect(
        server: &str,
        stream_name: impl Into<Box<str>>,
        subject: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let stream_name = stream_name.into();
        let subject = subject.into();
        let stream = context
            .get_or_create_stream(stream::Config {
                name: stream_name.to_string(),
                subjects: vec![format!("{subject}.>")],
                ..Default::default()
            })
            .await
            .map_err(map_error)?;
        Ok(Self {
            context,
            stream,
            subject,
            codec: PostcardCodec,
        })
    }
}
#[async_trait]
impl DeadLetterStore for NatsDeadLetters {
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()> {
        let payload = encode(&self.codec, &letter)?;
        self.context
            .publish(
                format!("{}.{}", self.subject, letter.envelope().id()),
                payload.into(),
            )
            .await
            .map_err(map_error)?
            .await
            .map_err(map_error)?;
        Ok(())
    }
    async fn list(&self, limit: usize) -> CatgaResult<Vec<DeadLetter>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut info = self.stream.clone();
        let state = info.info().await.map_err(map_error)?.state;
        let mut letters = Vec::with_capacity(limit);
        if state.messages == 0 {
            return Ok(letters);
        }
        for sequence in state.first_sequence..=state.last_sequence {
            let raw = self
                .stream
                .get_raw_message(sequence)
                .await
                .map_err(map_error)?;
            if raw
                .subject
                .as_str()
                .starts_with(&format!("{}.", self.subject))
            {
                let message: async_nats::Message = raw.try_into().map_err(map_error)?;
                letters.push(decode(&self.codec, &message.payload)?);
                if letters.len() == limit {
                    break;
                }
            }
        }
        Ok(letters)
    }
}
fn encode(codec: &PostcardCodec, letter: &DeadLetter) -> CatgaResult<Vec<u8>> {
    let envelope = codec.encode(letter.envelope())?;
    let reason = letter.reason().as_bytes();
    let mut value = Vec::with_capacity(
        12usize
            .saturating_add(reason.len())
            .saturating_add(envelope.len()),
    );
    value.extend_from_slice(&letter.attempts().to_be_bytes());
    value.extend_from_slice(
        &u32::try_from(reason.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    value.extend_from_slice(
        &u32::try_from(envelope.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    value.extend_from_slice(reason);
    value.extend_from_slice(&envelope);
    Ok(value)
}
fn decode(codec: &PostcardCodec, value: &[u8]) -> CatgaResult<DeadLetter> {
    if value.len() < 12 {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter record is malformed",
        ));
    }
    let attempts = u32::from_be_bytes(value[..4].try_into().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter attempts are malformed",
        )
    })?);
    let reason_len = usize::try_from(u32::from_be_bytes(value[4..8].try_into().map_err(
        |_| CatgaError::new(ErrorCode::Internal, "NATS dead-letter reason is malformed"),
    )?))
    .unwrap_or(usize::MAX);
    let envelope_len = usize::try_from(u32::from_be_bytes(value[8..12].try_into().map_err(
        |_| {
            CatgaError::new(
                ErrorCode::Internal,
                "NATS dead-letter envelope is malformed",
            )
        },
    )?))
    .unwrap_or(usize::MAX);
    let end = 12usize
        .checked_add(reason_len)
        .and_then(|n| n.checked_add(envelope_len))
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "NATS dead-letter lengths overflow"))?;
    if end != value.len() {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter lengths are malformed",
        ));
    }
    let reason = std::str::from_utf8(&value[12..12 + reason_len])
        .map_err(|e| CatgaError::new(ErrorCode::Internal, e.to_string()))?;
    Ok(DeadLetter::new(
        codec.decode(&value[12 + reason_len..])?,
        reason,
        attempts,
    ))
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
