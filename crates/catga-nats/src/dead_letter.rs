//! JetStream-backed ordered dead-letter queue.

use async_nats::jetstream::{
    self,
    stream::{self, Stream},
};
use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, EnvelopeCodec,
    ErrorCode,
};

const DIAGNOSTICS_MAGIC: &[u8; 4] = b"DLQ2";

/// JetStream append-only dead-letter store.
pub struct NatsDeadLetters {
    context: jetstream::Context,
    stream: Stream,
    subject: Box<str>,
    codec: MemoryPackCodec,
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
            codec: MemoryPackCodec::default(),
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
fn encode(codec: &MemoryPackCodec, letter: &DeadLetter) -> CatgaResult<Vec<u8>> {
    let envelope = codec.encode(letter.envelope())?;
    let reason = letter.reason().as_bytes();
    let diagnostics = letter.diagnostics();
    let error_code = diagnostics.error_code().as_stable_str().as_bytes();
    let stage = diagnostics.stage().as_bytes();
    let reason_len = u32::try_from(reason.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "NATS dead-letter reason exceeds wire limits",
        )
    })?;
    let envelope_len = u32::try_from(envelope.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "NATS dead-letter envelope exceeds wire limits",
        )
    })?;
    let error_code_len = u8::try_from(error_code.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "NATS dead-letter error code exceeds wire limits",
        )
    })?;
    let stage_len = u8::try_from(stage.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "NATS dead-letter stage exceeds wire limits",
        )
    })?;
    let mut value = Vec::with_capacity(
        26usize
            .saturating_add(reason.len())
            .saturating_add(envelope.len())
            .saturating_add(error_code.len())
            .saturating_add(stage.len()),
    );
    value.extend_from_slice(&letter.attempts().to_be_bytes());
    value.extend_from_slice(&reason_len.to_be_bytes());
    value.extend_from_slice(&envelope_len.to_be_bytes());
    value.extend_from_slice(reason);
    value.extend_from_slice(&envelope);
    value.extend_from_slice(DIAGNOSTICS_MAGIC);
    value.extend_from_slice(&diagnostics.failed_at_unix_ms().to_be_bytes());
    value.push(error_code_len);
    value.push(stage_len);
    value.extend_from_slice(error_code);
    value.extend_from_slice(stage);
    Ok(value)
}
fn decode(codec: &MemoryPackCodec, value: &[u8]) -> CatgaResult<DeadLetter> {
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
    .map_err(|_| CatgaError::new(ErrorCode::Internal, "NATS dead-letter reason is too large"))?;
    let envelope_len = usize::try_from(u32::from_be_bytes(value[8..12].try_into().map_err(
        |_| {
            CatgaError::new(
                ErrorCode::Internal,
                "NATS dead-letter envelope is malformed",
            )
        },
    )?))
    .map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter envelope is too large",
        )
    })?;
    let end = 12usize
        .checked_add(reason_len)
        .and_then(|n| n.checked_add(envelope_len))
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "NATS dead-letter lengths overflow"))?;
    if end > value.len() {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter lengths are malformed",
        ));
    }
    let reason = std::str::from_utf8(&value[12..12 + reason_len])
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "NATS dead-letter reason is invalid"))?;
    if end == value.len() {
        return Ok(DeadLetter::new(
            codec.decode(&value[12 + reason_len..end])?,
            reason,
            attempts,
        ));
    }
    let diagnostics = &value[end..];
    if diagnostics.len() < 14 || &diagnostics[..4] != DIAGNOSTICS_MAGIC {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter diagnostics are malformed",
        ));
    }
    let failed_at_unix_ms = u64::from_be_bytes(diagnostics[4..12].try_into().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter failure time is malformed",
        )
    })?);
    let error_code_len = usize::from(diagnostics[12]);
    let stage_len = usize::from(diagnostics[13]);
    let diagnostics_end = 14usize
        .checked_add(error_code_len)
        .and_then(|length| length.checked_add(stage_len))
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "NATS dead-letter diagnostics overflow")
        })?;
    if diagnostics_end != diagnostics.len() {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter diagnostics lengths are malformed",
        ));
    }
    let error_code = std::str::from_utf8(&diagnostics[14..14 + error_code_len]).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter error code is invalid",
        )
    })?;
    let error_code = ErrorCode::from_stable_str(error_code).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS dead-letter error code is unknown",
        )
    })?;
    let stage = std::str::from_utf8(&diagnostics[14 + error_code_len..])
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "NATS dead-letter stage is invalid"))?;
    let diagnostics =
        DeadLetterDiagnostics::try_at(failed_at_unix_ms, error_code, stage).map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "NATS dead-letter diagnostics are invalid",
            )
        })?;
    DeadLetter::try_with_diagnostics(
        codec.decode(&value[12 + reason_len..end])?,
        reason,
        attempts,
        diagnostics,
    )
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
