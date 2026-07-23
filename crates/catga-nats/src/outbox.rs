//! JetStream KV-backed owner-CAS outbox.

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{CatgaError, CatgaResult, EnvelopeCodec, ErrorCode, OutboxMessage, OutboxStore};
use futures::TryStreamExt;

/// JetStream KV outbox with revision-CAS owner transitions.
pub struct NatsOutbox {
    store: kv::Store,
    codec: PostcardCodec,
}
impl NatsOutbox {
    /// Connects and provisions a one-history KV bucket for outbox messages.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = match context.get_key_value(bucket.as_ref()).await {
            Ok(store) => store,
            Err(_) => match context
                .create_key_value(kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    ..Default::default()
                })
                .await
            {
                Ok(store) => store,
                Err(_) => context
                    .get_key_value(bucket.as_ref())
                    .await
                    .map_err(map_error)?,
            },
        };
        Ok(Self {
            store,
            codec: PostcardCodec,
        })
    }
    fn key(id: u64) -> String {
        format!("m{id}")
    }
}
#[async_trait]
impl OutboxStore for NatsOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        let key = Self::key(message.id());
        let value = encode(&self.codec, "", &message)?;
        match self.store.create(key, value.into()).await {
            Ok(_) => Ok(()),
            Err(_) => Err(CatgaError::new(
                ErrorCode::Conflict,
                "an outbox message with this identifier already exists",
            )),
        }
    }
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut keys: Vec<_> = self
            .store
            .keys()
            .await
            .map_err(map_error)?
            .try_collect()
            .await
            .map_err(map_error)?;
        keys.sort_unstable();
        let mut claimed = Vec::with_capacity(limit);
        for key in keys {
            if claimed.len() == limit {
                break;
            }
            let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                continue;
            };
            let (current, message) = decode(&self.codec, &entry.value)?;
            if current.is_empty() {
                let next = encode(&self.codec, owner, &message)?;
                if self
                    .store
                    .update(&key, next.into(), entry.revision)
                    .await
                    .is_ok()
                {
                    let mut message = message;
                    message.claim(owner);
                    claimed.push(message)
                }
            }
        }
        Ok(claimed)
    }
    async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let key = Self::key(id);
        let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
            return Ok(());
        };
        let (current, _) = decode(&self.codec, &entry.value)?;
        if current == owner {
            self.store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
                .map_err(map_error)?;
        }
        Ok(())
    }
    async fn release(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let key = Self::key(id);
        let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
            return Ok(());
        };
        let (current, message) = decode(&self.codec, &entry.value)?;
        if current == owner {
            self.store
                .update(
                    &key,
                    encode(&self.codec, "", &message)?.into(),
                    entry.revision,
                )
                .await
                .map_err(map_error)?;
        }
        Ok(())
    }
}
fn encode(codec: &PostcardCodec, owner: &str, message: &OutboxMessage) -> CatgaResult<Vec<u8>> {
    let payload = codec.encode(message.envelope())?;
    let owner = owner.as_bytes();
    let length = u16::try_from(owner.len())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "outbox owner is too long"))?;
    let mut value = Vec::with_capacity(2 + owner.len() + payload.len());
    value.extend_from_slice(&length.to_be_bytes());
    value.extend_from_slice(owner);
    value.extend_from_slice(&payload);
    Ok(value)
}
fn decode(codec: &PostcardCodec, value: &[u8]) -> CatgaResult<(String, OutboxMessage)> {
    if value.len() < 2 {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox record is malformed",
        ));
    }
    let length = usize::from(u16::from_be_bytes(value[..2].try_into().map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "NATS outbox owner is malformed")
    })?));
    let start = 2usize.checked_add(length).ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "NATS outbox owner length overflows")
    })?;
    if start > value.len() {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox owner length is malformed",
        ));
    }
    let owner = std::str::from_utf8(&value[2..start])
        .map_err(|e| CatgaError::new(ErrorCode::Internal, e.to_string()))?
        .to_owned();
    Ok((owner, OutboxMessage::new(codec.decode(&value[start..])?)))
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
