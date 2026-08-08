//! JetStream KV-backed owner-CAS outbox.

use std::{
    collections::BinaryHeap,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_OUTBOX_CLAIM_LEASE, DEFAULT_OUTBOX_MAX_RETRIES, EnvelopeCodec,
    ErrorCode, OutboxMessage, OutboxState, OutboxStore, outbox_claim_expires_at, telemetry,
    validate_outbox_claim_limit, validate_outbox_message_id, validate_retention_cleanup_limit,
};
use futures::TryStreamExt;
use uuid::Uuid;

/// JetStream KV outbox with revision-CAS owner transitions.
pub struct NatsOutbox {
    store: kv::Store,
    codec: MemoryPackCodec,
}
impl NatsOutbox {
    /// Connects and provisions a one-history KV bucket for outbox messages.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = crate::kv::open_or_create(&context, bucket.as_ref())
            .await
            .map_err(map_error)?;
        Ok(Self {
            store,
            codec: MemoryPackCodec::default(),
        })
    }
    fn key(id: u64) -> String {
        format!("m{id:020}")
    }

    async fn entry_for_id(&self, id: u64) -> CatgaResult<Option<(String, kv::Entry)>> {
        let modern_key = Self::key(id);
        if let Some(entry) = self.store.entry(&modern_key).await.map_err(map_error)?
            && !matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            )
        {
            return Ok(Some((modern_key, entry)));
        }

        let mut keys = self.store.keys().await.map_err(map_error)?;
        while let Some(key) = keys.try_next().await.map_err(map_error)? {
            if key == modern_key {
                continue;
            }
            let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                continue;
            }
            if decode(&self.codec, &entry.value)?.message.id() == id {
                return Ok(Some((key, entry)));
            }
        }
        Ok(None)
    }
}
#[async_trait]
impl OutboxStore for NatsOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "outbox", "enqueue", async {
            validate_outbox_message_id(message.id())?;
            if self.entry_for_id(message.id()).await?.is_some() {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "an outbox message with this identifier already exists",
                ));
            }
            let key = Self::key(message.id());
            let value = encode(&self.codec, StoredState::Pending, &message)?;
            match self.store.create(key, value.into()).await {
                Ok(_) => Ok(()),
                Err(_) => Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "an outbox message with this identifier already exists",
                )),
            }
        })
        .await
    }
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        self.claim_for(owner, limit, DEFAULT_OUTBOX_CLAIM_LEASE)
            .await
    }

    async fn claim_for(
        &self,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> CatgaResult<Vec<OutboxMessage>> {
        telemetry::record_persistence("nats", "outbox", "claim", async {
            validate_outbox_claim_limit(limit)?;
            let expires_at = outbox_claim_expires_at(lease)?;
            if limit == 0 {
                return Ok(Vec::new());
            }
            let now = SystemTime::now();
            let now_unix_ms = current_unix_ms()?;
            // JetStream KV has no ordered range query. Stream the server-side key listing instead
            // of materializing it, retaining only the earliest `limit` eligible keys locally.
            let mut keys = self.store.keys().await.map_err(map_error)?;
            let mut candidates = BinaryHeap::with_capacity(limit);
            while let Some(key) = keys.try_next().await.map_err(map_error)? {
                let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                    continue;
                };
                let record = decode(&self.codec, &entry.value)?;
                if record.message.is_claimable_at(now_unix_ms)
                    && record.message.retry_count() < record.message.max_retries()
                    && record.message.is_due_at(now)
                {
                    if candidates.len() < limit {
                        candidates.push(key);
                    } else if candidates.peek().is_some_and(|largest| key < *largest) {
                        candidates.pop();
                        candidates.push(key);
                    }
                }
            }
            let candidates = candidates.into_sorted_vec();
            let mut claimed = Vec::with_capacity(candidates.len());
            for key in candidates {
                let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                    continue;
                };
                let record = decode(&self.codec, &entry.value)?;
                if !record.message.is_claimable_at(now_unix_ms)
                    || record.message.retry_count() >= record.message.max_retries()
                    || !record.message.is_due_at(now)
                {
                    continue;
                }
                let mut message = record.message;
                message.claim_until_with_token(owner, Uuid::new_v4().to_string(), expires_at);
                let next = encode(&self.codec, StoredState::Claimed, &message)?;
                match self.store.update(&key, next.into(), entry.revision).await {
                    Ok(_) => claimed.push(message),
                    Err(error) if is_revision_conflict(&error) => {}
                    Err(error) => return Err(map_error(error)),
                }
            }
            Ok(claimed)
        })
        .await
    }
    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "outbox", "ack", async {
            let Some((key, entry)) = self.entry_for_id(id).await? else {
                return Ok(());
            };
            let mut record = decode(&self.codec, &entry.value)?;
            if record.owner.as_ref() == owner
                && record.message.claim_token() == Some(claim_token)
                && record.state == StoredState::Claimed
            {
                record.message.mark_published(current_unix_ms()?);
                self.store
                    .update(
                        &key,
                        encode(&self.codec, StoredState::Published, &record.message)?.into(),
                        entry.revision,
                    )
                    .await
                    .map_err(map_error)?;
            }
            Ok(())
        })
        .await
    }
    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "outbox", "release", async {
            let Some((key, entry)) = self.entry_for_id(id).await? else {
                return Ok(());
            };
            let mut record = decode(&self.codec, &entry.value)?;
            if record.owner.as_ref() == owner
                && record.message.claim_token() == Some(claim_token)
                && record.state == StoredState::Claimed
            {
                record.message.release();
                self.store
                    .update(
                        &key,
                        encode(&self.codec, StoredState::Pending, &record.message)?.into(),
                        entry.revision,
                    )
                    .await
                    .map_err(map_error)?;
            }
            Ok(())
        })
        .await
    }

    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "outbox", "failure", async {
            let Some((key, entry)) = self.entry_for_id(id).await? else {
                return Ok(());
            };
            let record = decode(&self.codec, &entry.value)?;
            if record.owner.as_ref() != owner
                || record.message.claim_token() != Some(claim_token)
                || record.state != StoredState::Claimed
            {
                return Ok(());
            }
            let mut message = record.message;
            message.record_failure(reason);
            let state = StoredState::from_message(&message);
            self.store
                .update(
                    &key,
                    encode(&self.codec, state, &message)?.into(),
                    entry.revision,
                )
                .await
                .map_err(map_error)?;
            Ok(())
        })
        .await
    }

    async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "outbox", "cancel", async {
            let Some((key, entry)) = self.entry_for_id(id).await? else {
                return Ok(false);
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(false);
            }
            let record = decode(&self.codec, &entry.value)?;
            if !record.owner.is_empty() || record.state != StoredState::Pending {
                return Ok(false);
            }
            Ok(self
                .store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
                .is_ok())
        })
        .await
    }

    async fn list_published(&self, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        telemetry::record_persistence("nats", "outbox", "list_published", async {
            validate_outbox_claim_limit(limit)?;
            if limit == 0 {
                return Ok(Vec::new());
            }
            let mut keys = self.store.keys().await.map_err(map_error)?;
            let mut published = Vec::with_capacity(limit);
            while let Some(key) = keys.try_next().await.map_err(map_error)? {
                let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                    continue;
                };
                let mut record = decode(&self.codec, &entry.value)?;
                if record.state == StoredState::Published {
                    record.restore_published_at(entry_unix_ms(&entry)?);
                    published.push(record.message);
                    published.sort_unstable_by_key(|message| {
                        (
                            message.published_at_unix_ms().unwrap_or(u64::MAX),
                            message.id(),
                        )
                    });
                    if published.len() > limit {
                        published.pop();
                    }
                }
            }
            Ok(published)
        })
        .await
    }

    async fn cleanup_published(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("nats", "outbox", "cleanup_published", async {
            validate_retention_cleanup_limit(limit)?;
            if limit == 0 {
                return Ok(0);
            }
            let retention = u64::try_from(retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "outbox retention exceeds the supported millisecond range",
                )
            })?;
            let now = current_unix_ms()?;
            let mut keys = self.store.keys().await.map_err(map_error)?;
            let mut inspected = 0;
            let mut removed = 0;
            while inspected < limit {
                let Some(key) = keys.try_next().await.map_err(map_error)? else {
                    break;
                };
                inspected += 1;
                let Some(entry) = self.store.entry(&key).await.map_err(map_error)? else {
                    continue;
                };
                let mut record = decode(&self.codec, &entry.value)?;
                if record.state != StoredState::Published {
                    continue;
                }
                record.restore_published_at(entry_unix_ms(&entry)?);
                if record
                    .message
                    .published_at_unix_ms()
                    .is_some_and(|published_at| now.saturating_sub(published_at) >= retention)
                    && self
                        .store
                        .delete_expect_revision(&key, Some(entry.revision))
                        .await
                        .is_ok()
                {
                    removed += 1;
                }
            }
            Ok(removed)
        })
        .await
    }
}

const RECORD_MAGIC: &[u8; 5] = b"CGOB\x04";
const PREVIOUS_RECORD_MAGIC: &[u8; 5] = b"CGOB\x03";
const OLDER_RECORD_MAGIC: &[u8; 5] = b"CGOB\x02";
const LEGACY_RECORD_MAGIC: &[u8; 5] = b"CGOB\x01";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoredState {
    Pending,
    Claimed,
    Failed,
    Published,
}

impl StoredState {
    fn encode(self) -> u8 {
        match self {
            Self::Pending => 0,
            Self::Claimed => 1,
            Self::Failed => 2,
            Self::Published => 3,
        }
    }

    fn decode(value: u8) -> CatgaResult<Self> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Claimed),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Published),
            _ => Err(CatgaError::new(
                ErrorCode::Internal,
                "NATS outbox state is malformed",
            )),
        }
    }

    fn from_message(message: &OutboxMessage) -> Self {
        match message.state() {
            catga_core::OutboxState::Pending => Self::Pending,
            catga_core::OutboxState::Claimed => Self::Claimed,
            catga_core::OutboxState::Failed => Self::Failed,
            catga_core::OutboxState::Published => Self::Published,
        }
    }
}

struct StoredRecord {
    owner: Box<str>,
    state: StoredState,
    message: OutboxMessage,
}

impl StoredRecord {
    fn restore_published_at(&mut self, fallback: u64) {
        if self.state == StoredState::Published && self.message.state() != OutboxState::Published {
            self.message.claim("");
            self.message.mark_published(fallback);
        }
    }
}

fn encode(
    codec: &MemoryPackCodec,
    state: StoredState,
    message: &OutboxMessage,
) -> CatgaResult<Vec<u8>> {
    let payload = codec.encode(message.envelope())?;
    let owner = message.owner().unwrap_or_default().as_bytes();
    let owner_length = u16::try_from(owner.len())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "outbox owner is too long"))?;
    let claim_token = message.claim_token().unwrap_or_default().as_bytes();
    let claim_token_length = u16::try_from(claim_token.len())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "outbox claim token is too long"))?;
    let error = message.last_error().unwrap_or_default().as_bytes();
    let error_length = u16::try_from(error.len())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "outbox failure reason is too long"))?;
    let capacity = RECORD_MAGIC
        .len()
        .checked_add(1)
        .and_then(|length| length.checked_add(2))
        .and_then(|length| length.checked_add(2))
        .and_then(|length| length.checked_add(4))
        .and_then(|length| length.checked_add(4))
        .and_then(|length| length.checked_add(2))
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(8))
        .and_then(|length| length.checked_add(owner.len()))
        .and_then(|length| length.checked_add(claim_token.len()))
        .and_then(|length| length.checked_add(error.len()))
        .and_then(|length| length.checked_add(payload.len()))
        .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "NATS outbox record is too large"))?;
    let mut value = Vec::with_capacity(capacity);
    value.extend_from_slice(RECORD_MAGIC);
    value.push(state.encode());
    value.extend_from_slice(&owner_length.to_be_bytes());
    value.extend_from_slice(&claim_token_length.to_be_bytes());
    value.extend_from_slice(&message.retry_count().to_be_bytes());
    value.extend_from_slice(&message.max_retries().to_be_bytes());
    value.extend_from_slice(&error_length.to_be_bytes());
    value.extend_from_slice(
        &message
            .published_at_unix_ms()
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    value.extend_from_slice(
        &message
            .claimed_until_unix_ms()
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    value.extend_from_slice(owner);
    value.extend_from_slice(claim_token);
    value.extend_from_slice(error);
    value.extend_from_slice(&payload);
    Ok(value)
}

fn decode(codec: &MemoryPackCodec, value: &[u8]) -> CatgaResult<StoredRecord> {
    if value.starts_with(RECORD_MAGIC) {
        return decode_versioned(codec, &value[RECORD_MAGIC.len()..], true, true, true);
    }
    if value.starts_with(PREVIOUS_RECORD_MAGIC) {
        return decode_versioned(
            codec,
            &value[PREVIOUS_RECORD_MAGIC.len()..],
            true,
            true,
            false,
        );
    }
    if value.starts_with(OLDER_RECORD_MAGIC) {
        return decode_versioned(
            codec,
            &value[OLDER_RECORD_MAGIC.len()..],
            true,
            false,
            false,
        );
    }
    if value.starts_with(LEGACY_RECORD_MAGIC) {
        return decode_versioned(
            codec,
            &value[LEGACY_RECORD_MAGIC.len()..],
            false,
            false,
            false,
        );
    }
    decode_legacy(codec, value)
}

fn decode_versioned(
    codec: &MemoryPackCodec,
    value: &[u8],
    includes_published_at: bool,
    includes_claimed_until: bool,
    includes_claim_token: bool,
) -> CatgaResult<StoredRecord> {
    const BASE_PREFIX_LENGTH: usize = 1 + 2 + 4 + 4 + 2;
    let prefix_length = BASE_PREFIX_LENGTH
        + usize::from(includes_claim_token) * 2
        + usize::from(includes_published_at) * 8
        + usize::from(includes_claimed_until) * 8;
    if value.len() < prefix_length {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox record is malformed",
        ));
    }
    let state = StoredState::decode(value[0])?;
    let owner_length = usize::from(u16::from_be_bytes([value[1], value[2]]));
    let claim_token_length = if includes_claim_token {
        usize::from(u16::from_be_bytes([value[3], value[4]]))
    } else {
        0
    };
    let field_offset = usize::from(includes_claim_token) * 2;
    let retry_count = u32::from_be_bytes([
        value[3 + field_offset],
        value[4 + field_offset],
        value[5 + field_offset],
        value[6 + field_offset],
    ]);
    let max_retries = u32::from_be_bytes([
        value[7 + field_offset],
        value[8 + field_offset],
        value[9 + field_offset],
        value[10 + field_offset],
    ]);
    let error_length = usize::from(u16::from_be_bytes([
        value[11 + field_offset],
        value[12 + field_offset],
    ]));
    let mut offset = BASE_PREFIX_LENGTH + field_offset;
    let published_at = if includes_published_at {
        let timestamp = u64::from_be_bytes([
            value[offset],
            value[offset + 1],
            value[offset + 2],
            value[offset + 3],
            value[offset + 4],
            value[offset + 5],
            value[offset + 6],
            value[offset + 7],
        ]);
        offset += 8;
        Some(timestamp)
    } else {
        None
    };
    let claimed_until = if includes_claimed_until {
        let deadline = u64::from_be_bytes([
            value[offset],
            value[offset + 1],
            value[offset + 2],
            value[offset + 3],
            value[offset + 4],
            value[offset + 5],
            value[offset + 6],
            value[offset + 7],
        ]);
        offset += 8;
        Some(deadline)
    } else {
        None
    };
    let owner_start = offset;
    let token_start = owner_start.checked_add(owner_length).ok_or_else(|| {
        CatgaError::new(ErrorCode::Internal, "NATS outbox owner length overflows")
    })?;
    let error_start = token_start.checked_add(claim_token_length).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox claim token length overflows",
        )
    })?;
    let payload_start = error_start.checked_add(error_length).ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox failure reason length overflows",
        )
    })?;
    if payload_start > value.len() {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS outbox record lengths are malformed",
        ));
    }
    let owner = std::str::from_utf8(&value[owner_start..token_start])
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let claim_token = std::str::from_utf8(&value[token_start..error_start])
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let last_error = std::str::from_utf8(&value[error_start..payload_start])
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let mut message = OutboxMessage::new(codec.decode(&value[payload_start..])?)
        .with_max_retries(max_retries)?
        .with_retry_history(retry_count, (!last_error.is_empty()).then_some(last_error));
    match state {
        StoredState::Claimed => {
            if !claim_token.is_empty() {
                message.claim_until_with_token(
                    owner,
                    claim_token,
                    claimed_until
                        .filter(|value| *value != u64::MAX)
                        .unwrap_or(0),
                );
            } else {
                message.claim_until(
                    owner,
                    claimed_until
                        .filter(|value| *value != u64::MAX)
                        .unwrap_or(0),
                );
            }
        }
        StoredState::Published => {
            if let Some(published_at) = published_at.filter(|value| *value != u64::MAX) {
                message.claim("");
                message.mark_published(published_at);
            }
        }
        StoredState::Pending | StoredState::Failed => {}
    }
    Ok(StoredRecord {
        owner: owner.into(),
        state,
        message,
    })
}

fn decode_legacy(codec: &MemoryPackCodec, value: &[u8]) -> CatgaResult<StoredRecord> {
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
    let mut message = OutboxMessage::new(codec.decode(&value[start..])?)
        .with_max_retries(DEFAULT_OUTBOX_MAX_RETRIES)?;
    if !owner.is_empty() {
        message.claim_until(owner.as_str(), 0);
    }
    Ok(StoredRecord {
        state: if owner.is_empty() {
            StoredState::Pending
        } else {
            StoredState::Claimed
        },
        owner: owner.into_boxed_str(),
        message,
    })
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

fn current_unix_ms() -> CatgaResult<u64> {
    system_time_unix_ms(SystemTime::now())
}

fn entry_unix_ms(entry: &kv::Entry) -> CatgaResult<u64> {
    system_time_unix_ms(entry.created.into())
}

fn system_time_unix_ms(time: SystemTime) -> CatgaResult<u64> {
    let elapsed = time.duration_since(UNIX_EPOCH).map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "system clock precedes the Unix epoch")
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "system clock exceeds the supported millisecond range",
        )
    })
}

fn is_revision_conflict(error: &kv::UpdateError) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<jetstream::context::PublishError>())
        .is_some_and(|source| is_revision_conflict_kind(source.kind()))
}

fn is_revision_conflict_kind(kind: jetstream::context::PublishErrorKind) -> bool {
    kind == jetstream::context::PublishErrorKind::WrongLastSequence
}
