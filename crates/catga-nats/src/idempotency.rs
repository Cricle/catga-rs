//! JetStream KV revision-CAS idempotency records.

use std::sync::Arc;

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, IdempotencyStore, ProcessingState};

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;
const RETRIES: usize = 8;

/// JetStream KV-backed idempotency store with per-key revision CAS.
pub struct NatsIdempotency {
    store: kv::Store,
}

impl NatsIdempotency {
    /// Connects and provisions a one-history KV bucket for idempotency keys.
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
        Ok(Self { store })
    }

    async fn entry(&self, key: &str) -> CatgaResult<Option<kv::Entry>> {
        self.store.entry(key).await.map_err(map_error)
    }

    async fn transition(&self, key: &str, next: Vec<u8>) -> CatgaResult<()> {
        let key = kv_key(key);
        for _ in 0..RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Err(CatgaError::new(
                    ErrorCode::NotFound,
                    "idempotency key is not claimed",
                ));
            };
            if state(&entry.value)? != ProcessingState::Claimed {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "idempotency key is not currently claimed",
                ));
            }
            if self
                .store
                .update(&key, next.clone().into(), entry.revision)
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS idempotency compare-and-swap did not stabilize",
        ))
    }
}

#[async_trait]
impl IdempotencyStore for NatsIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        let key = kv_key(key);
        for _ in 0..RETRIES {
            match self.entry(&key).await? {
                None => {
                    if self.store.create(&key, vec![CLAIMED].into()).await.is_ok() {
                        return Ok(true);
                    }
                }
                Some(entry)
                    if matches!(
                        entry.operation,
                        kv::Operation::Delete | kv::Operation::Purge
                    ) =>
                {
                    if self
                        .store
                        .update(&key, vec![CLAIMED].into(), entry.revision)
                        .await
                        .is_ok()
                    {
                        return Ok(true);
                    }
                }
                Some(entry) => match state(&entry.value)? {
                    ProcessingState::Failed => {
                        if self
                            .store
                            .update(&key, vec![CLAIMED].into(), entry.revision)
                            .await
                            .is_ok()
                        {
                            return Ok(true);
                        }
                    }
                    _ => return Ok(false),
                },
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS idempotency compare-and-swap did not stabilize",
        ))
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let mut value = Vec::with_capacity(
            result
                .as_ref()
                .map_or(1, |value| value.len().saturating_add(1)),
        );
        value.push(if result.is_some() {
            COMPLETED_RESULT
        } else {
            COMPLETED_EMPTY
        });
        if let Some(result) = result {
            value.extend_from_slice(&result);
        }
        self.transition(key, value).await
    }

    async fn fail(&self, key: &str) -> CatgaResult<()> {
        self.transition(key, vec![FAILED]).await
    }

    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        self.entry(&kv_key(key))
            .await?
            .map(|entry| state(&entry.value))
            .transpose()
    }

    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        let Some(entry) = self.entry(&kv_key(key)).await? else {
            return Ok(None);
        };
        Ok((entry.value.first() == Some(&COMPLETED_RESULT)).then(|| Arc::from(&entry.value[1..])))
    }
}

fn state(value: &[u8]) -> CatgaResult<ProcessingState> {
    match value.first() {
        Some(&CLAIMED) => Ok(ProcessingState::Claimed),
        Some(&COMPLETED_EMPTY | &COMPLETED_RESULT) => Ok(ProcessingState::Completed),
        Some(&FAILED) => Ok(ProcessingState::Failed),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS idempotency record is malformed",
        )),
    }
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

fn kv_key(key: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = key.as_bytes();
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(1));
    encoded.push('k');
    for &byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}
