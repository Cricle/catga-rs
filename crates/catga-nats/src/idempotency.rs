//! JetStream KV revision-CAS idempotency records.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, ErrorCode, IdempotencyStore,
    ProcessingState, telemetry, validate_completed_retention, validate_retention_cleanup_limit,
};
use futures::TryStreamExt;

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;
const RETRIES: usize = 8;

/// JetStream KV-backed idempotency store with per-key revision CAS.
pub struct NatsIdempotency {
    store: kv::Store,
    retention: Duration,
}

impl NatsIdempotency {
    /// Connects and provisions a one-history KV bucket using the default completed-record TTL.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_retention(server, bucket, DEFAULT_IDEMPOTENCY_RETENTION).await
    }

    /// Connects and provisions a one-history KV bucket whose records expire after `retention`.
    ///
    /// Existing buckets are updated to the requested JetStream maximum age. The same duration
    /// controls explicit bounded cleanup through [`IdempotencyStore::cleanup_completed`].
    pub async fn with_retention(
        server: &str,
        bucket: impl Into<Box<str>>,
        retention: Duration,
    ) -> CatgaResult<Self> {
        validate_completed_retention(retention)?;
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = match context.get_key_value(bucket.as_ref()).await {
            Ok(store) => store,
            Err(_) => match context
                .create_key_value(kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    max_age: retention,
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
        let status = store.status().await.map_err(map_error)?;
        if status.max_age() != retention {
            let mut config = status.info.config.clone();
            config.max_age = retention;
            context.update_stream(config).await.map_err(map_error)?;
        }
        Ok(Self { store, retention })
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

    pub(crate) async fn try_claim_until(&self, key: &str, expires_at: u64) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "idempotency", "try_claim", async {
            let key = kv_key(key);
            let value = claimed_with_expiry(expires_at);
            let now = now_millis();
            for _ in 0..RETRIES {
                match self.entry(&key).await? {
                    None => {
                        if self.store.create(&key, value.clone().into()).await.is_ok() {
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
                            .update(&key, value.clone().into(), entry.revision)
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
                                .update(&key, value.clone().into(), entry.revision)
                                .await
                                .is_ok()
                            {
                                return Ok(true);
                            }
                        }
                        ProcessingState::Claimed if claim_expired(&entry.value, now) => {
                            if self
                                .store
                                .update(&key, value.clone().into(), entry.revision)
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
                "NATS inbox claim compare-and-swap did not stabilize",
            ))
        })
        .await
    }

    pub(crate) async fn cleanup_completed_for(
        &self,
        retention: Duration,
        limit: usize,
    ) -> CatgaResult<usize> {
        validate_retention_cleanup_limit(limit)?;
        if limit == 0 {
            return Ok(0);
        }
        let now = SystemTime::now();
        let mut keys = self.store.keys().await.map_err(map_error)?;
        let mut inspected = 0;
        let mut removed = 0;
        while inspected < limit {
            let Some(key) = keys.try_next().await.map_err(map_error)? else {
                break;
            };
            inspected += 1;
            let Some(entry) = self.entry(&key).await? else {
                continue;
            };
            let created_at: SystemTime = entry.created.into();
            if state(&entry.value)? == ProcessingState::Completed
                && now
                    .duration_since(created_at)
                    .is_ok_and(|age| age >= retention)
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
    }
}

#[async_trait]
impl IdempotencyStore for NatsIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "idempotency", "try_claim", async {
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
        })
        .await
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "idempotency", "complete", async {
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
        })
        .await
    }

    async fn fail(&self, key: &str) -> CatgaResult<()> {
        telemetry::record_persistence("nats", "idempotency", "fail", async {
            self.transition(key, vec![FAILED]).await
        })
        .await
    }

    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        telemetry::record_persistence("nats", "idempotency", "state", async {
            self.entry(&kv_key(key))
                .await?
                .map(|entry| state(&entry.value))
                .transpose()
        })
        .await
    }

    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        telemetry::record_persistence("nats", "idempotency", "result", async {
            let Some(entry) = self.entry(&kv_key(key)).await? else {
                return Ok(None);
            };
            Ok((entry.value.first() == Some(&COMPLETED_RESULT))
                .then(|| Arc::from(&entry.value[1..])))
        })
        .await
    }

    async fn cleanup_completed(&self, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("nats", "idempotency", "cleanup", async {
            self.cleanup_completed_for(self.retention, limit).await
        })
        .await
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

fn claimed_with_expiry(expires_at: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + std::mem::size_of::<u64>());
    value.push(CLAIMED);
    value.extend_from_slice(&expires_at.to_be_bytes());
    value
}

fn claim_expired(value: &[u8], now: u64) -> bool {
    value
        .get(1..)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_be_bytes)
        .is_none_or(|expires_at| expires_at <= now)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
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
