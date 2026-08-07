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
    /// Connects and provisions a one-history KV bucket using the default completed-record policy.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_retention(server, bucket, DEFAULT_IDEMPOTENCY_RETENTION).await
    }

    /// Connects and provisions a one-history KV bucket with a completed-record retention policy.
    ///
    /// The duration controls explicit bounded cleanup through
    /// [`IdempotencyStore::cleanup_completed`]. The bucket has no maximum age:
    /// claimed and failed records must not expire before their state transition.
    /// Existing buckets with a maximum age are reset during connection.
    pub async fn with_retention(
        server: &str,
        bucket: impl Into<Box<str>>,
        retention: Duration,
    ) -> CatgaResult<Self> {
        validate_completed_retention(retention)?;
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = crate::kv::open_or_create(&context, bucket.as_ref())
            .await
            .map_err(map_error)?;
        let status = store.status().await.map_err(map_error)?;
        if !status.max_age().is_zero() {
            let mut config = status.info.config.clone();
            config.max_age = Duration::ZERO;
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

    pub(crate) async fn try_claim_until(
        &self,
        key: &str,
        expires_at: u64,
    ) -> CatgaResult<Option<u64>> {
        telemetry::record_persistence_optional_claim("nats", "idempotency", "try_claim", async {
            let key = kv_key(key);
            let value = claimed_with_expiry(expires_at);
            let now = now_millis();
            for _ in 0..RETRIES {
                match self.entry(&key).await? {
                    None => {
                        if let Ok(revision) = self.store.create(&key, value.clone().into()).await {
                            return Ok(Some(revision));
                        }
                    }
                    Some(entry)
                        if matches!(
                            entry.operation,
                            kv::Operation::Delete | kv::Operation::Purge
                        ) =>
                    {
                        if let Ok(revision) = self
                            .store
                            .update(&key, value.clone().into(), entry.revision)
                            .await
                        {
                            return Ok(Some(revision));
                        }
                    }
                    Some(entry) => match state(&entry.value)? {
                        ProcessingState::Failed => {
                            if let Ok(revision) = self
                                .store
                                .update(&key, value.clone().into(), entry.revision)
                                .await
                            {
                                return Ok(Some(revision));
                            }
                        }
                        ProcessingState::Claimed if claim_expired(&entry.value, now) => {
                            if let Ok(revision) = self
                                .store
                                .update(&key, value.clone().into(), entry.revision)
                                .await
                            {
                                return Ok(Some(revision));
                            }
                        }
                        _ => return Ok(None),
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

    pub(crate) async fn complete_claim(
        &self,
        key: &str,
        generation: u64,
        result: Option<Arc<[u8]>>,
    ) -> CatgaResult<()> {
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
        self.transition_claim(key, generation, value).await
    }

    pub(crate) async fn fail_claim(&self, key: &str, generation: u64) -> CatgaResult<()> {
        self.transition_claim(key, generation, vec![FAILED]).await
    }

    async fn transition_claim(&self, key: &str, generation: u64, next: Vec<u8>) -> CatgaResult<()> {
        let key = kv_key(key);
        let Some(entry) = self.entry(&key).await? else {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "inbox message is not claimed",
            ));
        };
        if entry.revision != generation || state(&entry.value)? != ProcessingState::Claimed {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "inbox claim is no longer owned",
            ));
        }
        self.store
            .update(&key, next.into(), generation)
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Conflict, "inbox claim is no longer owned"))?;
        Ok(())
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
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                continue;
            }
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
        telemetry::record_persistence_claim("nats", "idempotency", "try_claim", async {
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
            let Some(entry) = self.entry(&kv_key(key)).await? else {
                return Ok(None);
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(None);
            }
            state(&entry.value).map(Some)
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- state() tests ---

    #[test]
    fn state_claimed() {
        let value = vec![CLAIMED];
        assert_eq!(
            state(&value).expect("state should be Claimed"),
            ProcessingState::Claimed
        );
    }

    #[test]
    fn state_completed_empty() {
        let value = vec![COMPLETED_EMPTY];
        assert_eq!(
            state(&value).expect("state should be Completed"),
            ProcessingState::Completed
        );
    }

    #[test]
    fn state_completed_result() {
        let mut value = vec![COMPLETED_RESULT];
        value.extend_from_slice(b"result data");
        assert_eq!(
            state(&value).expect("state should be Completed"),
            ProcessingState::Completed
        );
    }

    #[test]
    fn state_failed() {
        let value = vec![FAILED];
        assert_eq!(
            state(&value).expect("state should be Failed"),
            ProcessingState::Failed
        );
    }

    #[test]
    fn state_rejects_unknown_first_byte() {
        let value = vec![99]; // Unknown state
        assert!(state(&value).is_err());
        let err = state(&value).expect_err("state should return an error for unknown byte");
        assert_eq!(err.code(), ErrorCode::Internal);
        assert!(err.to_string().contains("malformed"));
    }

    #[test]
    fn state_rejects_empty() {
        let value = vec![];
        assert!(state(&value).is_err());
    }

    #[test]
    fn state_handles_large_payload() {
        // State byte followed by lots of data
        let mut value = vec![COMPLETED_RESULT];
        value.extend(vec![0u8; 1000]);
        assert_eq!(
            state(&value).expect("state should be Completed"),
            ProcessingState::Completed
        );
    }

    // --- claimed_with_expiry() tests ---

    #[test]
    fn claimed_with_expiry_format() {
        let expires_at = 1_700_000_000_000u64;
        let value = claimed_with_expiry(expires_at);
        assert_eq!(value[0], CLAIMED);
        let decoded = u64::from_be_bytes(value[1..9].try_into().expect("9 bytes for u64"));
        assert_eq!(decoded, expires_at);
    }

    #[test]
    fn claimed_with_expiry_zero() {
        let value = claimed_with_expiry(0);
        assert_eq!(value[0], CLAIMED);
        assert_eq!(value.len(), 9);
    }

    #[test]
    fn claimed_with_expiry_max_value() {
        let value = claimed_with_expiry(u64::MAX);
        assert_eq!(value[0], CLAIMED);
        let decoded = u64::from_be_bytes(value[1..9].try_into().expect("9 bytes for u64"));
        assert_eq!(decoded, u64::MAX);
    }

    // --- claim_expired() tests ---

    #[test]
    fn claim_expired_when_expired() {
        let expires_at = 1_000_000_000_000u64; // Far in the past
        let value = claimed_with_expiry(expires_at);
        let now = expires_at + 1;
        assert!(claim_expired(&value, now));
    }

    #[test]
    fn claim_expired_when_not_expired() {
        let expires_at = 9_000_000_000_000u64; // Far in the future
        let value = claimed_with_expiry(expires_at);
        let now = 1_000_000_000_000u64;
        assert!(!claim_expired(&value, now));
    }

    #[test]
    fn claim_expired_at_exact_boundary() {
        let expires_at = 1_000_000_000u64;
        let value = claimed_with_expiry(expires_at);
        // At exactly the expiry time, it's considered expired
        assert!(claim_expired(&value, expires_at));
        assert!(!claim_expired(&value, expires_at - 1));
    }

    #[test]
    fn claim_expired_rejects_short_payload() {
        // Too short to contain u64 expiry
        let value = vec![CLAIMED];
        let now = u64::MAX;
        assert!(claim_expired(&value, now));
    }

    #[test]
    fn claim_expired_rejects_empty() {
        let value = vec![];
        let now = u64::MAX;
        assert!(claim_expired(&value, now));
    }

    #[test]
    fn claim_expired_rejects_partial_u64() {
        // Has CLAIMED but not enough bytes for full u64
        let value = vec![CLAIMED, 0x12, 0x34];
        let now = u64::MAX;
        assert!(claim_expired(&value, now));
    }

    // --- kv_key() tests ---

    #[test]
    fn kv_key_prefix() {
        let result = kv_key("");
        assert!(
            result.starts_with('k'),
            "kv_key should start with 'k': {result}"
        );
    }

    #[test]
    fn kv_key_empty() {
        let result = kv_key("");
        assert_eq!(result, "k");
    }

    #[test]
    fn kv_key_simple() {
        let result = kv_key("abc");
        assert_eq!(result, "k616263");
    }

    #[test]
    fn kv_key_digits() {
        let result = kv_key("123");
        assert_eq!(result, "k313233");
    }

    #[test]
    fn kv_key_hex_encoding() {
        // Test with bytes that are valid in strings
        // 0x00 = '0', 0x0f = 'f', 0xff cannot be in string directly
        let result = kv_key("\x00\x0f");
        assert_eq!(result, "k000f");
    }

    #[test]
    fn kv_key_special_chars() {
        // Colon, underscore, hyphen, dot
        let result = kv_key("test:key-1.val");
        assert_eq!(result, "k746573743a6b65792d312e76616c");
    }

    #[test]
    fn kv_key_unicode() {
        // Unicode characters
        let result = kv_key("你好");
        // UTF-8: e4 bd a0 e5 a5 bd
        assert_eq!(result, "ke4bda0e5a5bd");
    }

    #[test]
    fn kv_key_idempotent() {
        let key = "order-12345";
        let result1 = kv_key(key);
        let result2 = kv_key(key);
        assert_eq!(result1, result2, "kv_key should be deterministic");
    }

    #[test]
    fn kv_key_unique_for_different_inputs() {
        let result_a = kv_key("a");
        let result_b = kv_key("b");
        assert_ne!(
            result_a, result_b,
            "different keys should produce different results"
        );
    }

    #[test]
    fn kv_key_encoding_correctness() {
        // Verify the hex encoding is correct
        // 'A' = 0x41, 'B' = 0x42
        let result = kv_key("AB");
        assert_eq!(result, "k4142");

        // Verify lowercase hex
        let result = kv_key("\x00\x0f");
        assert_eq!(result, "k000f");
    }

    #[test]
    fn kv_key_consistent_length() {
        // Verify that kv_key produces consistent encoding length
        // The encoding is: 'k' + 2 hex chars per byte
        let test_cases: &[(&str, usize)] = &[
            ("", 1),       // just 'k'
            ("a", 3),      // 'k' + 2 hex chars
            ("ab", 5),     // 'k' + 4 hex chars
            ("abc", 7),    // 'k' + 6 hex chars
            ("hello", 11), // 'k' + 10 hex chars
        ];
        for (input, expected_len) in test_cases {
            let result = kv_key(input);
            assert_eq!(
                result.len(),
                *expected_len,
                "kv_key({:?}) length should be {}",
                input,
                expected_len
            );
        }

        // Verify the pattern: 'k' prefix + 2 hex chars per input byte
        for i in 0..10 {
            let input = "x".repeat(i);
            let result = kv_key(&input);
            assert_eq!(
                result.len(),
                1 + 2 * i,
                "length for {} chars should be {}",
                i,
                1 + 2 * i
            );
        }
    }

    // --- now_millis() tests ---

    #[test]
    fn now_millis_returns_reasonable_timestamp() {
        let now = now_millis();
        // Should be after Jan 1, 2020 (Unix ms)
        assert!(now > 1_577_836_800_000u64);
        // Should be before Jan 1, 2100
        assert!(now < 4_104_451_840_000u64);
    }

    #[test]
    fn now_millis_is_increasing() {
        let now1 = now_millis();
        let now2 = now_millis();
        assert!(now2 >= now1);
    }

    // --- map_error() tests ---

    #[test]
    fn map_error_creates_transient_error() {
        let err = map_error("test error message");
        assert_eq!(err.code(), ErrorCode::Transient);
        assert!(err.to_string().contains("test error message"));
    }

    #[test]
    fn map_error_handles_empty_string() {
        let err = map_error("");
        assert_eq!(err.code(), ErrorCode::Transient);
    }

    #[test]
    fn map_error_handles_unicode_error() {
        let err = map_error("错误消息");
        assert_eq!(err.code(), ErrorCode::Transient);
    }

    // --- Constant tests ---

    #[test]
    fn state_constants_are_distinct() {
        assert_ne!(CLAIMED, COMPLETED_EMPTY);
        assert_ne!(CLAIMED, COMPLETED_RESULT);
        assert_ne!(CLAIMED, FAILED);
        assert_ne!(COMPLETED_EMPTY, COMPLETED_RESULT);
        assert_ne!(COMPLETED_EMPTY, FAILED);
        assert_ne!(COMPLETED_RESULT, FAILED);
    }

    #[test]
    fn state_constants_are_single_bytes() {
        // All constants are u8 literals, so they fit in a u8 by construction
    }

    #[test]
    fn retries_constant() {
        assert_eq!(RETRIES, 8);
    }
}
