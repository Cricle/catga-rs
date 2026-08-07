//! Redis Lua-CAS idempotency records.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, ErrorCode, IdempotencyStore,
    ProcessingState, telemetry, validate_completed_retention, validate_retention_cleanup_limit,
};
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use sha2::{Digest, Sha256};

use crate::transport::map_error;

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;
const MAX_RESULT_BYTES: usize = 1024 * 1024;
const MAX_REDIS_RETENTION_MILLIS: i64 = 100 * 365 * 24 * 60 * 60 * 1_000;

const CLAIM: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false or string.byte(value, 1) == 4 then
    redis.call('SET', KEYS[1], string.char(1))
    return 1
end
return 0
"#;

const TRANSITION: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false then return -1 end
if string.byte(value, 1) ~= 1 then return 0 end
if ARGV[2] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
else
    redis.call('SET', KEYS[1], ARGV[1])
end
return 1
"#;

/// Redis-backed idempotency store with atomic per-key processing transitions.
///
/// Caller-provided keys are SHA-256-derived before reaching Redis, keeping the
/// persisted key size fixed and avoiding disclosure of business identifiers.
pub struct RedisIdempotency {
    connection: ConnectionManager,
    prefix: Box<str>,
    completed_retention_millis: i64,
}

impl RedisIdempotency {
    /// Connects to Redis and namespaces records beneath `prefix`.
    ///
    /// Completed records retain their cached results for
    /// [`DEFAULT_IDEMPOTENCY_RETENTION`]. Use [`Self::with_retention`] to
    /// configure a different completed-record retention period.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        Self::with_retention(server, prefix, DEFAULT_IDEMPOTENCY_RETENTION).await
    }

    /// Connects to Redis with a completed-record retention period.
    ///
    /// A successful [`IdempotencyStore::complete`] atomically replaces a
    /// claimed record and assigns this duration as its Redis `PX` expiration.
    /// Claimed and failed records never receive this expiration. `retention`
    /// must be nonzero; sub-millisecond values round up to one millisecond,
    /// while values greater than the maximum 100-year retention return
    /// [`ErrorCode::Validation`] before a Redis connection is attempted.
    pub async fn with_retention(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
        retention: Duration,
    ) -> CatgaResult<Self> {
        let completed_retention_millis = retention_millis(retention)?;
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            completed_retention_millis,
        })
    }

    fn key(&self, key: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(key.as_bytes());
        format!("{}:{}", self.prefix, hex::encode(digest.finalize()))
    }

    async fn transition(
        &self,
        key: &str,
        value: &[u8],
        retention_millis: Option<i64>,
    ) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        let script = Script::new(TRANSITION);
        let transition = match retention_millis {
            Some(retention_millis) => {
                script
                    .key(self.key(key))
                    .arg(value)
                    .arg(retention_millis)
                    .invoke_async::<i64>(&mut connection)
                    .await
            }
            None => {
                script
                    .key(self.key(key))
                    .arg(value)
                    .invoke_async::<i64>(&mut connection)
                    .await
            }
        }
        .map_err(map_error)?;
        match transition {
            1 => Ok(()),
            -1 => Err(CatgaError::new(
                ErrorCode::NotFound,
                "idempotency key is not claimed",
            )),
            _ => Err(CatgaError::new(
                ErrorCode::Conflict,
                "idempotency key is not currently claimed",
            )),
        }
    }
}

#[async_trait]
impl IdempotencyStore for RedisIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        telemetry::record_persistence_claim("redis", "idempotency", "try_claim", async {
            let mut connection = self.connection.clone();
            Script::new(CLAIM)
                .key(self.key(key))
                .invoke_async::<i64>(&mut connection)
                .await
                .map(|claimed| claimed == 1)
                .map_err(map_error)
        })
        .await
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "idempotency", "complete", async {
            if result
                .as_ref()
                .is_some_and(|result| result.len() > MAX_RESULT_BYTES)
            {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "idempotency result exceeds the Redis payload limit",
                ));
            }
            let mut value = Vec::with_capacity(
                result
                    .as_ref()
                    .map_or(1, |result| result.len().saturating_add(1)),
            );
            value.push(if result.is_some() {
                COMPLETED_RESULT
            } else {
                COMPLETED_EMPTY
            });
            if let Some(result) = result {
                value.extend_from_slice(&result);
            }
            self.transition(key, &value, Some(self.completed_retention_millis))
                .await
        })
        .await
    }

    async fn fail(&self, key: &str) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "idempotency", "fail", async {
            self.transition(key, &[FAILED], None).await
        })
        .await
    }

    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        telemetry::record_persistence("redis", "idempotency", "state", async {
            let mut connection = self.connection.clone();
            let value: Option<Vec<u8>> = connection.get(self.key(key)).await.map_err(map_error)?;
            value.map(|value| state(&value)).transpose()
        })
        .await
    }

    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        telemetry::record_persistence("redis", "idempotency", "result", async {
            let mut connection = self.connection.clone();
            let value: Option<Vec<u8>> = connection.get(self.key(key)).await.map_err(map_error)?;
            Ok(value.and_then(|value| {
                (value.first() == Some(&COMPLETED_RESULT)).then(|| Arc::from(&value[1..]))
            }))
        })
        .await
    }

    /// Validates `limit` without scanning because Redis expires completed records through key TTL.
    async fn cleanup_completed(&self, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("redis", "idempotency", "cleanup", async {
            validate_retention_cleanup_limit(limit)?;
            Ok(0)
        })
        .await
    }
}

fn retention_millis(retention: Duration) -> CatgaResult<i64> {
    validate_completed_retention(retention)?;
    let milliseconds = retention.as_millis();
    let milliseconds = if retention.subsec_nanos().is_multiple_of(1_000_000) {
        milliseconds
    } else {
        milliseconds.checked_add(1).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "idempotency retention exceeds the supported millisecond range",
            )
        })?
    };
    let retention_millis = i64::try_from(milliseconds).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "idempotency retention exceeds Redis's supported millisecond range",
        )
    })?;
    if retention_millis > MAX_REDIS_RETENTION_MILLIS {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "idempotency retention exceeds the Redis maximum of 100 years",
        ));
    }
    Ok(retention_millis)
}

fn state(value: &[u8]) -> CatgaResult<ProcessingState> {
    match value.first() {
        Some(&CLAIMED) => Ok(ProcessingState::Claimed),
        Some(&COMPLETED_EMPTY | &COMPLETED_RESULT) => Ok(ProcessingState::Completed),
        Some(&FAILED) => Ok(ProcessingState::Failed),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            "Redis idempotency record is malformed",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // State function tests

    #[test]
    fn state_claimed() {
        let value = vec![CLAIMED];
        assert_eq!(
            state(&value).expect("claimed state"),
            ProcessingState::Claimed
        );
    }

    #[test]
    fn state_completed_empty() {
        let value = vec![COMPLETED_EMPTY];
        assert_eq!(
            state(&value).expect("completed empty"),
            ProcessingState::Completed
        );
    }

    #[test]
    fn state_completed_result() {
        let value = vec![COMPLETED_RESULT, b'd', b'a', b't', b'a'];
        assert_eq!(
            state(&value).expect("completed result"),
            ProcessingState::Completed
        );
    }

    #[test]
    fn state_failed() {
        let value = vec![FAILED];
        assert_eq!(
            state(&value).expect("failed state"),
            ProcessingState::Failed
        );
    }

    #[test]
    fn state_malformed_empty() {
        let err = state(&[]).expect_err("empty fails");
        assert_eq!(err.code(), ErrorCode::Internal);
        assert!(err.message().contains("malformed"));
    }

    #[test]
    fn state_malformed_unknown_byte() {
        let value = vec![99]; // Unknown byte value
        let err = state(&value).expect_err("unknown byte fails");
        assert_eq!(err.code(), ErrorCode::Internal);
        assert!(err.message().contains("malformed"));
    }

    #[test]
    fn state_multi_byte_value_with_claimed_prefix() {
        // Test that state only checks the first byte, regardless of remaining content
        let value = vec![CLAIMED, 0, 0, 0];
        assert_eq!(
            state(&value).expect("claimed prefix"),
            ProcessingState::Claimed
        );
    }

    #[test]
    fn state_multi_byte_value_with_completed_result_prefix() {
        // Completed result with payload data still returns Completed
        let value = vec![COMPLETED_RESULT, 1, 2, 3, 4, 5];
        assert_eq!(
            state(&value).expect("completed result prefix"),
            ProcessingState::Completed
        );
    }

    // Retention millis tests

    #[test]
    fn retention_millis_valid() {
        let duration = Duration::from_secs(60);
        let millis = retention_millis(duration).expect("valid");
        assert_eq!(millis, 60_000);
    }

    #[test]
    fn retention_millis_zero_fails() {
        let err = retention_millis(Duration::ZERO).expect_err("zero fails");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(err.message().contains("greater than zero"));
    }

    #[test]
    fn retention_millis_with_sub_millis_rounds_up() {
        // 1ms + 500 microseconds = 1.5ms should round up to 2ms
        // subsec_nanos for 1.5ms = 500_000 (500 microseconds)
        let duration = Duration::from_millis(1).saturating_add(Duration::from_micros(500));
        let millis = retention_millis(duration).expect("valid");
        assert_eq!(millis, 2); // 1.5ms rounds up to 2ms
    }

    #[test]
    fn retention_millis_exceeds_max_fails() {
        // 100 years + 1 millisecond (exceeds i64 max redis retention)
        let duration = Duration::from_millis((MAX_REDIS_RETENTION_MILLIS as u64) + 1);
        let err = retention_millis(duration).expect_err("exceeds max fails");
        assert_eq!(err.code(), ErrorCode::Validation);
        assert!(err.message().contains("100 years"));
    }

    #[test]
    fn retention_millis_at_max() {
        let duration = Duration::from_millis(MAX_REDIS_RETENTION_MILLIS as u64);
        let millis = retention_millis(duration).expect("at max is valid");
        assert_eq!(millis, MAX_REDIS_RETENTION_MILLIS);
    }

    #[test]
    fn retention_millis_very_large() {
        // Test with a very large but valid duration
        let duration = Duration::from_secs(365 * 24 * 60 * 60); // 1 year
        let millis = retention_millis(duration).expect("1 year valid");
        assert_eq!(millis, 31_536_000_000);
    }

    #[test]
    fn retention_millis_one_millisecond() {
        // Minimum valid: exactly 1 millisecond
        let duration = Duration::from_millis(1);
        let millis = retention_millis(duration).expect("1ms valid");
        assert_eq!(millis, 1);
    }

    #[test]
    fn retention_millis_sub_millisecond_rounds_up() {
        // Sub-millisecond (500 nanoseconds) should round up to 1ms
        let duration = Duration::from_nanos(500);
        let millis = retention_millis(duration).expect("sub-ms valid");
        assert_eq!(millis, 1);
    }

    // Constants tests

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
    fn max_result_bytes_value() {
        assert_eq!(MAX_RESULT_BYTES, 1024 * 1024);
    }

    #[test]
    fn max_redis_retention_millis_value() {
        // Verify 100 years in milliseconds
        let expected = 100i64 * 365 * 24 * 60 * 60 * 1_000;
        assert_eq!(MAX_REDIS_RETENTION_MILLIS, expected);
    }

    #[test]
    fn state_constants_are_nonzero() {
        // All state constants should be non-zero for proper Lua byte comparison
        assert_ne!(CLAIMED, 0);
        assert_ne!(COMPLETED_EMPTY, 0);
        assert_ne!(COMPLETED_RESULT, 0);
        assert_ne!(FAILED, 0);
    }

    // Lua script tests

    #[test]
    fn claim_script_contains_expected_operations() {
        assert!(CLAIM.contains("GET"));
        assert!(CLAIM.contains("SET"));
        assert!(CLAIM.contains("string.byte"));
        assert!(CLAIM.contains("string.char"));
    }

    #[test]
    fn transition_script_contains_expected_operations() {
        assert!(TRANSITION.contains("GET"));
        assert!(TRANSITION.contains("SET"));
        assert!(TRANSITION.contains("PX")); // Millisecond expiration
    }
}
