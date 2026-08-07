//! JetStream KV revision-CAS distributed leases.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, LeaseStore, telemetry};

/// A JetStream KV lease store using entry revisions for conditional updates.
pub struct NatsLeases {
    store: kv::Store,
}

impl NatsLeases {
    /// Connects and idempotently provisions a KV bucket for lease resources.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = crate::kv::open_or_create(&context, bucket.as_ref())
            .await
            .map_err(map_error)?;
        Ok(Self { store })
    }
    async fn entry(&self, resource: &str) -> CatgaResult<Option<kv::Entry>> {
        self.store.entry(resource).await.map_err(map_error)
    }
}

#[async_trait]
impl LeaseStore for NatsLeases {
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        telemetry::record_persistence_claim("nats", "lease", "try_acquire", async {
            let value = value(owner, ttl);
            if self
                .store
                .create(resource, value.clone().into())
                .await
                .is_ok()
            {
                return Ok(true);
            }
            let Some(entry) = self.entry(resource).await? else {
                return Ok(false);
            };
            let Some((current_owner, expires)) = parse(&entry.value) else {
                return Ok(false);
            };
            if current_owner != owner && expires > now_millis() {
                return Ok(false);
            }
            Ok(self
                .store
                .update(resource, value.into(), entry.revision)
                .await
                .is_ok())
        })
        .await
    }
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "lease", "renew", async {
            let Some(entry) = self.entry(resource).await? else {
                return Ok(false);
            };
            let Some((current_owner, expires)) = parse(&entry.value) else {
                return Ok(false);
            };
            if current_owner != owner || expires <= now_millis() {
                return Ok(false);
            }
            Ok(self
                .store
                .update(resource, value(owner, ttl).into(), entry.revision)
                .await
                .is_ok())
        })
        .await
    }
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool> {
        telemetry::record_persistence("nats", "lease", "release", async {
            let Some(entry) = self.entry(resource).await? else {
                return Ok(false);
            };
            if parse(&entry.value).is_none_or(|(current_owner, _)| current_owner != owner) {
                return Ok(false);
            }
            Ok(self
                .store
                .delete_expect_revision(resource, Some(entry.revision))
                .await
                .is_ok())
        })
        .await
    }
}

fn value(owner: &str, ttl: Duration) -> String {
    format!(
        "{owner}\t{}",
        now_millis().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1))
    )
}
fn parse(value: &[u8]) -> Option<(&str, u64)> {
    let text = std::str::from_utf8(value).ok()?;
    let (owner, expiry) = text.rsplit_once('\t')?;
    Some((owner, expiry.parse().ok()?))
}
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn value_format_matches_parse_roundtrip() {
        let owner = "worker-1";
        let ttl = Duration::from_secs(30);
        let val = value(owner, ttl);
        let parsed = parse(val.as_bytes());
        assert!(parsed.is_some(), "value string should be parseable: {val}");
        let (parsed_owner, parsed_expires) = parsed.expect("value string should be parseable");
        assert_eq!(parsed_owner, owner);
        // expiry should be approximately now + ttl (within 10 seconds tolerance)
        let expected_min = now_millis() + ttl.as_millis() as u64;
        let expected_max = expected_min + 10_000;
        assert!(
            parsed_expires >= expected_min && parsed_expires <= expected_max,
            "expiry {parsed_expires} should be between {expected_min} and {expected_max}"
        );
    }

    #[test]
    fn value_uses_tab_separator() {
        let owner = "test-worker";
        let ttl = Duration::from_secs(60);
        let val = value(owner, ttl);
        // The format is "owner\texpiry"
        assert!(
            val.contains('\t'),
            "value should contain tab separator: {val}"
        );
        let parts: Vec<&str> = val.split('\t').collect();
        assert_eq!(parts.len(), 2, "should have exactly two parts");
        assert_eq!(parts[0], owner);
        // Second part should be a valid u64
        assert!(parts[1].parse::<u64>().is_ok());
    }

    #[test]
    fn parse_rejects_invalid_utf8() {
        // Invalid UTF-8 sequence
        let invalid = vec![0x80, 0x81, 0x82];
        assert!(parse(&invalid).is_none());
    }

    #[test]
    fn parse_rejects_missing_tab() {
        // No tab separator
        let no_tab = b"owner_only_no_expiry";
        assert!(parse(no_tab).is_none());
    }

    #[test]
    fn parse_rejects_invalid_expiry() {
        // Valid owner but invalid expiry (not a number)
        let invalid_expiry = "owner\tnot_a_number";
        assert!(parse(invalid_expiry.as_bytes()).is_none());
    }

    #[test]
    fn parse_rejects_empty_expiry() {
        // Owner with empty expiry
        let empty_expiry = "owner\t";
        assert!(parse(empty_expiry.as_bytes()).is_none());
    }

    #[test]
    fn parse_accepts_empty_owner() {
        // Empty owner is technically valid in the tab-separated format
        // (e.g., "\t1234567890" produces owner="", expiry=1234567890)
        let empty_owner = "\t1234567890";
        assert!(parse(empty_owner.as_bytes()).is_some());
        let parsed = parse(empty_owner.as_bytes()).expect("empty owner should parse");
        assert_eq!(parsed.0, "");
        assert_eq!(parsed.1, 1234567890);
    }

    #[test]
    fn parse_accepts_various_owners() {
        // Simple ASCII owner
        let val = value("worker", Duration::from_secs(1));
        assert!(parse(val.as_bytes()).is_some());

        // Owner with special characters (that are valid UTF-8)
        let val2 = value("worker-1@region-2", Duration::from_secs(1));
        assert!(parse(val2.as_bytes()).is_some());

        // Owner with unicode
        let val3 = value("工作器-1", Duration::from_secs(1));
        assert!(parse(val3.as_bytes()).is_some());
    }

    #[test]
    fn value_ttl_must_be_at_least_1_millis() {
        // Even with zero duration, expiry should be at least now + 1
        let owner = "test";
        let val = value(owner, Duration::ZERO);
        let parsed = parse(val.as_bytes()).expect("should parse");
        let (_, expires) = parsed;
        // expiry should be >= now (not zero, not negative)
        assert!(
            expires > now_millis(),
            "zero-ttl expiry should still be in the future"
        );
    }

    #[test]
    fn value_handles_very_long_ttl() {
        // Test with a very long TTL (100 years)
        let owner = "test";
        let very_long_ttl = Duration::from_secs(60 * 60 * 24 * 365 * 100);
        let val = value(owner, very_long_ttl);
        let parsed = parse(val.as_bytes()).expect("should parse");
        let (_, expires) = parsed;
        // Should handle u64::MAX gracefully
        assert!(expires > now_millis());
    }

    #[test]
    fn now_millis_returns_positive_value() {
        let now = now_millis();
        // Should be a reasonable Unix timestamp (after year 2020)
        assert!(now > 1_577_836_800_000u64, "timestamp should be after 2020");
        // Should be less than year 2100
        assert!(
            now < 4_104_451_840_000u64,
            "timestamp should be before 2100"
        );
    }

    #[test]
    fn now_millis_is_monotonic_increasing() {
        // Two calls should return the same or slightly different values
        let now1 = now_millis();
        let now2 = now_millis();
        assert!(now2 >= now1, "subsequent calls should return >= value");
    }

    #[test]
    fn map_error_creates_transient_error() {
        let err = map_error("connection refused");
        assert_eq!(err.code(), ErrorCode::Transient);
        assert!(err.to_string().contains("connection refused"));
    }
}
