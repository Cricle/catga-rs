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
