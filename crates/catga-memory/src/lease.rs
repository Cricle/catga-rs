//! Sharded, expiring process-local leases for deterministic cluster tests.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{CatgaResult, LeaseStore};
use dashmap::{DashMap, mapref::entry::Entry};

/// An in-memory lease store with owner-conditional operations.
#[derive(Default)]
pub struct MemoryLeases {
    leases: DashMap<Box<str>, Lease>,
}

struct Lease {
    owner: Box<str>,
    expires_at_millis: AtomicU64,
}

#[async_trait]
impl LeaseStore for MemoryLeases {
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        let expiry = expiry(ttl);
        Ok(match self.leases.entry(resource.into()) {
            Entry::Vacant(entry) => {
                entry.insert(Lease {
                    owner: owner.into(),
                    expires_at_millis: AtomicU64::new(expiry),
                });
                true
            }
            Entry::Occupied(entry) if entry.get().owner.as_ref() == owner => {
                entry
                    .get()
                    .expires_at_millis
                    .store(expiry, Ordering::Release);
                true
            }
            Entry::Occupied(mut entry)
                if entry.get().expires_at_millis.load(Ordering::Acquire) <= now_millis() =>
            {
                entry.insert(Lease {
                    owner: owner.into(),
                    expires_at_millis: AtomicU64::new(expiry),
                });
                true
            }
            Entry::Occupied(_) => false,
        })
    }
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        let Some(lease) = self.leases.get(resource) else {
            return Ok(false);
        };
        if lease.owner.as_ref() != owner
            || lease.expires_at_millis.load(Ordering::Acquire) <= now_millis()
        {
            return Ok(false);
        }
        lease
            .expires_at_millis
            .store(expiry(ttl), Ordering::Release);
        Ok(true)
    }
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool> {
        Ok(match self.leases.entry(resource.into()) {
            Entry::Occupied(entry) if entry.get().owner.as_ref() == owner => {
                entry.remove();
                true
            }
            _ => false,
        })
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
fn expiry(ttl: Duration) -> u64 {
    now_millis().saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
}
