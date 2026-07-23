//! Distributed lease contracts for leader-only and singleton work.

use std::time::Duration;

use async_trait::async_trait;

use crate::CatgaResult;

/// Atomically coordinates expiring ownership of named resources.
#[async_trait]
pub trait LeaseStore: Send + Sync {
    /// Acquires or renews `resource` for `owner` when it is currently unowned or expired.
    async fn try_acquire(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;
    /// Extends a non-expired lease only when it remains owned by `owner`.
    async fn renew(&self, resource: &str, owner: &str, ttl: Duration) -> CatgaResult<bool>;
    /// Releases a lease only when it remains owned by `owner`.
    async fn release(&self, resource: &str, owner: &str) -> CatgaResult<bool>;
}
