//! Optimistic storage contract for state-machine snapshots.

use async_trait::async_trait;
use catga_core::CatgaResult;

use super::StateMachineSnapshot;

/// Persists state-machine instances with compare-and-swap versioning.
#[async_trait]
pub trait StateMachineStore<S>: Send + Sync {
    /// Creates an instance when no snapshot already has the same identity.
    async fn create(&self, snapshot: StateMachineSnapshot<S>) -> CatgaResult<bool>;

    /// Loads one instance snapshot by identity.
    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<S>>>;

    /// Replaces a snapshot only when its current version is `expected_version` and `next` is its
    /// exact representable successor.
    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<S>,
    ) -> CatgaResult<bool>;
}
