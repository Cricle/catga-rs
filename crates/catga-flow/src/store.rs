use std::time::Duration;

use async_trait::async_trait;
use catga_core::CatgaResult;

use crate::FlowState;

/// Persists durable flow state with optimistic concurrency.
#[async_trait]
pub trait FlowStore: Send + Sync {
    /// Creates `state` when no flow has the same identity.
    async fn create(&self, state: FlowState) -> CatgaResult<bool>;

    /// Replaces a state only when its current version equals `expected_version`.
    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool>;

    /// Loads one flow state by identity.
    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>>;

    /// Claims one stale running flow of `flow_type` for `owner`.
    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>>;

    /// Records owner liveness if both owner and version are still current.
    ///
    /// A heartbeat does not change the logical flow version, but may replace the physical stored
    /// record. Callers that transition from the same version must handle a lost physical CAS by
    /// reloading the state before deciding whether ownership was lost.
    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool>;
}
