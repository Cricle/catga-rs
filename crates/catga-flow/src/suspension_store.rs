use async_trait::async_trait;
use catga_core::CatgaResult;

use crate::FlowContinuation;

/// Persists suspended flow continuations with optimistic concurrency.
#[async_trait]
pub trait SuspendedFlowStore: Send + Sync {
    /// Creates a continuation when no flow has the same identity.
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool>;

    /// Loads one continuation by flow identity.
    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>>;

    /// Replaces a continuation after a business-state version transition.
    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool>;

    /// Records one successful child payload without changing the business-state version.
    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool>;
}
