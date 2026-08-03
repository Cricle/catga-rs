use std::time::Duration;

use async_trait::async_trait;
use crate::{CatgaError, CatgaResult, ErrorCode};

use crate::flow::state::FlowState;

/// Maximum number of flow states one [`FlowStore::create_batch`] call accepts.
///
/// The bound keeps a single durable batch transaction's memory and lock footprint predictable,
/// matching the framework's finite-batching contract. Callers with more flows issue multiple
/// bounded batches.
pub const MAX_FLOW_STORE_BATCH: usize = 1024;

/// Validates a requested batch size against [`MAX_FLOW_STORE_BATCH`].
pub fn validate_flow_batch_size(count: usize) -> CatgaResult<()> {
    if count > MAX_FLOW_STORE_BATCH {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "flow batch size exceeds the supported maximum",
        ));
    }
    Ok(())
}

/// Persists durable flow state with optimistic concurrency.
#[async_trait]
pub trait FlowStore: Send + Sync {
    /// Creates `state` when no flow has the same identity.
    async fn create(&self, state: FlowState) -> CatgaResult<bool>;

    /// Creates multiple independent flow states as one durable unit of work.
    ///
    /// The returned flags align positionally with `states`: each is `true` when that flow was
    /// newly created and `false` when its identity already existed. The default implementation
    /// creates each state sequentially. Backends that support transactional batching override it
    /// to commit every insert in a single transaction, amortizing the per-commit durability
    /// flush across the whole batch while keeping every record fully durable.
    async fn create_batch(&self, states: Vec<FlowState>) -> CatgaResult<Vec<bool>> {
        validate_flow_batch_size(states.len())?;
        let mut created = Vec::with_capacity(states.len());
        for state in states {
            created.push(self.create(state).await?);
        }
        Ok(created)
    }

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
