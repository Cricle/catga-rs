//! Application-owned launch boundary for durable Flow child fan-out.

use async_trait::async_trait;
use catga_core::CatgaResult;

/// Starts one stable child identity for a durable parent wait.
///
/// The runtime persists a child identity before invoking this boundary and can invoke the same
/// identity again after a process crash or an expired launch claim. Implementations must therefore
/// make `(parent_flow_id, child_id)` idempotent. The runtime never spawns or retains child tasks.
#[async_trait]
pub trait FlowChildLauncher: Send + Sync {
    /// Starts the stable `child_id` for `parent_flow_id` and its persisted wait correlation.
    async fn launch(
        &self,
        parent_flow_id: &str,
        child_id: &str,
        correlation_id: &str,
    ) -> CatgaResult<()>;
}
