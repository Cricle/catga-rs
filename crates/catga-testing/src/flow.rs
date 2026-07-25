//! Typed, in-memory dependencies for durable Flow runtime tests.

use std::sync::Arc;

use catga_flow::MemoryFlowScheduler;
use catga_memory::MemorySuspendedFlows;

/// Test-only ownership of the bounded in-memory Flow persistence dependencies.
///
/// Each context owns one [`MemorySuspendedFlows`] and one [`MemoryFlowScheduler`], exposing
/// clones for direct construction of a [`catga_flow::FlowRuntime`]. It deliberately provides no
/// global mediator, runtime registration, reflection, or production lifecycle management.
#[derive(Clone, Default)]
pub struct FlowTestContext {
    suspended_flows: Arc<MemorySuspendedFlows>,
    scheduler: Arc<MemoryFlowScheduler>,
}

impl FlowTestContext {
    /// Creates isolated in-memory continuation storage and scheduling for one Flow test.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the context's bounded in-memory suspended-flow store.
    pub fn suspended_flows(&self) -> Arc<MemorySuspendedFlows> {
        Arc::clone(&self.suspended_flows)
    }

    /// Returns the context's deterministic in-memory Flow scheduler.
    pub fn scheduler(&self) -> Arc<MemoryFlowScheduler> {
        Arc::clone(&self.scheduler)
    }
}
