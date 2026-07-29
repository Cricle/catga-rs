//! Lifecycle observation hooks for top-level [`crate::DslFlow`] execution.

use catga_core::{CatgaError, CatgaResult};
use futures::future::BoxFuture;

/// One observable outcome from a top-level [`crate::DslFlow`] step or the flow itself.
#[derive(Clone, Debug)]
pub enum DslFlowLifecycleEvent {
    /// A top-level step completed successfully.
    StepSucceeded {
        /// Zero-based position of the completed step.
        step_index: usize,
    },
    /// A top-level step returned an error.
    StepFailed {
        /// Zero-based position of the failed step.
        step_index: usize,
        /// The original step error.
        error: CatgaError,
    },
    /// All top-level steps completed successfully.
    FlowSucceeded,
    /// Flow execution stopped at a failed step.
    FlowFailed {
        /// The original step error.
        error: CatgaError,
    },
}

/// Receives configured DSL flow lifecycle events synchronously.
///
/// Observers must not block because delivery occurs on the flow's execution future. An observer
/// does not control the flow result: step errors remain the result returned by
/// [`crate::DslFlow::run`].
pub trait DslFlowLifecycleObserver: Send + Sync {
    /// Records one step- or flow-level lifecycle event.
    fn observe(&self, event: &DslFlowLifecycleEvent);
}

/// Async callback invoked with immutable state after a top-level step succeeds.
pub type DslFlowStepSucceededHook<S> =
    Box<dyn for<'a> Fn(&'a S, usize) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;

/// Async callback invoked with immutable state after a top-level step fails.
pub type DslFlowStepFailedHook<S> = Box<
    dyn for<'a> Fn(&'a S, usize, &'a CatgaError) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync,
>;

/// Async callback invoked with immutable state after every top-level step succeeds.
pub type DslFlowSucceededHook<S> =
    Box<dyn for<'a> Fn(&'a S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;

/// Async callback invoked with immutable state after a top-level step failure ends the flow.
pub type DslFlowFailedHook<S> =
    Box<dyn for<'a> Fn(&'a S, &'a CatgaError) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;

/// Optional, async lifecycle hooks for one [`crate::DslFlow`].
///
/// Hooks run sequentially on the caller's execution future after the configured synchronous
/// [`DslFlowLifecycleObserver`]s. A hook error is returned unchanged and stops execution; it is
/// not converted into another lifecycle event. Hooks configured on an outer flow are emitted only
/// for that flow's top-level steps.
pub struct DslFlowLifecycleHooks<S> {
    pub(crate) step_succeeded: Option<DslFlowStepSucceededHook<S>>,
    pub(crate) step_failed: Option<DslFlowStepFailedHook<S>>,
    pub(crate) flow_succeeded: Option<DslFlowSucceededHook<S>>,
    pub(crate) flow_failed: Option<DslFlowFailedHook<S>>,
}

impl<S> DslFlowLifecycleHooks<S> {
    /// Creates an empty async lifecycle hook set.
    pub const fn new() -> Self {
        Self {
            step_succeeded: None,
            step_failed: None,
            flow_succeeded: None,
            flow_failed: None,
        }
    }

    /// Configures the hook invoked after each successful top-level step.
    pub fn on_step_succeeded<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a S, usize) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.step_succeeded = Some(Box::new(hook));
        self
    }

    /// Configures the hook invoked after a failed top-level step.
    pub fn on_step_failed<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a S, usize, &'a CatgaError) -> BoxFuture<'a, CatgaResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.step_failed = Some(Box::new(hook));
        self
    }

    /// Configures the hook invoked after all top-level steps succeed.
    pub fn on_flow_succeeded<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.flow_succeeded = Some(Box::new(hook));
        self
    }

    /// Configures the hook invoked after a top-level step failure ends the flow.
    pub fn on_flow_failed<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a S, &'a CatgaError) -> BoxFuture<'a, CatgaResult<()>>
            + Send
            + Sync
            + 'static,
    {
        self.flow_failed = Some(Box::new(hook));
        self
    }
}

impl<S> Default for DslFlowLifecycleHooks<S> {
    fn default() -> Self {
        Self::new()
    }
}
