use std::sync::Arc;

use crate::{CatgaError, CatgaResult};

use crate::flow::{
    runtime::{FlowRuntime, FlowRuntimeResult},
    scheduler::FlowScheduler,
    suspension_store::SuspendedFlowStore,
};

/// A child-flow completion supplied by the caller after its transport has decoded it.
///
/// The value deliberately contains neither transport metadata nor acknowledgement state.  Its
/// [`Self::correlation_id`] identifies the parent wait and its [`Self::child_id`] identifies one
/// expected child within that wait.  Both identities are owned, so a caller can construct this
/// value from a transient broker or RPC payload without retaining that payload's backing storage.
#[derive(Debug)]
pub enum FlowCompletion {
    /// A child completed successfully with its bounded opaque result payload.
    Success {
        /// The parent wait correlation identity.
        correlation_id: Box<str>,
        /// The stable identity of the completed child.
        child_id: Box<str>,
        /// The child result payload passed to the durable wait.
        payload: Vec<u8>,
    },
    /// A child completed with a structured Catga error.
    Failure {
        /// The parent wait correlation identity.
        correlation_id: Box<str>,
        /// The stable identity of the completed child.
        child_id: Box<str>,
        /// The failure to record for the child.
        error: CatgaError,
    },
}

impl FlowCompletion {
    /// Creates a successful completion with owned parent and child identities.
    pub fn success(
        correlation_id: impl Into<Box<str>>,
        child_id: impl Into<Box<str>>,
        payload: Vec<u8>,
    ) -> Self {
        Self::Success {
            correlation_id: correlation_id.into(),
            child_id: child_id.into(),
            payload,
        }
    }

    /// Creates a failed completion with owned parent and child identities.
    pub fn failure(
        correlation_id: impl Into<Box<str>>,
        child_id: impl Into<Box<str>>,
        error: CatgaError,
    ) -> Self {
        Self::Failure {
            correlation_id: correlation_id.into(),
            child_id: child_id.into(),
            error,
        }
    }

    /// Returns the immutable parent wait correlation identity.
    pub fn correlation_id(&self) -> &str {
        match self {
            Self::Success { correlation_id, .. } | Self::Failure { correlation_id, .. } => {
                correlation_id
            }
        }
    }

    /// Returns the immutable stable child identity.
    pub fn child_id(&self) -> &str {
        match self {
            Self::Success { child_id, .. } | Self::Failure { child_id, .. } => child_id,
        }
    }
}

/// Caller-owned adapter that records decoded child completions for one durable flow runtime.
///
/// This adapter does not decode transport messages, acknowledge deliveries, add retries, or
/// spawn work.  [`Self::record`] delegates validation, version fencing, duplicate handling, and
/// parent resumption to [`FlowRuntime`]'s correlation-based completion APIs.
pub struct FlowCompletionAdapter<S: ?Sized, H: ?Sized> {
    runtime: Arc<FlowRuntime<S, H>>,
}

impl<S, H> FlowCompletionAdapter<S, H>
where
    S: SuspendedFlowStore + ?Sized,
    H: FlowScheduler + ?Sized,
{
    /// Creates an adapter over a caller-owned durable flow runtime.
    pub fn new(runtime: Arc<FlowRuntime<S, H>>) -> Self {
        Self { runtime }
    }

    /// Records one decoded child completion and returns the resulting parent runtime state.
    ///
    /// The runtime validates the payload and child identity, performs the indexed correlation
    /// lookup, applies optimistic-concurrency fencing, and resumes the parent only when its wait
    /// policy permits it.  Unknown correlations and all other validation failures are returned
    /// unchanged to the caller.
    pub async fn record(&self, completion: FlowCompletion) -> CatgaResult<FlowRuntimeResult> {
        match completion {
            FlowCompletion::Success {
                correlation_id,
                child_id,
                payload,
            } => {
                self.runtime
                    .record_wait_success_by_correlation(&correlation_id, &child_id, payload)
                    .await
            }
            FlowCompletion::Failure {
                correlation_id,
                child_id,
                error,
            } => {
                self.runtime
                    .record_wait_failure_by_correlation(&correlation_id, &child_id, error)
                    .await
            }
        }
    }
}
