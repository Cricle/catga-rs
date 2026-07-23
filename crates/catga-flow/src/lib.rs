#![forbid(unsafe_code)]
//! Durable and compensating flow primitives for Catga.

mod definition;
mod executor;
mod local;
mod runtime;
mod scheduler;
mod state;
mod store;
mod suspension;
mod suspension_store;

pub use definition::{FlowDefinition, FlowStepOutcome};
pub use executor::FlowExecutor;
pub use local::{Flow, FlowResult};
pub use runtime::{FlowRuntime, FlowRuntimeResult};
pub use scheduler::{FlowScheduler, MemoryFlowScheduler, ScheduledResume};
pub use state::{FlowState, FlowStatus};
pub use store::FlowStore;
pub use suspension::{FlowContinuation, WaitCondition, WaitPolicy, WaitResult};
pub use suspension_store::SuspendedFlowStore;
