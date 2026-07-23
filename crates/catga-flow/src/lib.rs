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

/// Builds a named durable flow definition from registered async step handlers.
///
/// ```ignore
/// let definition = catga_flow::flow_definition! {
///     "payment";
///     "reserve" => |_| async { Ok(FlowStepOutcome::Advance) };
///     "charge" => |_| async { Ok(FlowStepOutcome::complete()) };
/// };
/// ```
#[macro_export]
macro_rules! flow_definition {
    ($name:expr; $($step_name:expr => $handler:expr);+ $(;)?) => {{
        let definition = $crate::FlowDefinition::new($name);
        $(let definition = definition.step($step_name, $handler);)+
        definition
    }};
}
