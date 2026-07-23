#![forbid(unsafe_code)]
//! Durable and compensating flow primitives for Catga.

mod definition;
mod dsl;
mod executor;
mod local;
mod runtime;
mod scheduler;
mod state;
mod state_machine;
mod store;
mod suspension;
mod suspension_store;

pub use definition::{FlowDefinition, FlowStepOutcome};
pub use dsl::DslFlow;
pub use executor::FlowExecutor;
pub use local::{Flow, FlowResult};
pub use runtime::{FlowRuntime, FlowRuntimeResult};
pub use scheduler::{FlowScheduler, MemoryFlowScheduler, ScheduledResume};
pub use state::{FlowState, FlowStatus};
pub use state_machine::{StateMachine, StateMachineResult};
pub use state_machine::{
    StateMachineBuilder, StateMachineEventRouter, StateMachineExecutor, StateMachineSnapshot,
    StateMachineState, StateMachineStore,
};
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

/// Converts a natural async state action into a [`DslFlow`] action closure.
///
/// ```ignore
/// .action(dsl_action!(|state: &mut State| async move { Ok(()) }))
/// ```
#[macro_export]
macro_rules! dsl_action {
    (|$state:ident : $state_ty:ty| async move $body:block) => {
        |$state: $state_ty| Box::pin(async move $body)
    };
}
