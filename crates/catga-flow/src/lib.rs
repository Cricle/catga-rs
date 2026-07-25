#![forbid(unsafe_code)]
//! Durable and compensating flow primitives for Catga.

mod definition;
mod dsl;
mod dsl_checkpoint;
mod dsl_parallel_recovery;
mod dsl_progress;
mod dsl_recovery;
mod dsl_when_any;
mod due_service;
mod executor;
mod hot_reload;
mod local;
mod metrics;
mod persistence;
mod runtime;
mod scheduler;
mod state;
mod state_machine;
mod store;
mod suspension;
mod suspension_store;
mod tag_policy;
mod timeout;

pub use definition::{FlowDefinition, FlowStepOutcome};
pub use dsl::{
    DslFlow, DslFlowFailedHook, DslFlowLifecycleEvent, DslFlowLifecycleHooks,
    DslFlowLifecycleObserver, DslFlowStepFailedHook, DslFlowStepSucceededHook,
    DslFlowSucceededHook, FlowThrottle,
};
pub use dsl_progress::{DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore};
pub use due_service::{DueFlowOptions, FlowDueService};
pub use executor::{FlowExecutor, FlowHeartbeatOptions, FlowRecoveryOptions};
pub use hot_reload::{
    FlowRegistry, FlowReloaded, FlowVersionManager, RegistryFlowRuntime, VersionedFlowDefinition,
};
pub use local::{Flow, FlowResult};
pub use persistence::{decode_continuation, encode_continuation};
pub use runtime::{FlowRuntime, FlowRuntimeResult};
pub use scheduler::{DueFlowScheduler, FlowScheduler, MemoryFlowScheduler, ScheduledResume};
pub use state::{FlowState, FlowStatus};
pub use state_machine::{StateMachine, StateMachineResult};
pub use state_machine::{
    StateMachineBuilder, StateMachineEventRouter, StateMachineExecutor, StateMachineSnapshot,
    StateMachineState, StateMachineStore, decode_state_machine_snapshot,
    encode_state_machine_snapshot,
};
pub use store::FlowStore;
pub use suspension::{FlowContinuation, WaitCondition, WaitPolicy, WaitResult};
pub use suspension_store::{
    FlowQuery, FlowSummary, MAX_FLOW_QUERY_RESULTS, MAX_FLOW_QUERY_SCAN, SuspendedFlowStore,
};
pub use tag_policy::FlowTagPolicy;
pub use timeout::{
    DEFAULT_FLOW_TIMEOUT_BATCH_SIZE, DEFAULT_FLOW_TIMEOUT_SCAN_LIMIT, FlowTimeoutOptions,
    FlowTimeoutService, MAX_FLOW_TIMEOUT_BATCH_SIZE, MAX_FLOW_TIMEOUT_SCAN_LIMIT, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, flow_timeout_deadline_unix_ms,
};

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

/// Converts a natural async item action into a [`DslFlow::for_each`] action closure.
///
/// ```ignore
/// .for_each(|state| state.items.clone(), dsl_each_action!(|state: &mut State, item: Item| async move {
///     Ok(())
/// }))
/// ```
#[macro_export]
macro_rules! dsl_each_action {
    (|$state:ident : $state_ty:ty, $item:ident : $item_ty:ty| async move $body:block) => {
        |$state: $state_ty, $item: $item_ty| Box::pin(async move $body)
    };
}
