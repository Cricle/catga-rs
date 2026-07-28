#![forbid(unsafe_code)]
//! Durable and compensating flow primitives for Catga.
//!
//! Use [`Flow`] for a small in-process sequence with reverse-order compensation. Use
//! [`FlowDefinition`] with a caller-owned [`SuspendedFlowStore`] when work must survive a
//! restart, wait for a child result, or resume after a delay. [`FlowRuntime`] owns one definition
//! and performs version-fenced transitions; the application owns storage, scheduler polling, and
//! transport integration. This keeps deployment and failure policy explicit instead of starting
//! hidden background tasks.
//!
//! # Durable flow recipe
//!
//! 1. Build a [`FlowDefinition`] with stable step names. [`flow_definition!`] removes only the
//!    repetitive builder reassignment; ordinary [`FlowDefinition`] calls remain available for
//!    dynamic composition.
//! 2. Construct a [`FlowRuntime`] with a durable [`SuspendedFlowStore`] and a [`FlowScheduler`].
//!    Call [`FlowRuntime::start`] for new work and [`FlowRuntime::resume`] from the worker that
//!    owns a due schedule or child completion.
//! 3. Poll a [`DueFlowScheduler`] from an application-owned worker and acknowledge a schedule
//!    only after `resume` completes. For child completions, route the result through
//!    [`FlowCompletionAdapter`] or the correlation-based `FlowRuntime::record_wait_*` methods.
//!
//! A lease prevents a stale executor from persisting a later transition after another owner has
//! taken over. It cannot undo an already-started external action: durable flow steps are
//! at-least-once, so charge, email, and other external effects must use an idempotency key derived
//! from the stable flow and step identity. [`FlowRuntime::cancel`] fences late state writes but
//! does not cancel an external system that has already accepted a request.
//!
//! # Deterministic delayed transition
//!
//! A zero delay advances immediately and therefore does not allocate a timer or persist a
//! needless scheduled wake-up:
//!
//! ```
//! use std::time::Duration;
//! use catga_flow::FlowStepOutcome;
//!
//! assert!(matches!(FlowStepOutcome::delay(Duration::ZERO)?, FlowStepOutcome::Advance));
//! # Ok::<(), catga_core::CatgaError>(())
//! ```
//!
//! # Durable composition
//!
//! ```
//! use catga_flow::{FlowStepOutcome, flow_definition};
//!
//! let checkout = flow_definition! {
//!     "checkout";
//!     "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
//!     "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
//! };
//! assert_eq!(checkout.name(), "checkout");
//! ```

mod child_launch;
mod completion;
mod definition;
mod dsl;
mod dsl_checkpoint;
mod dsl_parallel_recovery;
mod dsl_progress;
mod dsl_recovery;
mod dsl_when_any;
mod due_service;
mod executor;
mod local;
mod memorypack;
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

pub use child_launch::FlowChildLauncher;
pub use completion::{FlowCompletion, FlowCompletionAdapter};
pub use definition::{FlowDefinition, FlowStepOutcome};
pub use dsl::{
    DslFlow, DslFlowFailedHook, DslFlowLifecycleEvent, DslFlowLifecycleHooks,
    DslFlowLifecycleObserver, DslFlowStepFailedHook, DslFlowStepSucceededHook,
    DslFlowSucceededHook, DslQueryStep, DslStep, FlowThrottle, MAX_DSL_PARALLEL_BRANCHES,
};
pub use dsl_progress::{DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore};
pub use due_service::{DueFlowOptions, FlowDueService};
pub use executor::{FlowExecutor, FlowHeartbeatOptions, FlowRecoveryOptions};
pub use local::{Flow, FlowResult};
pub use persistence::{decode_continuation, encode_continuation};
pub use runtime::{FlowRuntime, FlowRuntimeResult};
pub use scheduler::{DueFlowScheduler, FlowScheduler, MemoryFlowScheduler, ScheduledResume};
pub use state::{FlowState, FlowStatus, MAX_FLOW_DATA_BYTES};
pub use state_machine::{StateMachine, StateMachineResult};
pub use state_machine::{
    StateMachineBuilder, StateMachineEventRouter, StateMachineExecutor, StateMachineSnapshot,
    StateMachineState, StateMachineStore, decode_state_machine_snapshot,
    encode_state_machine_snapshot,
};
pub use store::FlowStore;
pub use suspension::{
    FlowChildLaunch, FlowChildLaunchState, FlowContinuation, MAX_FLOW_COMPENSATIONS,
    MAX_WAIT_CHILDREN, MAX_WAIT_RESULT_BYTES, WaitCondition, WaitPolicy, WaitResult,
};
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
/// ```
/// use catga_flow::{FlowStepOutcome, flow_definition};
///
/// let definition = flow_definition! {
///     "payment";
///     "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
///     "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
/// };
/// assert_eq!(definition.name(), "payment");
/// ```
#[macro_export]
macro_rules! flow_definition {
    ($name:expr; $($step_name:expr => $handler:expr);+ $(;)?) => {{
        let definition = $crate::FlowDefinition::new($name);
        $(let definition = definition.step($step_name, $handler);)+
        definition
    }};
}

/// Converts a typed async callback closure into one that returns a boxed future.
///
/// This removes the repetitive `Box::pin(async move { ... })` wrapper required by callback APIs
/// that return boxed futures, while preserving the closure parameter and result types exactly.
/// The async block remains explicit, so callback errors continue to use the result type required
/// by the receiving API (for example, [`catga_core::CatgaResult`]).
///
/// ```
/// use catga_core::CatgaError;
///
/// let callback = catga_flow::flow_async!(|value: u32| async move {
///     Ok::<_, CatgaError>(value + 1)
/// });
/// let _future = callback(41);
/// ```
///
/// A `move` closure is also supported when the callback needs to own captured values:
///
/// ```
/// use catga_core::CatgaError;
///
/// let suffix = String::from("!");
/// let callback = catga_flow::flow_async!(move |request: String| async move {
///     Ok::<_, CatgaError>(format!("{request}{suffix}"))
/// });
/// let _future = callback(String::from("done"));
/// ```
#[macro_export]
macro_rules! flow_async {
    (|$($argument:tt : $argument_ty:ty),+ $(,)?| async move $body:block) => {
        |$($argument: $argument_ty),+| ::std::boxed::Box::pin(async move $body)
    };
    (move |$($argument:tt : $argument_ty:ty),+ $(,)?| async move $body:block) => {
        move |$($argument: $argument_ty),+| ::std::boxed::Box::pin(async move $body)
    };
}

/// Converts a natural async state action into a [`DslFlow`] action closure.
///
/// ```
/// use catga_core::CatgaError;
/// use catga_flow::DslFlow;
///
/// struct State(u32);
/// let _flow = DslFlow::new().action(catga_flow::dsl_action!(|state: &mut State| async move {
///     state.0 += 1;
///     Ok::<_, CatgaError>(())
/// }));
/// ```
#[macro_export]
macro_rules! dsl_action {
    (|$state:ident : $state_ty:ty| async move $body:block) => {
        |$state: $state_ty| Box::pin(async move $body)
    };
}

/// Converts a natural async item action into a [`DslFlow::for_each`] action closure.
///
/// ```
/// use catga_core::CatgaError;
/// use catga_flow::DslFlow;
///
/// struct State(u32);
/// let _flow = DslFlow::new().for_each(|state: &State| vec![state.0], catga_flow::dsl_each_action!(|state: &mut State, item: u32| async move {
///     state.0 += item;
///     Ok::<_, CatgaError>(())
/// }));
/// ```
#[macro_export]
macro_rules! dsl_each_action {
    (|$state:ident : $state_ty:ty, $item:ident : $item_ty:ty| async move $body:block) => {
        |$state: $state_ty, $item: $item_ty| Box::pin(async move $body)
    };
}
