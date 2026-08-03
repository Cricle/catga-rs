#![forbid(unsafe_code)]
//! Flow runtime for Catga.
//!
//! The flow module provides durable, checkpointable flow execution for long-running
//! business processes with support for parallel branches, wait conditions, and
//! compensation/rollback semantics.

pub mod child_launch;
/// Flow step completion tracking and signaling.
pub mod completion;
/// Flow definition and DSL builders.
pub mod definition;
/// Core DSL flow execution engine.
pub mod dsl;
/// Checkpoint frame and work types for durable flows.
pub mod dsl_checkpoint;
/// Lifecycle management for DSL flows.
pub mod dsl_lifecycle;
/// Recovery support for parallel branch flows.
pub mod dsl_parallel_recovery;
/// Progress tracking for DSL step execution.
pub mod dsl_progress;
/// Checkpoint persistence and recovery operations.
pub mod dsl_recovery;
/// DSL step definitions and merge strategies.
pub mod dsl_step;
/// Checkpoint-aware when_any execution for DSL flows.
pub mod dsl_when_any;
/// Due service processing for scheduled flows.
pub mod due_service;
/// Flow executor for running durable flows.
pub mod executor;
/// Local in-memory flow execution.
pub mod local;
/// MemoryPack codec support for flow state.
pub mod memorypack;
/// Metrics collection for flow execution.
pub mod metrics;
/// Persistence layer for flow state stores.
pub mod persistence;
/// Runtime execution context for flows.
pub mod runtime;
/// Wait condition handling for child flows.
pub mod runtime_wait;
/// Task scheduling for delayed flow execution.
pub mod scheduler;
/// Serialization helpers for Arc slice types.
pub mod serde_helpers;
/// Flow state management and persistence.
pub mod state;
/// State machine definitions for flows.
pub mod state_machine;
/// Persistence store implementations for flows.
pub mod store;
/// Suspension state for paused flows.
pub mod suspension;
/// Persistence for suspended flow states.
pub mod suspension_store;
/// Wire format for suspension serialization.
pub mod suspension_wire;
/// Tag-based policy enforcement for flows.
pub mod tag_policy;
/// Timeout handling for flow execution.
pub mod timeout;

pub use child_launch::FlowChildLauncher;
pub use completion::{FlowCompletion, FlowCompletionAdapter};
pub use definition::{FlowDefinition, FlowStepOutcome};
pub use dsl::{DslFlow, FlowThrottle};
pub use dsl_lifecycle::{
    DslFlowFailedHook, DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver,
    DslFlowStepFailedHook, DslFlowStepSucceededHook, DslFlowSucceededHook,
};
pub use dsl_progress::{DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore};
pub use dsl_step::{DslQueryStep, DslStep, MAX_DSL_PARALLEL_BRANCHES};
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
pub use store::{FlowStore, MAX_FLOW_STORE_BATCH, validate_flow_batch_size};
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
