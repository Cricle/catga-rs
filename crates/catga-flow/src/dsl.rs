//! Typed, state-owning flow DSL primitives.

use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use crate::{
    DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore,
    dsl_checkpoint::{CheckpointFrame, CheckpointLevel, CheckpointWork, MAX_CHECKPOINT_PATH_DEPTH},
    dsl_parallel_recovery::run_checkpointed_parallel,
    dsl_recovery::{
        CheckpointContext, persist_checkpoint_payload, persist_completed_checkpoint,
        validate_replayable_for_each_items,
    },
    dsl_when_any::run_checkpointed_when_any,
    metrics::{FLOWS_COMPLETED, FLOWS_FAILED, FlowMetrics, ForEachMetrics},
};
use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer, MemoryPackable,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, Mediator, RemoteRequest, Request, RequestClient,
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

type Action<S> = Box<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
type Condition<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;
type StateFailure<S> = Box<dyn Fn(&S) -> Option<CatgaError> + Send + Sync>;
type Query<S, R> = Box<dyn for<'a> Fn(&'a S) -> BoxFuture<'a, CatgaResult<R>> + Send + Sync>;
type ResponseFailure<R> = Box<dyn Fn(&R) -> Option<CatgaError> + Send + Sync>;
type BranchSelector<S> = Box<dyn Fn(&S) -> Option<usize> + Send + Sync>;
pub(super) type Merge<S> = Box<dyn Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync>;
pub(super) type MergeWinner<S> = Box<dyn Fn(&mut S, S) -> CatgaResult<()> + Send + Sync>;
pub(super) type CloneState<S> = fn(&S) -> S;
type ReplayableForEachSelect<S> = Box<dyn Fn(&S) -> CatgaResult<Vec<Vec<u8>>> + Send + Sync>;
type ReplayableForEachItem<S> =
    Box<dyn for<'a> Fn(&'a mut S, &'a [u8]) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
type ReplayableForEachErrorHandler<S> =
    dyn for<'a> Fn(&'a mut S, usize, CatgaError) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync;

struct ReplayableForEach<S> {
    select: ReplayableForEachSelect<S>,
    action: ReplayableForEachItem<S>,
    on_error: Option<Box<ReplayableForEachErrorHandler<S>>>,
}

const DEFAULT_BRANCH: u32 = u32::MAX;

// Top-level DSL step indices cannot use this reserved progress slot.
const DSL_TERMINAL_STEP_INDEX: u32 = u32::MAX;
const MAX_DSL_TERMINAL_BYTES: usize = 1024 * 1024;

#[derive(Debug, MemoryPackable)]
struct CheckpointTerminal(Vec<u8>);

/// Largest number of branches one DSL parallel or `when_any` step may retain.
///
/// The caller-owned execution future keeps one cloned state and one nested future per branch, so
/// this bound prevents an untrusted or accidentally unbounded iterator from retaining unbounded
/// state or scheduling unbounded work. Checkpointed and in-process DSL execution share it.
pub const MAX_DSL_PARALLEL_BRANCHES: usize = 64;

/// A composable, state-mutating DSL action.
///
/// A `DslStep` owns only its action and decorator closures. Converting it through
/// [`DslFlow::step`] adds no task, lock, or persistence layer: the flow executes the resulting
/// action in its existing caller-owned future.
pub struct DslStep<S> {
    action: Action<S>,
    condition: Option<Condition<S>>,
    failure: Option<StateFailure<S>>,
    optional: bool,
}

impl<S: Send + 'static> DslStep<S> {
    /// Builds one asynchronous state-mutating action.
    pub fn action<F>(action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        Self {
            action: Box::new(action),
            condition: None,
            failure: None,
            optional: false,
        }
    }

    /// Builds one typed query whose result can later be stored with
    /// [`DslQueryStep::into_state`] or discarded with [`DslQueryStep::discard`].
    pub fn query<R, F>(query: F) -> DslQueryStep<S, R>
    where
        R: Send + 'static,
        F: for<'a> Fn(&'a S) -> BoxFuture<'a, CatgaResult<R>> + Send + Sync + 'static,
    {
        DslQueryStep {
            query: Box::new(query),
            condition: None,
            failure: None,
            optional: false,
        }
    }

    /// Builds a typed mediator request step.
    ///
    /// The request is constructed only after the step condition permits execution. The response
    /// remains available to [`DslQueryStep::fail_if_response`] before it is written to state.
    pub fn send<M, F>(mediator: Arc<Mediator>, request: F) -> DslQueryStep<S, M::Response>
    where
        M: Request,
        F: Fn(&S) -> M + Send + Sync + 'static,
    {
        Self::query(move |state| {
            let message = request(state);
            let mediator = Arc::clone(&mediator);
            Box::pin(async move { mediator.send(message).await })
        })
    }

    /// Builds a typed remote request step.
    ///
    /// The caller chooses the destination-bound client, so the step does not couple a reusable
    /// flow definition to a transport implementation.
    pub fn remote_request<M, C, F>(client: Arc<C>, request: F) -> DslQueryStep<S, M::Response>
    where
        M: RemoteRequest,
        C: RequestClient<M> + 'static,
        F: Fn(&S) -> M + Send + Sync + 'static,
    {
        Self::query(move |state| {
            let message = request(state);
            let client = Arc::clone(&client);
            Box::pin(async move { client.request(&message).await })
        })
    }

    /// Adds a condition that must evaluate to `true` for this step to run.
    ///
    /// Repeated calls accumulate with short-circuiting logical AND in registration order.
    pub fn only_when<F>(mut self, condition: F) -> Self
    where
        F: Fn(&S) -> bool + Send + Sync + 'static,
    {
        let condition: Condition<S> = Box::new(condition);
        self.condition = Some(match self.condition.take() {
            Some(previous) => Box::new(move |state| previous(state) && condition(state)),
            None => condition,
        });
        self
    }

    /// Continues the flow when this step's underlying action returns a non-cancellation
    /// [`CatgaError`].
    ///
    /// This only handles structured operation failures. It never catches panics, so a panic is
    /// not silently converted into a successful flow result.
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    /// Fails this step with [`ErrorCode::Validation`] when `condition` holds after a successful
    /// action.
    pub fn fail_if<F>(self, condition: F) -> Self
    where
        F: Fn(&S) -> bool + Send + Sync + 'static,
    {
        self.fail_if_with(condition, |_| {
            CatgaError::new(ErrorCode::Validation, "DSL state failure condition matched")
        })
    }

    /// Adds a caller-created structured failure when `condition` holds after a successful action.
    ///
    /// Repeated calls retain their registration order; the first matching condition supplies the
    /// returned error.
    pub fn fail_if_with<C, E>(mut self, condition: C, error: E) -> Self
    where
        C: Fn(&S) -> bool + Send + Sync + 'static,
        E: Fn(&S) -> CatgaError + Send + Sync + 'static,
    {
        let failure: StateFailure<S> =
            Box::new(move |state| condition(state).then(|| error(state)));
        self.failure = Some(match self.failure.take() {
            Some(previous) => Box::new(move |state| previous(state).or_else(|| failure(state))),
            None => failure,
        });
        self
    }

    fn into_action(self) -> Action<S> {
        let step = Arc::new(self);
        Box::new(move |state| {
            let step = Arc::clone(&step);
            Box::pin(async move {
                if step
                    .condition
                    .as_ref()
                    .is_some_and(|condition| !condition(state))
                {
                    return Ok(());
                }
                if let Err(error) = step.action.as_ref()(state).await {
                    if step.optional && error.code() != ErrorCode::Cancelled {
                        return Ok(());
                    }
                    return Err(error);
                }
                if let Some(error) = step.failure.as_ref().and_then(|failure| failure(state)) {
                    return Err(error);
                }
                Ok(())
            })
        })
    }
}

/// A typed DSL query that can validate its response before it changes flow state.
pub struct DslQueryStep<S, R> {
    query: Query<S, R>,
    condition: Option<Condition<S>>,
    failure: Option<ResponseFailure<R>>,
    optional: bool,
}

impl<S: Send + 'static, R: Send + 'static> DslQueryStep<S, R> {
    /// Adds a condition that must evaluate to `true` before this request is constructed.
    ///
    /// Repeated calls accumulate with short-circuiting logical AND in registration order.
    pub fn only_when<F>(mut self, condition: F) -> Self
    where
        F: Fn(&S) -> bool + Send + Sync + 'static,
    {
        let condition: Condition<S> = Box::new(condition);
        self.condition = Some(match self.condition.take() {
            Some(previous) => Box::new(move |state| previous(state) && condition(state)),
            None => condition,
        });
        self
    }

    /// Continues the flow when the underlying request returns a non-cancellation structured
    /// error.
    ///
    /// Like [`DslStep::optional`], this does not catch panics.
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    /// Fails with [`ErrorCode::Validation`] when `condition` matches a successful response.
    pub fn fail_if_response<F>(self, condition: F) -> Self
    where
        F: Fn(&R) -> bool + Send + Sync + 'static,
    {
        self.fail_if_response_with(condition, |_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL response failure condition matched",
            )
        })
    }

    /// Adds a caller-created structured failure when `condition` matches a successful response.
    ///
    /// Repeated calls retain their registration order; the first matching condition supplies the
    /// returned error.
    pub fn fail_if_response_with<C, E>(mut self, condition: C, error: E) -> Self
    where
        C: Fn(&R) -> bool + Send + Sync + 'static,
        E: Fn(&R) -> CatgaError + Send + Sync + 'static,
    {
        let failure: ResponseFailure<R> =
            Box::new(move |response| condition(response).then(|| error(response)));
        self.failure = Some(match self.failure.take() {
            Some(previous) => {
                Box::new(move |response| previous(response).or_else(|| failure(response)))
            }
            None => failure,
        });
        self
    }

    /// Converts this query into a state-mutating step.
    ///
    /// `set` runs only after the request succeeds and the response passes its failure condition.
    pub fn into_state<Set>(self, set: Set) -> DslStep<S>
    where
        Set: Fn(&mut S, R) + Send + Sync + 'static,
    {
        let query = Arc::new((self, set));
        DslStep::action(move |state| {
            let query = Arc::clone(&query);
            Box::pin(async move {
                let (query, set) = query.as_ref();
                if query
                    .condition
                    .as_ref()
                    .is_some_and(|condition| !condition(state))
                {
                    return Ok(());
                }
                let response = match query.query.as_ref()(state).await {
                    Ok(response) => response,
                    Err(error) if query.optional && error.code() != ErrorCode::Cancelled => {
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                if let Some(error) = query
                    .failure
                    .as_ref()
                    .and_then(|failure| failure(&response))
                {
                    return Err(error);
                }
                set(state, response);
                Ok(())
            })
        })
    }

    /// Converts this query into a step that discards a successful response.
    pub fn discard(self) -> DslStep<S> {
        self.into_state(|_, _| {})
    }
}

/// One observable outcome from a top-level [`DslFlow`] step or the flow itself.
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
/// does not control the flow result: step errors remain the result returned by [`DslFlow::run`].
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

/// Optional, async lifecycle hooks for one [`DslFlow`].
///
/// Hooks run sequentially on the caller's execution future after the configured synchronous
/// [`DslFlowLifecycleObserver`]s. A hook error is returned unchanged and stops execution; it is
/// not converted into another lifecycle event. Hooks configured on an outer flow are emitted only
/// for that flow's top-level steps.
pub struct DslFlowLifecycleHooks<S> {
    step_succeeded: Option<DslFlowStepSucceededHook<S>>,
    step_failed: Option<DslFlowStepFailedHook<S>>,
    flow_succeeded: Option<DslFlowSucceededHook<S>>,
    flow_failed: Option<DslFlowFailedHook<S>>,
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

enum Step<S> {
    Action(Action<S>),
    ForEach {
        run_all: Action<S>,
    },
    ReplayableForEach(ReplayableForEach<S>),
    StreamForEach(Action<S>),
    ConcurrentStreamForEach(Action<S>),
    Retry {
        action: Action<S>,
        max_retries: usize,
        initial_delay: Duration,
    },
    Timeout {
        action: Action<S>,
        duration: Duration,
    },
    If {
        condition: Condition<S>,
        then_branch: DslFlow<S>,
        else_branch: DslFlow<S>,
    },
    Match {
        select_branch: BranchSelector<S>,
        branches: Vec<DslFlow<S>>,
        default_branch: DslFlow<S>,
    },
    Parallel {
        branches: Vec<DslFlow<S>>,
        clone_state: CloneState<S>,
        merge: Merge<S>,
    },
    WhenAny {
        branches: Vec<DslFlow<S>>,
        clone_state: CloneState<S>,
        merge: MergeWinner<S>,
    },
}

/// Composable, process-local stateful flow with deterministic conditional branches.
///
/// `DslFlow` is appropriate for work that completes while the caller keeps the
/// future alive. [`DslFlow::run_checkpointed`] can persist completed nested conditional children,
/// replayable sequential `for_each` items, and completed `parallel` branches, but it intentionally
/// does not model durable
/// timers or external waits. Use [`crate::FlowDefinition`] together with a
/// [`crate::FlowRuntime`] when a flow must survive process restart, wait for
/// an external result, or resume at a scheduled time. Keeping those execution
/// models separate avoids hidden timer tasks and makes durable ownership
/// explicit.
pub struct DslFlow<S> {
    steps: Vec<Step<S>>,
    lifecycle_observers: Vec<Arc<dyn DslFlowLifecycleObserver>>,
    lifecycle_hooks: Option<DslFlowLifecycleHooks<S>>,
    metrics: FlowMetrics,
}

/// Shared concurrency budget for throttled flow actions.
#[derive(Clone)]
pub struct FlowThrottle {
    permits: Arc<Semaphore>,
}

impl FlowThrottle {
    /// Creates a throttle that permits at most `limit` actions at once.
    pub fn new(limit: usize) -> CatgaResult<Self> {
        if limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow throttle limit must be greater than zero",
            ));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(limit)),
        })
    }

    async fn acquire(&self) -> CatgaResult<OwnedSemaphorePermit> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Cancelled, "flow throttle is closed"))
    }
}

impl<S: Send> DslFlow<S> {
    /// Creates an empty DSL flow.
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            lifecycle_observers: Vec::new(),
            lifecycle_hooks: None,
            metrics: FlowMetrics::default(),
        }
    }

    /// Adds an event sink that observes top-level step and flow outcomes.
    ///
    /// Observers are called in registration order. This configuration is part of the reusable
    /// flow definition and does not create background tasks.
    pub fn with_lifecycle_observer<O>(mut self, observer: Arc<O>) -> Self
    where
        O: DslFlowLifecycleObserver + 'static,
    {
        let observer: Arc<dyn DslFlowLifecycleObserver> = observer;
        self.lifecycle_observers.push(observer);
        self
    }

    /// Adds asynchronous lifecycle hooks for top-level step and flow outcomes.
    ///
    /// A synchronous observer receives each event before the corresponding hook. Hooks are
    /// awaited in the execution future, so a hook error is returned unchanged and prevents later
    /// steps. In [`DslFlow::run_checkpointed`], successful-step hooks run before their completed
    /// checkpoint is persisted. A persistence failure can therefore replay the step and hook on
    /// retry; consumers must treat such successful-step effects as at-least-once.
    pub fn with_lifecycle_hooks(mut self, hooks: DslFlowLifecycleHooks<S>) -> Self {
        self.lifecycle_hooks = Some(hooks);
        self
    }

    /// Appends one composable action built with [`DslStep`].
    ///
    /// Convert a [`DslQueryStep`] with [`DslQueryStep::into_state`] or
    /// [`DslQueryStep::discard`] before passing it here. The resulting action is compiled into
    /// the same caller-owned execution path as [`Self::action`], so decorators create no tasks,
    /// locks, or separate persistence model.
    pub fn step(mut self, step: DslStep<S>) -> Self
    where
        S: 'static,
    {
        self.steps.push(Step::Action(step.into_action()));
        self
    }

    /// Appends one state-mutating asynchronous action.
    pub fn action<F>(mut self, action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.steps.push(Step::Action(Box::new(action)));
        self
    }

    /// Appends an action that retries only transient failures.
    ///
    /// `max_retries` counts attempts after the first execution. Each delay doubles from
    /// `initial_delay` and saturates at [`Duration::MAX`].
    pub fn retry<F>(mut self, max_retries: usize, initial_delay: Duration, action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.steps.push(Step::Retry {
            action: Box::new(action),
            max_retries,
            initial_delay,
        });
        self
    }

    /// Appends an action that is cancelled when it exceeds `duration`.
    pub fn timeout<F>(mut self, duration: Duration, action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.steps.push(Step::Timeout {
            action: Box::new(action),
            duration,
        });
        self
    }

    /// Appends an action that consumes one permit from a shared [`FlowThrottle`].
    pub fn throttle<F>(self, throttle: FlowThrottle, action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        let action = Arc::new(action);
        self.action(move |state| {
            let throttle = throttle.clone();
            let action = Arc::clone(&action);
            Box::pin(async move {
                let _permit = throttle.acquire().await?;
                action(state).await
            })
        })
    }

    /// Appends a typed mediator request and discards its successful response.
    ///
    /// The mediator is shared through [`Arc`] so a flow can be reused concurrently
    /// without a service locator or a mutable dispatcher.
    pub fn send<M, F>(self, mediator: Arc<Mediator>, request: F) -> Self
    where
        M: Request,
        F: Fn(&S) -> M + Send + Sync + 'static,
    {
        self.action(move |state| {
            let request = request(state);
            let mediator = Arc::clone(&mediator);
            Box::pin(async move {
                mediator.send(request).await?;
                Ok(())
            })
        })
    }

    /// Appends a typed mediator request and stores its successful response in the state.
    pub fn send_into<M, F, Set>(self, mediator: Arc<Mediator>, request: F, set: Set) -> Self
    where
        M: Request,
        F: Fn(&S) -> M + Send + Sync + 'static,
        Set: Fn(&mut S, M::Response) + Send + Sync + 'static,
    {
        let set = Arc::new(set);
        self.action(move |state| {
            let request = request(state);
            let mediator = Arc::clone(&mediator);
            let set = Arc::clone(&set);
            Box::pin(async move {
                let response = mediator.send(request).await?;
                set(state, response);
                Ok(())
            })
        })
    }

    /// Appends a typed remote request and discards its successful response.
    ///
    /// The caller provides a destination-bound client, keeping a flow independent of the
    /// selected request/reply transport and avoiding runtime service lookup.
    pub fn remote_send<M, C, F>(self, client: Arc<C>, request: F) -> Self
    where
        M: RemoteRequest,
        C: RequestClient<M> + 'static,
        F: Fn(&S) -> M + Send + Sync + 'static,
    {
        self.action(move |state| {
            let request = request(state);
            let client = Arc::clone(&client);
            Box::pin(async move {
                client.request(&request).await?;
                Ok(())
            })
        })
    }

    /// Appends a typed remote request and stores its successful response in the state.
    pub fn remote_send_into<M, C, F, Set>(self, client: Arc<C>, request: F, set: Set) -> Self
    where
        M: RemoteRequest,
        C: RequestClient<M> + 'static,
        F: Fn(&S) -> M + Send + Sync + 'static,
        Set: Fn(&mut S, M::Response) + Send + Sync + 'static,
    {
        let set = Arc::new(set);
        self.action(move |state| {
            let request = request(state);
            let client = Arc::clone(&client);
            let set = Arc::clone(&set);
            Box::pin(async move {
                let response = client.request(&request).await?;
                set(state, response);
                Ok(())
            })
        })
    }

    /// Appends a typed mediator event publication.
    pub fn publish<E, F>(self, mediator: Arc<Mediator>, event: F) -> Self
    where
        E: Event,
        F: Fn(&S) -> E + Send + Sync + 'static,
    {
        self.action(move |state| {
            let event = event(state);
            let mediator = Arc::clone(&mediator);
            Box::pin(async move { mediator.publish(event).await })
        })
    }

    /// Appends a branch that runs exactly one nested flow against the same state.
    pub fn if_else<C>(mut self, condition: C, then_branch: Self, else_branch: Self) -> Self
    where
        C: Fn(&S) -> bool + Send + Sync + 'static,
    {
        self.steps.push(Step::If {
            condition: Box::new(condition),
            then_branch,
            else_branch,
        });
        self
    }

    /// Appends an equality-based branch with one default flow for unmatched values.
    ///
    /// Duplicate case values keep the last registered branch, matching map insertion
    /// semantics while retaining O(1) branch lookup during execution.
    pub fn match_on<V, I, F>(mut self, selector: F, cases: I, default_branch: Self) -> Self
    where
        V: Eq + Hash + Send + Sync + 'static,
        I: IntoIterator<Item = (V, Self)>,
        F: Fn(&S) -> V + Send + Sync + 'static,
    {
        let mut positions = HashMap::new();
        let mut branches = Vec::new();
        for (value, branch) in cases {
            match positions.entry(value) {
                Entry::Occupied(position) => branches[*position.get()] = branch,
                Entry::Vacant(position) => {
                    let index = branches.len();
                    branches.push(branch);
                    position.insert(index);
                }
            }
        }
        self.steps.push(Step::Match {
            select_branch: Box::new(move |state| positions.get(&selector(state)).copied()),
            branches,
            default_branch,
        });
        self
    }

    /// Appends branches that run concurrently on isolated state copies.
    ///
    /// The merge closure receives branch states in declaration order only after every branch
    /// succeeds. A failed branch leaves the original state unchanged.
    pub fn parallel<I, M>(mut self, branches: I, merge: M) -> Self
    where
        S: Clone,
        I: IntoIterator<Item = Self>,
        M: Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.steps.push(Step::Parallel {
            branches: Self::collect_parallel_branches(branches),
            clone_state: Clone::clone,
            merge: Box::new(merge),
        });
        self
    }

    /// Appends branches that all complete before their isolated states are merged.
    ///
    /// This names the request-oriented `WhenAll` operation directly while preserving the full
    /// branch semantics of [`DslFlow::parallel`].
    pub fn when_all<I, M>(self, branches: I, merge: M) -> Self
    where
        S: Clone,
        I: IntoIterator<Item = Self>,
        M: Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.parallel(branches, merge)
    }

    /// Appends branches that run concurrently until the first branch succeeds.
    ///
    /// Every branch starts with an isolated state copy. Failed branches are ignored while another
    /// branch remains pending; if every branch fails, the last completed structured error is
    /// returned. The winning state is merged only after a successful branch, and unfinished
    /// cooperative futures are dropped without spawning tasks.
    pub fn when_any<I, M>(mut self, branches: I, merge: M) -> Self
    where
        S: Clone,
        I: IntoIterator<Item = Self>,
        M: Fn(&mut S, S) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.steps.push(Step::WhenAny {
            branches: Self::collect_parallel_branches(branches),
            clone_state: Clone::clone,
            merge: Box::new(merge),
        });
        self
    }

    fn collect_parallel_branches<I>(branches: I) -> Vec<Self>
    where
        I: IntoIterator<Item = Self>,
    {
        branches
            .into_iter()
            .take(MAX_DSL_PARALLEL_BRANCHES.saturating_add(1))
            .collect()
    }

    /// Appends an action that runs sequentially for every item selected from the state.
    ///
    /// The selector returns an owned collection before item actions begin, so no immutable state
    /// borrow remains while an action mutates the state asynchronously. Each item emits
    /// `catga.flow.foreach.items.processed` or `.failed` and an item-duration histogram; the
    /// complete operation emits `catga.flow.foreach.duration`. Metrics use only a static `mode`
    /// label, never a flow identity or item value. [`DslFlow::run_checkpointed`] rejects this
    /// generic operation with [`ErrorCode::Validation`]; use
    /// [`DslFlow::for_each_replayable`] for checkpointed execution.
    pub fn for_each<T, Select, F>(mut self, select: Select, action: F) -> Self
    where
        T: Send + 'static,
        Select: Fn(&S) -> Vec<T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        self.steps.push(Step::ForEach {
            run_all: Box::new({
                let select = Arc::clone(&select);
                let action = Arc::clone(&action);
                move |state| {
                    let select = Arc::clone(&select);
                    let action = Arc::clone(&action);
                    Box::pin(async move {
                        let metrics = ForEachMetrics::new("sequential");
                        for item in select(state) {
                            let item_metrics = metrics.begin_item();
                            match action(state, item).await {
                                Ok(()) => item_metrics.complete(true),
                                Err(error) => {
                                    item_metrics.complete(false);
                                    return Err(error);
                                }
                            }
                        }
                        Ok(())
                    })
                }
            }),
        });
        self
    }

    /// Appends a sequential action that continues after an item failure.
    ///
    /// `on_error` is mandatory: it receives the zero-based item index and the original
    /// [`CatgaError`] while the state is exclusively borrowed. This makes every ignored item an
    /// explicit application decision, without retaining an unbounded error collection in the
    /// flow. If the callback fails, execution stops immediately and later items are not started.
    ///
    /// Like [`DslFlow::for_each`], this process-local operation is rejected by
    /// [`DslFlow::run_checkpointed`]. Use
    /// [`DslFlow::for_each_replayable_continue_on_error`] when the item cursor and callback state
    /// must survive restart.
    pub fn for_each_continue_on_error<T, Select, F, OnError>(
        mut self,
        select: Select,
        action: F,
        on_error: OnError,
    ) -> Self
    where
        T: Send + 'static,
        Select: Fn(&S) -> Vec<T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
        OnError: for<'a> Fn(&'a mut S, usize, CatgaError) -> BoxFuture<'a, CatgaResult<()>>
            + Send
            + Sync
            + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        let on_error = Arc::new(on_error);
        self.steps.push(Step::ForEach {
            run_all: Box::new(move |state| {
                let select = Arc::clone(&select);
                let action = Arc::clone(&action);
                let on_error = Arc::clone(&on_error);
                Box::pin(async move {
                    let metrics = ForEachMetrics::new("sequential");
                    for (index, item) in select(state).into_iter().enumerate() {
                        let item_metrics = metrics.begin_item();
                        match action(state, item).await {
                            Ok(()) => item_metrics.complete(true),
                            Err(error) => {
                                item_metrics.complete(false);
                                on_error(state, index, error).await?;
                            }
                        }
                    }
                    Ok(())
                })
            }),
        });
        self
    }

    /// Appends a sequential action whose selected items can be safely checkpointed.
    ///
    /// The selector runs once when the operation begins. Its owned items are serialized and saved
    /// with the item cursor before each subsequent item starts, so recovery uses the original
    /// selection even when earlier item actions mutate the state. Serialized item count and size
    /// are bounded; selections that exceed the checkpoint limit return [`ErrorCode::Validation`].
    pub fn for_each_replayable<T, Select, F>(mut self, select: Select, action: F) -> Self
    where
        T: MemoryPackSerialize + MemoryPackDeserialize + Send + 'static,
        Select: Fn(&S) -> Vec<T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        self.steps.push(Step::ReplayableForEach(ReplayableForEach {
            select: Box::new(move |state| {
                select(state)
                    .into_iter()
                    .map(|item| {
                        MemoryPackSerializer::serialize(&item).map_err(|_| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "replayable for_each item cannot be encoded",
                            )
                        })
                    })
                    .collect()
            }),
            action: Box::new(move |state, bytes| {
                let item = MemoryPackSerializer::deserialize(bytes).map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "replayable for_each checkpoint item is invalid",
                    )
                });
                let action = Arc::clone(&action);
                Box::pin(async move { action(state, item?).await })
            }),
            on_error: None,
        }));
        self
    }

    /// Appends a restart-safe sequential action that continues after an item failure.
    ///
    /// `on_error` receives the zero-based index and original error while it exclusively owns the
    /// flow state. It must record, compensate, or reject the failure explicitly. After either a
    /// successful item or successful error callback, the encoded state and next cursor are saved
    /// in one checkpoint update. If the callback fails, that item remains the current cursor and
    /// no later item starts.
    pub fn for_each_replayable_continue_on_error<T, Select, F, OnError>(
        mut self,
        select: Select,
        action: F,
        on_error: OnError,
    ) -> Self
    where
        T: MemoryPackSerialize + MemoryPackDeserialize + Send + 'static,
        Select: Fn(&S) -> Vec<T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
        OnError: for<'a> Fn(&'a mut S, usize, CatgaError) -> BoxFuture<'a, CatgaResult<()>>
            + Send
            + Sync
            + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        self.steps.push(Step::ReplayableForEach(ReplayableForEach {
            select: Box::new(move |state| {
                select(state)
                    .into_iter()
                    .map(|item| {
                        MemoryPackSerializer::serialize(&item).map_err(|_| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "replayable for_each item cannot be encoded",
                            )
                        })
                    })
                    .collect()
            }),
            action: Box::new(move |state, bytes| {
                let item = MemoryPackSerializer::deserialize(bytes).map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "replayable for_each checkpoint item is invalid",
                    )
                });
                let action = Arc::clone(&action);
                Box::pin(async move { action(state, item?).await })
            }),
            on_error: Some(Box::new(on_error)),
        }));
        self
    }

    /// Appends a sequential action over an owned, lazily polled item stream.
    ///
    /// Unlike [`DslFlow::for_each`], this selector returns a `'static` stream so the immutable
    /// state borrow ends before each item action mutates the state. At most one item is retained
    /// at a time, and an item error stops polling immediately. This operation is rejected with
    /// [`ErrorCode::Validation`] by [`DslFlow::run_checkpointed`] because a generic stream has no
    /// replay cursor.
    pub fn for_each_stream<T, Select, F>(mut self, select: Select, action: F) -> Self
    where
        T: Send + 'static,
        Select: Fn(&S) -> futures::stream::BoxStream<'static, T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        self.steps.push(Step::StreamForEach(Box::new(move |state| {
            let select = Arc::clone(&select);
            let action = Arc::clone(&action);
            Box::pin(async move {
                let metrics = ForEachMetrics::new("stream");
                let mut items = select(state);
                while let Some(item) = items.next().await {
                    let item_metrics = metrics.begin_item();
                    match action(state, item).await {
                        Ok(()) => item_metrics.complete(true),
                        Err(error) => {
                            item_metrics.complete(false);
                            return Err(error);
                        }
                    }
                }
                Ok(())
            })
        })));
        self
    }

    /// Runs a lazily selected stream in bounded concurrent batches and reduces each batch in
    /// source order.
    ///
    /// At most `limit` stream items, work futures, and completed values are retained at once.
    /// Work observes immutable state, so each completed batch is drained before `reduce` receives
    /// mutable state; this preserves Rust's aliasing rules without locks or an unbounded result
    /// collection. The operation is process-local and is rejected by [`DslFlow::run_checkpointed`]
    /// because a generic stream has no durable replay cursor.
    pub fn for_each_stream_concurrent<T, R, Select, Work, Reduce>(
        mut self,
        limit: usize,
        select: Select,
        work: Work,
        reduce: Reduce,
    ) -> CatgaResult<Self>
    where
        S: Sync,
        T: Send + 'static,
        R: Send + 'static,
        Select: Fn(&S) -> futures::stream::BoxStream<'static, T> + Send + Sync + 'static,
        Work: for<'a> Fn(&'a S, T) -> BoxFuture<'a, CatgaResult<R>> + Send + Sync + 'static,
        Reduce: Fn(&mut S, R) -> CatgaResult<()> + Send + Sync + 'static,
    {
        if limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "concurrent stream for_each limit must be greater than zero",
            ));
        }
        let select = Arc::new(select);
        let work = Arc::new(work);
        let reduce = Arc::new(reduce);
        self.steps
            .push(Step::ConcurrentStreamForEach(Box::new(move |state| {
                let select = Arc::clone(&select);
                let work = Arc::clone(&work);
                let reduce = Arc::clone(&reduce);
                Box::pin(async move {
                    let metrics = ForEachMetrics::new("concurrent_stream");
                    let mut items = select(state);
                    let mut next_index = 0_usize;
                    loop {
                        let mut completed = {
                            let state: &S = state;
                            let mut batch = FuturesUnordered::new();
                            for _ in 0..limit {
                                let Some(item) = items.next().await else {
                                    break;
                                };
                                let index = next_index;
                                next_index = next_index.checked_add(1).ok_or_else(|| {
                                    CatgaError::new(
                                        ErrorCode::Internal,
                                        "concurrent stream item index exceeds usize",
                                    )
                                })?;
                                let work = Arc::clone(&work);
                                let metrics = Arc::clone(&metrics);
                                batch.push(async move {
                                    let item_metrics = metrics.begin_item();
                                    match work(state, item).await {
                                        Ok(result) => {
                                            item_metrics.complete(true);
                                            Ok((index, result))
                                        }
                                        Err(error) => {
                                            item_metrics.complete(false);
                                            Err(error)
                                        }
                                    }
                                });
                            }
                            let mut results = Vec::with_capacity(batch.len());
                            while let Some(result) = batch.next().await {
                                results.push(result?);
                            }
                            results
                        };
                        if completed.is_empty() {
                            break;
                        }
                        completed.sort_unstable_by_key(|(index, _)| *index);
                        for (_, result) in completed {
                            reduce(state, result)?;
                        }
                    }
                    Ok(())
                })
            })));
        Ok(self)
    }

    /// Runs all selected steps against one mutable state value.
    ///
    /// The caller-owned future emits the standard bounded Flow counters, active gauge, execution
    /// and top-level-step duration histograms, and `tracing` spans. The DSL has no stable durable
    /// identity, so metrics carry no flow ID or user-defined label; the trace uses the static
    /// `dsl` flow type to avoid unbounded metric cardinality.
    pub fn run<'a>(&'a self, state: &'a mut S) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            self.metrics.record_started();
            let mut execution = self.metrics.begin_execution("", "dsl");
            for (step_index, step) in self.steps.iter().enumerate() {
                let mut step_execution = execution.begin_step("dsl");
                let result = self
                    .run_step(state, step)
                    .instrument(step_execution.span())
                    .await;
                step_execution.complete(if result.is_ok() { "success" } else { "failure" });
                match result {
                    Ok(()) => {
                        if let Err(error) = self.notify_step_succeeded(state, step_index).await {
                            self.complete_dsl_execution(&mut execution, &error);
                            return Err(error);
                        }
                    }
                    Err(error) => {
                        if let Err(hook_error) =
                            self.notify_step_failed(state, step_index, &error).await
                        {
                            self.complete_dsl_execution(&mut execution, &hook_error);
                            return Err(hook_error);
                        }
                        if let Err(hook_error) = self.notify_flow_failed(state, &error).await {
                            self.complete_dsl_execution(&mut execution, &hook_error);
                            return Err(hook_error);
                        }
                        self.complete_dsl_execution(&mut execution, &error);
                        return Err(error);
                    }
                }
            }
            if let Err(error) = self.notify_flow_succeeded(state).await {
                self.complete_dsl_execution(&mut execution, &error);
                return Err(error);
            }
            metrics::counter!(FLOWS_COMPLETED).increment(1);
            execution.complete("success");
            Ok(())
        })
    }

    fn complete_dsl_execution(
        &self,
        execution: &mut crate::metrics::FlowExecution,
        error: &CatgaError,
    ) {
        let outcome = if error.code() == ErrorCode::Cancelled {
            "cancelled"
        } else {
            metrics::counter!(FLOWS_FAILED).increment(1);
            "failure"
        };
        execution.complete(outcome);
    }

    fn notify(&self, event: DslFlowLifecycleEvent) {
        for observer in &self.lifecycle_observers {
            observer.observe(&event);
        }
    }

    fn notify_step_succeeded<'a>(
        &'a self,
        state: &'a mut S,
        step_index: usize,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            self.notify(DslFlowLifecycleEvent::StepSucceeded { step_index });
            if let Some(hooks) = &self.lifecycle_hooks
                && let Some(hook) = &hooks.step_succeeded
            {
                hook(&*state, step_index).await?;
            }
            Ok(())
        })
    }

    fn notify_step_failed<'a>(
        &'a self,
        state: &'a mut S,
        step_index: usize,
        error: &'a CatgaError,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            self.notify(DslFlowLifecycleEvent::StepFailed {
                step_index,
                error: error.clone(),
            });
            if let Some(hooks) = &self.lifecycle_hooks
                && let Some(hook) = &hooks.step_failed
            {
                hook(&*state, step_index, error).await?;
            }
            Ok(())
        })
    }

    fn notify_flow_succeeded<'a>(&'a self, state: &'a mut S) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            self.notify(DslFlowLifecycleEvent::FlowSucceeded);
            if let Some(hooks) = &self.lifecycle_hooks
                && let Some(hook) = &hooks.flow_succeeded
            {
                hook(&*state).await?;
            }
            Ok(())
        })
    }

    fn notify_flow_failed<'a>(
        &'a self,
        state: &'a mut S,
        error: &'a CatgaError,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            self.notify(DslFlowLifecycleEvent::FlowFailed {
                error: error.clone(),
            });
            if let Some(hooks) = &self.lifecycle_hooks
                && let Some(hook) = &hooks.flow_failed
            {
                hook(&*state, error).await?;
            }
            Ok(())
        })
    }

    /// Runs a named flow from its latest durable checkpoint.
    ///
    /// The caller must hold an exclusive flow lease. Successful nested `if_else` and `match_on`
    /// children, [`DslFlow::for_each_replayable`] items, and completed `parallel` branches are
    /// checkpointed with a bounded, versioned cursor, so recovery does not replay them. Generic
    /// collection and stream selectors return [`ErrorCode::Validation`] because they have no
    /// persisted replay cursor.
    /// Other top-level steps remain one recovery unit. This is checkpointed local
    /// execution, not a durable scheduler: use
    /// [`crate::FlowDefinition`] and [`crate::FlowStepOutcome::delay`] or
    /// [`crate::FlowStepOutcome::suspend_until`] for restart-safe delay and
    /// schedule-at behavior. A successful run writes one bounded terminal state before
    /// announcing [`DslFlowLifecycleEvent::FlowSucceeded`]; later invocations with the same
    /// `flow_id` restore that state without replaying steps or lifecycle hooks. Failed runs keep
    /// their existing recovery behavior so transient step failures can be retried. Each call
    /// emits the same bounded Flow metrics and trace spans as [`Self::run`], while `flow_id`
    /// remains trace-only rather than becoming a metric label.
    pub async fn run_checkpointed<C, P>(
        &self,
        flow_id: &str,
        initial: S,
        progress: &P,
        codec: &C,
    ) -> CatgaResult<S>
    where
        C: DslStateCodec<S>,
        P: DslStepProgressStore + ?Sized,
    {
        self.metrics.record_started();
        let mut execution = self.metrics.begin_execution(flow_id, "dsl_checkpointed");
        let result = self
            .run_checkpointed_inner(flow_id, initial, progress, codec, &mut execution)
            .await;
        match &result {
            Ok(_) => {
                metrics::counter!(FLOWS_COMPLETED).increment(1);
                execution.complete("success");
            }
            Err(error) => self.complete_dsl_execution(&mut execution, error),
        }
        result
    }

    async fn run_checkpointed_inner<C, P>(
        &self,
        flow_id: &str,
        mut initial: S,
        progress: &P,
        codec: &C,
        execution: &mut crate::metrics::FlowExecution,
    ) -> CatgaResult<S>
    where
        C: DslStateCodec<S>,
        P: DslStepProgressStore + ?Sized,
    {
        if let Some(terminal) = load_checkpoint_terminal(flow_id, progress).await? {
            return terminal_result(terminal, codec);
        }
        let mut start = 0;
        let mut cursor = None;
        for index in (0..self.steps.len()).rev() {
            let step = top_level_step_index(index)?;
            if let Some(saved) = progress.get(flow_id, step).await? {
                if saved.kind() == DslProgressKind::CheckpointFrame {
                    let frame = CheckpointFrame::decode(saved.payload())?.ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint frame record has no internal frame payload",
                        )
                    })?;
                    initial = codec.decode(&frame.state)?;
                    start = index;
                    cursor = Some(frame);
                } else {
                    initial = codec.decode(saved.payload())?;
                    start = index.saturating_add(1);
                }
                break;
            }
        }
        for (index, step) in self.steps.iter().enumerate().skip(start) {
            let step_index = top_level_step_index(index)?;
            let step_cursor = if index == start { cursor.take() } else { None };
            let context = CheckpointContext {
                flow_id,
                top_level_step: step_index,
                progress,
                codec,
            };
            let mut step_execution = execution.begin_step("dsl");
            let result = self
                .run_checkpointed_step(&mut initial, step, step_cursor, &context)
                .instrument(step_execution.span())
                .await;
            step_execution.complete(if result.is_ok() { "success" } else { "failure" });
            match result {
                Ok(()) => self.notify_step_succeeded(&mut initial, index).await?,
                Err(error) => {
                    self.notify_step_failed(&mut initial, index, &error).await?;
                    self.notify_flow_failed(&mut initial, &error).await?;
                    return Err(error);
                }
            }
            persist_completed_checkpoint(&initial, &context).await?;
        }
        let (terminal, created) = persist_checkpoint_terminal(
            flow_id,
            progress,
            CheckpointTerminal(codec.encode(&initial)?),
        )
        .await?;
        if !created {
            return terminal_result(terminal, codec);
        }
        self.notify_flow_succeeded(&mut initial).await?;
        Ok(initial)
    }

    fn run_checkpointed_step<'a, C, P>(
        &'a self,
        state: &'a mut S,
        step: &'a Step<S>,
        cursor: Option<CheckpointFrame>,
        context: &'a CheckpointContext<'a, C, P>,
    ) -> BoxFuture<'a, CatgaResult<()>>
    where
        C: DslStateCodec<S> + 'a,
        P: DslStepProgressStore + ?Sized + 'a,
    {
        Box::pin(async move {
            match step {
                Step::StreamForEach(_) => {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "checkpointed for_each_stream requires a replay cursor",
                    ));
                }
                Step::ConcurrentStreamForEach(_) => {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "checkpointed concurrent stream for_each requires a replay cursor",
                    ));
                }
                Step::ForEach { .. } => {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "checkpointed for_each requires for_each_replayable",
                    ));
                }
                Step::ReplayableForEach(operation) => {
                    let levels = cursor
                        .as_ref()
                        .map_or_else(Vec::new, |frame| frame.levels.clone());
                    return self
                        .run_checkpointed_replayable_for_each(
                            state,
                            operation,
                            cursor.as_ref().map(|frame| frame.work.clone()),
                            &levels,
                            context,
                        )
                        .await;
                }
                Step::Parallel {
                    branches,
                    clone_state,
                    merge,
                } => {
                    return run_checkpointed_parallel(
                        state,
                        branches,
                        clone_state,
                        merge,
                        cursor.as_ref().map(|frame| frame.work.clone()),
                        cursor.as_ref().map_or(&[], |frame| frame.levels.as_slice()),
                        context,
                    )
                    .await;
                }
                Step::WhenAny {
                    branches,
                    clone_state,
                    merge,
                } => {
                    return run_checkpointed_when_any(
                        state,
                        branches,
                        clone_state,
                        merge,
                        cursor.as_ref().map(|frame| frame.work.clone()),
                        cursor.as_ref().map_or(&[], |frame| frame.levels.as_slice()),
                        context,
                    )
                    .await;
                }
                _ => {}
            }
            let Some((branch, branch_code)) = self.selected_checkpoint_branch(
                state,
                step,
                cursor.as_ref().map(|frame| frame.levels.as_slice()),
                0,
            )?
            else {
                return self.run_step(state, step).await;
            };
            let (mut levels, work) = match cursor {
                Some(frame) => (frame.levels, Some(frame.work)),
                None => (Vec::new(), None),
            };
            if levels.is_empty() {
                levels.push(CheckpointLevel {
                    branch: branch_code,
                    next_step: 0,
                });
            }
            self.run_checkpointed_branch(state, branch, &mut levels, 0, work, context)
                .await
        })
    }

    fn run_checkpointed_replayable_for_each<'a, C, P>(
        &'a self,
        state: &'a mut S,
        operation: &'a ReplayableForEach<S>,
        work: Option<CheckpointWork>,
        levels: &'a [CheckpointLevel],
        context: &'a CheckpointContext<'a, C, P>,
    ) -> BoxFuture<'a, CatgaResult<()>>
    where
        C: DslStateCodec<S> + 'a,
        P: DslStepProgressStore + ?Sized + 'a,
    {
        Box::pin(async move {
            let (mut next_index, items) = match work {
                Some(CheckpointWork::ReplayableForEach { next_index, items }) => {
                    (next_index, items)
                }
                Some(_) => {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint cursor does not describe a replayable for_each step",
                    ));
                }
                None => (0, (operation.select)(state)?),
            };
            validate_replayable_for_each_items(&items)?;
            let total = u32::try_from(items.len()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "replayable for_each item count exceeds u32",
                )
            })?;
            if next_index > total {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "replayable for_each cursor is outside its saved items",
                ));
            }
            let metrics = ForEachMetrics::new("sequential");
            while next_index < total {
                let item_metrics = metrics.begin_item();
                let index = usize::try_from(next_index).map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "replayable for_each item index exceeds usize",
                    )
                })?;
                let item = items.get(index).ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "replayable for_each cursor is outside its saved items",
                    )
                })?;
                match (operation.action)(state, item).await {
                    Ok(()) => item_metrics.complete(true),
                    Err(error) => {
                        item_metrics.complete(false);
                        let Some(on_error) = operation.on_error.as_deref() else {
                            return Err(error);
                        };
                        on_error(state, index, error).await?;
                    }
                }
                next_index = next_index.checked_add(1).ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "replayable for_each item index exceeds u32",
                    )
                })?;
                let payload = CheckpointFrame::encode(
                    levels,
                    context.codec.encode(state)?,
                    CheckpointWork::ReplayableForEach {
                        next_index,
                        items: items.clone(),
                    },
                )?;
                persist_checkpoint_payload(context, payload, true).await?;
            }
            Ok(())
        })
    }

    fn run_checkpointed_branch<'a, C, P>(
        &'a self,
        state: &'a mut S,
        branch: &'a DslFlow<S>,
        levels: &'a mut Vec<CheckpointLevel>,
        depth: usize,
        work: Option<CheckpointWork>,
        context: &'a CheckpointContext<'a, C, P>,
    ) -> BoxFuture<'a, CatgaResult<()>>
    where
        C: DslStateCodec<S> + 'a,
        P: DslStepProgressStore + ?Sized + 'a,
    {
        Box::pin(async move {
            let mut work = work;
            let level = levels.get(depth).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint cursor is missing a branch level",
                )
            })?;
            let start = usize::try_from(level.next_step).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint step index is too large",
                )
            })?;
            if start > branch.steps.len() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint step index is outside its branch",
                ));
            }
            for (index, child) in branch.steps.iter().enumerate().skip(start) {
                let child_index = u32::try_from(index).map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint child index exceeds u32",
                    )
                })?;
                match child {
                    Step::ForEach { .. } => {
                        return Err(CatgaError::new(
                            ErrorCode::Validation,
                            "checkpointed for_each requires for_each_replayable",
                        ));
                    }
                    Step::ReplayableForEach(operation) => {
                        levels[depth].next_step = child_index;
                        levels.truncate(depth.checked_add(1).ok_or_else(|| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "DSL checkpoint path exceeds the maximum depth",
                            )
                        })?);
                        self.run_checkpointed_replayable_for_each(
                            state,
                            operation,
                            work.take(),
                            levels.as_slice(),
                            context,
                        )
                        .await?;
                        continue;
                    }
                    Step::Parallel {
                        branches,
                        clone_state,
                        merge,
                    } => {
                        levels[depth].next_step = child_index;
                        levels.truncate(depth.checked_add(1).ok_or_else(|| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "DSL checkpoint path exceeds the maximum depth",
                            )
                        })?);
                        run_checkpointed_parallel(
                            state,
                            branches,
                            clone_state,
                            merge,
                            work.take(),
                            levels.as_slice(),
                            context,
                        )
                        .await?;
                        continue;
                    }
                    Step::WhenAny {
                        branches,
                        clone_state,
                        merge,
                    } => {
                        levels[depth].next_step = child_index;
                        levels.truncate(depth.checked_add(1).ok_or_else(|| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "DSL checkpoint path exceeds the maximum depth",
                            )
                        })?);
                        run_checkpointed_when_any(
                            state,
                            branches,
                            clone_state,
                            merge,
                            work.take(),
                            levels.as_slice(),
                            context,
                        )
                        .await?;
                        continue;
                    }
                    Step::StreamForEach(_) | Step::ConcurrentStreamForEach(_) => {
                        return Err(CatgaError::new(
                            ErrorCode::Validation,
                            "checkpointed nested foreach operation has no replay cursor",
                        ));
                    }
                    _ => {}
                }
                if let Some((nested_branch, nested_code)) = self.selected_checkpoint_branch(
                    state,
                    child,
                    Some(levels.as_slice()),
                    depth.checked_add(1).ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint path exceeds the maximum depth",
                        )
                    })?,
                )? {
                    let next_depth = depth.checked_add(1).ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint path exceeds the maximum depth",
                        )
                    })?;
                    if next_depth >= MAX_CHECKPOINT_PATH_DEPTH {
                        return Err(CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint path exceeds the maximum depth",
                        ));
                    }
                    let nested_next_step =
                        levels.get(next_depth).map_or(0, |level| level.next_step);
                    levels[depth].next_step = child_index;
                    levels.truncate(next_depth);
                    levels.push(CheckpointLevel {
                        branch: nested_code,
                        next_step: nested_next_step,
                    });
                    self.run_checkpointed_branch(
                        state,
                        nested_branch,
                        levels,
                        next_depth,
                        work.take(),
                        context,
                    )
                    .await?;
                } else {
                    self.run_step(state, child).await?;
                    levels[depth].next_step = child_index.checked_add(1).ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint child index exceeds u32",
                        )
                    })?;
                    levels.truncate(depth.checked_add(1).ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint path exceeds the maximum depth",
                        )
                    })?);
                    let payload = self.nested_checkpoint_payload(state, context.codec, levels)?;
                    persist_checkpoint_payload(context, payload, true).await?;
                }
            }
            Ok(())
        })
    }

    fn selected_checkpoint_branch<'a>(
        &self,
        state: &S,
        step: &'a Step<S>,
        cursor: Option<&[CheckpointLevel]>,
        depth: usize,
    ) -> CatgaResult<Option<(&'a DslFlow<S>, u32)>> {
        let saved_branch = cursor
            .and_then(|levels| levels.get(depth))
            .map(|level| level.branch);
        match step {
            Step::If {
                condition,
                then_branch,
                else_branch,
            } => {
                let branch = saved_branch.unwrap_or_else(|| u32::from(!condition(state)));
                match branch {
                    0 => Ok(Some((then_branch, 0))),
                    1 => Ok(Some((else_branch, 1))),
                    _ => Err(CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint selected an invalid if branch",
                    )),
                }
            }
            Step::Match {
                select_branch,
                branches,
                default_branch,
            } => {
                let branch = match saved_branch {
                    Some(branch) => branch,
                    None => match select_branch(state) {
                        Some(index) => u32::try_from(index).map_err(|_| {
                            CatgaError::new(
                                ErrorCode::Validation,
                                "DSL checkpoint match branch index exceeds u32",
                            )
                        })?,
                        None => DEFAULT_BRANCH,
                    },
                };
                if branch == DEFAULT_BRANCH {
                    return Ok(Some((default_branch, DEFAULT_BRANCH)));
                }
                let index = usize::try_from(branch).map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint match branch is too large",
                    )
                })?;
                branches
                    .get(index)
                    .map(|selected| (selected, branch))
                    .map(Some)
                    .ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint selected an invalid match branch",
                        )
                    })
            }
            _ => Ok(None),
        }
    }

    fn nested_checkpoint_payload<C>(
        &self,
        state: &S,
        codec: &C,
        levels: &[CheckpointLevel],
    ) -> CatgaResult<Vec<u8>>
    where
        C: DslStateCodec<S>,
    {
        CheckpointFrame::encode(levels, codec.encode(state)?, CheckpointWork::Branch)
    }

    fn run_step<'a>(
        &'a self,
        state: &'a mut S,
        step: &'a Step<S>,
    ) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            match step {
                Step::Action(action) => action(state).await,
                Step::ForEach { run_all, .. } => run_all(state).await,
                Step::ReplayableForEach(operation) => {
                    let metrics = ForEachMetrics::new("sequential");
                    for (index, item) in (operation.select)(state)?.into_iter().enumerate() {
                        let item_metrics = metrics.begin_item();
                        match (operation.action)(state, &item).await {
                            Ok(()) => item_metrics.complete(true),
                            Err(error) => {
                                item_metrics.complete(false);
                                let Some(on_error) = operation.on_error.as_deref() else {
                                    return Err(error);
                                };
                                on_error(state, index, error).await?;
                            }
                        }
                    }
                    Ok(())
                }
                Step::StreamForEach(action) => action(state).await,
                Step::ConcurrentStreamForEach(action) => action(state).await,
                Step::Retry {
                    action,
                    max_retries,
                    initial_delay,
                } => {
                    let mut retry = 0;
                    loop {
                        match action(state).await {
                            Ok(()) => return Ok(()),
                            Err(error)
                                if error.code() == ErrorCode::Transient && retry < *max_retries =>
                            {
                                let delay = retry_delay(*initial_delay, retry);
                                retry = retry.saturating_add(1);
                                if !delay.is_zero() {
                                    tokio::time::sleep(delay).await;
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                Step::Timeout { action, duration } => {
                    tokio::time::timeout(*duration, action(state))
                        .await
                        .map_err(|_| {
                            CatgaError::new(ErrorCode::Timeout, "flow action timed out")
                        })??;
                    Ok(())
                }
                Step::If {
                    condition,
                    then_branch,
                    else_branch,
                } => {
                    if condition(state) {
                        then_branch.run(state).await?;
                    } else {
                        else_branch.run(state).await?;
                    }
                    Ok(())
                }
                Step::Match {
                    select_branch,
                    branches,
                    default_branch,
                } => {
                    if let Some(branch) = select_branch(state).and_then(|index| branches.get(index))
                    {
                        branch.run(state).await?;
                    } else {
                        default_branch.run(state).await?;
                    }
                    Ok(())
                }
                Step::Parallel {
                    branches,
                    clone_state,
                    merge,
                } => {
                    validate_parallel_branch_count(branches.len())?;
                    let mut branch_states = branches
                        .iter()
                        .map(|_| clone_state(state))
                        .collect::<Vec<_>>();
                    let results = futures::future::join_all(
                        branches
                            .iter()
                            .zip(branch_states.iter_mut())
                            .map(|(branch, branch_state)| branch.run(branch_state)),
                    )
                    .await;

                    for result in results {
                        result?;
                    }
                    merge(state, branch_states)
                }
                Step::WhenAny {
                    branches,
                    clone_state,
                    merge,
                } => {
                    validate_parallel_branch_count(branches.len())?;
                    let mut pending = FuturesUnordered::new();
                    for branch in branches {
                        let mut branch_state = clone_state(state);
                        pending.push(async move {
                            let result = branch.run(&mut branch_state).await;
                            (branch_state, result)
                        });
                    }
                    let mut last_error = None;
                    while let Some((winner, result)) = pending.next().await {
                        match result {
                            Ok(()) => return merge(state, winner),
                            Err(error) => last_error = Some(error),
                        }
                    }
                    match last_error {
                        Some(error) => Err(error),
                        None => Ok(()),
                    }
                }
            }
        })
    }
}

async fn load_checkpoint_terminal<P>(
    flow_id: &str,
    progress: &P,
) -> CatgaResult<Option<CheckpointTerminal>>
where
    P: DslStepProgressStore + ?Sized,
{
    progress
        .get(flow_id, DSL_TERMINAL_STEP_INDEX)
        .await?
        .map(|progress| {
            if progress.kind() != DslProgressKind::Terminal {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "DSL terminal progress slot is not a terminal record",
                ));
            }
            MemoryPackSerializer::deserialize(progress.payload()).map_err(|error| {
                CatgaError::new(ErrorCode::Validation, "DSL terminal record is invalid")
                    .with_details(error.to_string())
            })
        })
        .transpose()
}

async fn persist_checkpoint_terminal<P>(
    flow_id: &str,
    progress: &P,
    terminal: CheckpointTerminal,
) -> CatgaResult<(CheckpointTerminal, bool)>
where
    P: DslStepProgressStore + ?Sized,
{
    let payload = MemoryPackSerializer::serialize(&terminal).map_err(|error| {
        CatgaError::new(ErrorCode::Internal, "encode DSL terminal record")
            .with_details(error.to_string())
    })?;
    if payload.len() > MAX_DSL_TERMINAL_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL terminal record exceeds the size limit",
        ));
    }
    let marker = DslStepProgress::new(flow_id, DSL_TERMINAL_STEP_INDEX, []).terminal(payload);
    if progress.create(marker).await? {
        return Ok((terminal, true));
    }
    let terminal = load_checkpoint_terminal(flow_id, progress)
        .await?
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Conflict,
                "DSL terminal record disappeared while completing the flow",
            )
        })?;
    Ok((terminal, false))
}

fn terminal_result<S, C>(terminal: CheckpointTerminal, codec: &C) -> CatgaResult<S>
where
    C: DslStateCodec<S>,
{
    codec.decode(&terminal.0)
}

fn top_level_step_index(index: usize) -> CatgaResult<u32> {
    let index = u32::try_from(index)
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "DSL step index exceeds u32"))?;
    if index == DSL_TERMINAL_STEP_INDEX {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL step index is reserved for the terminal record",
        ));
    }
    Ok(index)
}

fn validate_parallel_branch_count(count: usize) -> CatgaResult<()> {
    if count > MAX_DSL_PARALLEL_BRANCHES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL parallel branch count exceeds the supported limit",
        ));
    }
    Ok(())
}

impl<S: Send> Default for DslFlow<S> {
    fn default() -> Self {
        Self::new()
    }
}

fn retry_delay(initial_delay: Duration, retry: usize) -> Duration {
    let multiplier = u32::try_from(retry)
        .ok()
        .and_then(|retry| 1_u32.checked_shl(retry))
        .unwrap_or(u32::MAX);
    initial_delay.saturating_mul(multiplier)
}
