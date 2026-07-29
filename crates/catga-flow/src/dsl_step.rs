//! Composable DSL step builders and the closure vocabulary they share with [`crate::DslFlow`].

use std::sync::Arc;

use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Mediator, RemoteRequest, Request, RequestClient,
};
use futures::future::BoxFuture;

pub(crate) type Action<S> =
    Box<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(crate) type Condition<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;
pub(crate) type StateFailure<S> = Box<dyn Fn(&S) -> Option<CatgaError> + Send + Sync>;
pub(crate) type Query<S, R> =
    Box<dyn for<'a> Fn(&'a S) -> BoxFuture<'a, CatgaResult<R>> + Send + Sync>;
pub(crate) type ResponseFailure<R> = Box<dyn Fn(&R) -> Option<CatgaError> + Send + Sync>;
pub(crate) type BranchSelector<S> = Box<dyn Fn(&S) -> Option<usize> + Send + Sync>;
pub(super) type Merge<S> = Box<dyn Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync>;
pub(super) type MergeWinner<S> = Box<dyn Fn(&mut S, S) -> CatgaResult<()> + Send + Sync>;
pub(super) type CloneState<S> = fn(&S) -> S;
pub(crate) type ReplayableForEachSelect<S> =
    Box<dyn Fn(&S) -> CatgaResult<Vec<Vec<u8>>> + Send + Sync>;
pub(crate) type ReplayableForEachItem<S> =
    Box<dyn for<'a> Fn(&'a mut S, &'a [u8]) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
pub(crate) type ReplayableForEachErrorHandler<S> =
    dyn for<'a> Fn(&'a mut S, usize, CatgaError) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync;

pub(crate) struct ReplayableForEach<S> {
    pub(crate) select: ReplayableForEachSelect<S>,
    pub(crate) action: ReplayableForEachItem<S>,
    pub(crate) on_error: Option<Box<ReplayableForEachErrorHandler<S>>>,
}

/// Largest number of branches one DSL parallel or `when_any` step may retain.
///
/// The caller-owned execution future keeps one cloned state and one nested future per branch, so
/// this bound prevents an untrusted or accidentally unbounded iterator from retaining unbounded
/// state or scheduling unbounded work. Checkpointed and in-process DSL execution share it.
pub const MAX_DSL_PARALLEL_BRANCHES: usize = 64;

/// A composable, state-mutating DSL action.
///
/// A `DslStep` owns only its action and decorator closures. Converting it through
/// [`crate::DslFlow::step`] adds no task, lock, or persistence layer: the flow executes the
/// resulting action in its existing caller-owned future.
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

    pub(crate) fn into_action(self) -> Action<S> {
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
