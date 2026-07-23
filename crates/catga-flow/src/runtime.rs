use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::{
    FlowContinuation, FlowDefinition, FlowScheduler, FlowState, FlowStatus, FlowStepOutcome,
    SuspendedFlowStore, WaitCondition, WaitPolicy,
};

/// The observable state after starting, resuming, or recording a durable flow trigger.
#[derive(Clone, Debug)]
pub struct FlowRuntimeResult {
    state: FlowState,
}

impl FlowRuntimeResult {
    fn new(state: FlowState) -> Self {
        Self { state }
    }

    /// Returns the flow's immutable durable state.
    pub fn state(&self) -> &FlowState {
        &self.state
    }

    /// Returns whether execution is waiting for a scheduler or child result.
    pub fn is_suspended(&self) -> bool {
        self.state.status() == FlowStatus::Suspended
    }

    /// Returns whether the flow reached a successful terminal state.
    pub fn is_success(&self) -> bool {
        self.state.status() == FlowStatus::Done
    }

    /// Returns whether the flow reached a failed terminal state.
    pub fn is_failure(&self) -> bool {
        self.state.status() == FlowStatus::Failed
    }

    /// Returns whether another executor currently owns active execution.
    pub fn is_running(&self) -> bool {
        self.state.status() == FlowStatus::Running
    }
}

/// Executes named flow definitions against a durable continuation store.
pub struct FlowRuntime<S: ?Sized, H: ?Sized> {
    store: Arc<S>,
    scheduler: Arc<H>,
    definition: FlowDefinition,
    owner: Box<str>,
    stale_after: Duration,
}

impl<S, H> FlowRuntime<S, H>
where
    S: SuspendedFlowStore + ?Sized,
    H: FlowScheduler + ?Sized,
{
    /// Creates a runtime for one registered definition.
    pub fn new(
        store: Arc<S>,
        scheduler: Arc<H>,
        definition: FlowDefinition,
        owner: impl Into<Box<str>>,
    ) -> Self {
        Self {
            store,
            scheduler,
            definition,
            owner: owner.into(),
            stale_after: Duration::from_secs(30),
        }
    }

    /// Sets how long an unheartbeated running continuation remains exclusively owned.
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    /// Starts a new flow and executes until it suspends or reaches a terminal state.
    pub async fn start(
        &self,
        flow_id: impl Into<Box<str>>,
        data: impl Into<Arc<[u8]>>,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(first_step) = self.definition.first_step_name() else {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "a flow definition requires at least one registered step",
            ));
        };
        let state =
            FlowState::new(flow_id, self.definition.name(), data, self.owner.clone()).suspended();
        let continuation = FlowContinuation::new(state, first_step);
        if !self.store.create(continuation.clone()).await? {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "a flow with this identity already exists",
            ));
        }
        self.resume(continuation.state().id()).await
    }

    /// Resumes a previously suspended flow from its persisted named step.
    pub async fn resume(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        self.resume_at(flow_id, SystemTime::now()).await
    }

    /// Resumes a flow using `now` to deterministically evaluate delay and wait deadlines.
    pub async fn resume_at(
        &self,
        flow_id: &str,
        now: SystemTime,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
        };
        self.ensure_definition(&continuation)?;
        if continuation.state().status().is_terminal() {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        let is_stale_running = continuation.state().status() == FlowStatus::Running
            && is_stale(continuation.state().heartbeat(), now, self.stale_after);
        if continuation.state().status() == FlowStatus::Running && !is_stale_running {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        if continuation.state().status() != FlowStatus::Suspended && !is_stale_running {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow is not resumable",
            ));
        }
        if !is_stale_running && let Some(wait) = continuation.wait() {
            match evaluate_wait(wait, now) {
                WaitEvaluation::Pending => {
                    return Ok(FlowRuntimeResult::new(continuation.state().clone()));
                }
                WaitEvaluation::Failed(error) => return self.fail(continuation, error).await,
                WaitEvaluation::Ready => {}
            }
        }
        if !is_stale_running && continuation.resume_at().is_some_and(|due_at| due_at > now) {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        if let Some(running) = self.claim(continuation).await? {
            self.drive(running).await
        } else {
            self.current_result(flow_id).await
        }
    }

    /// Records one successful child result and resumes when the wait policy is satisfied.
    pub async fn record_wait_success(
        &self,
        flow_id: &str,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
        };
        if continuation.state().status().is_terminal() {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        self.ensure_definition(&continuation)?;
        if !self
            .store
            .record_wait_success(flow_id, continuation.state().version(), child_id, payload)
            .await?
        {
            return self.current_result(flow_id).await;
        }
        self.resume(flow_id).await
    }

    /// Records one failed child result and resumes when the wait policy is resolved.
    pub async fn record_wait_failure(
        &self,
        flow_id: &str,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
        };
        if continuation.state().status().is_terminal() {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        self.ensure_definition(&continuation)?;
        if !self
            .store
            .record_wait_failure(flow_id, continuation.state().version(), child_id, error)
            .await?
        {
            return self.current_result(flow_id).await;
        }
        self.resume(flow_id).await
    }

    /// Refreshes the caller's durable execution lease without changing its business version.
    ///
    /// Long-running handlers should call this more frequently than `stale_after`; handlers remain
    /// at-least-once and must make external side effects idempotent.
    pub async fn heartbeat(&self, flow_id: &str, version: i64) -> CatgaResult<bool> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Ok(false);
        };
        self.ensure_definition(&continuation)?;
        self.store.heartbeat(flow_id, &self.owner, version).await
    }

    async fn drive(&self, mut continuation: FlowContinuation) -> CatgaResult<FlowRuntimeResult> {
        loop {
            let state = continuation.state().clone();
            let outcome = match self
                .definition
                .execute(continuation.step_name(), state.clone())
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => return self.fail(continuation, error).await,
            };
            match outcome {
                FlowStepOutcome::Advance => {
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self
                            .fail(
                                continuation,
                                CatgaError::new(
                                    ErrorCode::Validation,
                                    "an advancing flow step requires a following step",
                                ),
                            )
                            .await;
                    };
                    let completed_steps = state.step().saturating_add(1);
                    let next = continuation
                        .clone()
                        .at_step(next_step)
                        .with_state(state.at_step(completed_steps).suspended().next_version());
                    self.persist(continuation.state().version(), next.clone())
                        .await?;
                    let next_flow_id: Box<str> = next.state().id().into();
                    if let Some(running) = self.claim(next).await? {
                        continuation = running;
                    } else {
                        return self.current_result(&next_flow_id).await;
                    }
                }
                FlowStepOutcome::SuspendUntil(resume_at) => {
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self
                            .fail(
                                continuation,
                                CatgaError::new(
                                    ErrorCode::Validation,
                                    "a delayed flow step requires a following step",
                                ),
                            )
                            .await;
                    };
                    let completed_steps = state.step().saturating_add(1);
                    let suspended = continuation
                        .clone()
                        .at_step(next_step)
                        .with_state(state.at_step(completed_steps).suspended().next_version())
                        .delayed_until(resume_at);
                    self.persist(continuation.state().version(), suspended.clone())
                        .await?;
                    if let Err(error) = self
                        .scheduler
                        .schedule_resume(suspended.state().id(), resume_at)
                        .await
                    {
                        return self.fail(suspended, error).await;
                    }
                    return Ok(FlowRuntimeResult::new(suspended.state().clone()));
                }
                FlowStepOutcome::Wait(wait) => {
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self
                            .fail(
                                continuation,
                                CatgaError::new(
                                    ErrorCode::Validation,
                                    "a waiting flow step requires a following step",
                                ),
                            )
                            .await;
                    };
                    let completed_steps = state.step().saturating_add(1);
                    let suspended = continuation
                        .clone()
                        .at_step(next_step)
                        .with_state(state.at_step(completed_steps).suspended().next_version())
                        .with_wait(wait);
                    self.persist(continuation.state().version(), suspended.clone())
                        .await?;
                    return Ok(FlowRuntimeResult::new(suspended.state().clone()));
                }
                FlowStepOutcome::Complete => {
                    let completed_steps = state.step().saturating_add(1);
                    let done = continuation
                        .clone()
                        .with_state(state.done(completed_steps).next_version());
                    self.persist(continuation.state().version(), done.clone())
                        .await?;
                    return Ok(FlowRuntimeResult::new(done.state().clone()));
                }
                FlowStepOutcome::Fail(error) => return self.fail(continuation, error).await,
            }
        }
    }

    async fn fail(
        &self,
        continuation: FlowContinuation,
        error: CatgaError,
    ) -> CatgaResult<FlowRuntimeResult> {
        let failed = continuation
            .clone()
            .ready()
            .with_state(continuation.state().clone().failed(error).next_version());
        self.persist(continuation.state().version(), failed.clone())
            .await?;
        Ok(FlowRuntimeResult::new(failed.state().clone()))
    }

    async fn persist(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<()> {
        if self.store.update(expected_version, next).await? {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Transient,
                "flow continuation changed before it could be persisted",
            ))
        }
    }

    async fn claim(&self, continuation: FlowContinuation) -> CatgaResult<Option<FlowContinuation>> {
        let running = continuation.clone().ready().with_state(
            continuation
                .state()
                .clone()
                .claimed_by(self.owner.clone())
                .running()
                .next_version(),
        );
        Ok(self
            .store
            .claim(&continuation, running.clone())
            .await?
            .then_some(running))
    }

    async fn current_result(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        self.store
            .get(flow_id)
            .await?
            .map(|continuation| FlowRuntimeResult::new(continuation.state().clone()))
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "flow does not exist"))
    }

    fn ensure_definition(&self, continuation: &FlowContinuation) -> CatgaResult<()> {
        if continuation.state().flow_type() == self.definition.name() {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Validation,
                "flow continuation belongs to a different definition",
            ))
        }
    }
}

enum WaitEvaluation {
    Pending,
    Ready,
    Failed(CatgaError),
}

fn evaluate_wait(wait: &WaitCondition, now: SystemTime) -> WaitEvaluation {
    if wait.is_expired_at(now) {
        return WaitEvaluation::Failed(CatgaError::new(
            ErrorCode::Timeout,
            "flow wait condition timed out",
        ));
    }
    match wait.policy() {
        WaitPolicy::All => {
            if let Some(error) = wait
                .results()
                .iter()
                .find_map(|result| result.error().cloned())
            {
                return WaitEvaluation::Failed(error);
            }
            if wait.completed_count() >= wait.expected_count() {
                WaitEvaluation::Ready
            } else {
                WaitEvaluation::Pending
            }
        }
        WaitPolicy::Any => {
            if wait.results().iter().any(|result| result.is_success()) {
                return WaitEvaluation::Ready;
            }
            if wait.completed_count() >= wait.expected_count() {
                let error = wait
                    .results()
                    .last()
                    .and_then(|result| result.error().cloned())
                    .unwrap_or_else(|| {
                        CatgaError::new(ErrorCode::Transient, "all flow wait children failed")
                    });
                WaitEvaluation::Failed(error)
            } else {
                WaitEvaluation::Pending
            }
        }
    }
}

fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
