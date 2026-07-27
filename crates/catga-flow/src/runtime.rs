use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tracing::Instrument;

use crate::{
    FlowChildLauncher, FlowContinuation, FlowDefinition, FlowQuery, FlowScheduler, FlowState,
    FlowStatus, FlowStepOutcome, FlowTagPolicy, SuspendedFlowStore, WaitCondition, WaitPolicy,
    metrics::FlowMetrics,
};

const MAX_CHILD_LAUNCH_CAS_RETRIES: usize = 8;

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

    /// Returns whether cancellation reached a terminal flow state.
    pub fn is_cancelled(&self) -> bool {
        self.state.status() == FlowStatus::Cancelled
    }

    /// Returns whether another executor currently owns active execution.
    pub fn is_running(&self) -> bool {
        self.state.status() == FlowStatus::Running
    }

    /// Returns whether the runtime is durably retrying rollback actions.
    pub fn is_compensating(&self) -> bool {
        self.state.status() == FlowStatus::Compensating
    }
}

/// Executes named flow definitions against a durable continuation store.
///
/// While a step handler is pending, the runtime refreshes its durable heartbeat at half of
/// `stale_after`. Losing that owner-conditional heartbeat drops the handler future before it can
/// persist another transition. External effects remain at-least-once and must be idempotent.
pub struct FlowRuntime<S: ?Sized, H: ?Sized> {
    store: Arc<S>,
    scheduler: Arc<H>,
    definition: Arc<FlowDefinition>,
    owner: Box<str>,
    stale_after: Duration,
    metrics: FlowMetrics,
    tag_policy: Option<FlowTagPolicy>,
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
            definition: Arc::new(definition),
            owner: owner.into(),
            stale_after: Duration::from_secs(30),
            metrics: FlowMetrics::default(),
            tag_policy: None,
        }
    }

    /// Sets how long an unheartbeated running continuation remains exclusively owned.
    ///
    /// Positive durations also control automatic handler renewal at half this interval. Zero is
    /// retained for deterministic forced-recovery tests and disables automatic renewal.
    pub fn with_stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = stale_after;
        self
    }

    /// Applies timeout and retry rules to explicitly tagged durable steps.
    ///
    /// Timeouts and retries stay within the caller-owned `start` or `resume` future; no
    /// background task is spawned. Only transient execution errors retry, and every durable
    /// transition remains persisted regardless of policy persistence markers.
    pub fn with_tag_policy(mut self, tag_policy: FlowTagPolicy) -> Self {
        self.tag_policy = Some(tag_policy);
        self
    }

    /// Starts a new flow and executes until it suspends or reaches a terminal state.
    pub async fn start(
        &self,
        flow_id: impl Into<Box<str>>,
        data: impl Into<Arc<[u8]>>,
    ) -> CatgaResult<FlowRuntimeResult> {
        self.definition.validate()?;
        let Some(first_step) = self.definition.first_step_name() else {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "a flow definition requires at least one registered step",
            ));
        };
        let state =
            FlowState::new(flow_id, self.definition.name(), data, self.owner.clone()).suspended();
        state.validate()?;
        let continuation = FlowContinuation::new(state, first_step);
        if !self.store.create(continuation.clone()).await? {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "a flow with this identity already exists",
            ));
        }
        self.metrics.record_started();
        self.resume(continuation.state().id()).await
    }

    /// Resumes a previously suspended flow from its persisted named step.
    pub async fn resume(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        self.resume_at(flow_id, SystemTime::now()).await
    }

    /// Resumes `flow_id` only when its persisted suspended state still matches `state_id`.
    ///
    /// External schedulers should call this with the state target returned in
    /// [`crate::ScheduledResume`]. A stale job cannot resume a flow that has already advanced to
    /// another named step.
    pub async fn resume_scheduled(
        &self,
        flow_id: &str,
        state_id: &str,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
        };
        self.ensure_definition(&continuation)?;
        if continuation.step_name() != state_id {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "scheduled resume targets a stale flow state",
            ));
        }
        self.resume(flow_id).await
    }

    /// Cancels a durable flow unless it has already reached a terminal state.
    ///
    /// A handler that was already executing can still complete external effects; its stale
    /// continuation version cannot persist a later flow state after this cancellation wins.
    /// A flow that is durably compensating cannot be cancelled because cancellation would abandon
    /// rollback actions that are required to restore its previously completed effects.
    pub async fn cancel(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
        };
        self.ensure_definition(&continuation)?;
        if continuation.state().status().is_terminal() {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        if continuation.state().status() == FlowStatus::Compensating {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "a compensating flow cannot be cancelled before its rollback completes",
            ));
        }
        let schedule_id: Option<Box<str>> = continuation.schedule_id().map(Into::into);
        let cancelled = continuation
            .clone()
            .ready()
            .with_state(continuation.state().clone().cancelled().next_version()?);
        if self
            .store
            .update(continuation.state().version(), cancelled.clone())
            .await?
        {
            self.metrics.record_cancelled(continuation.created_at());
            if let Some(schedule_id) = schedule_id
                && let Err(error) = self.scheduler.cancel_resume(&schedule_id).await
            {
                tracing::warn!(
                    flow_id,
                    schedule_id = schedule_id.as_ref(),
                    error = ?error,
                    "flow cancellation persisted, but its scheduled resume could not be cancelled"
                );
            }
            Ok(FlowRuntimeResult::new(cancelled.state().clone()))
        } else {
            self.current_result(flow_id).await
        }
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
        let is_owned = matches!(
            continuation.state().status(),
            FlowStatus::Running | FlowStatus::Compensating
        );
        let is_stale_owned =
            is_owned && is_stale(continuation.state().heartbeat(), now, self.stale_after);
        if is_owned && !is_stale_owned {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        if continuation.state().status() != FlowStatus::Suspended && !is_stale_owned {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow is not resumable",
            ));
        }
        if !is_stale_owned && let Some(wait) = continuation.wait() {
            match evaluate_wait(wait, now) {
                WaitEvaluation::Pending => {
                    return Ok(FlowRuntimeResult::new(continuation.state().clone()));
                }
                WaitEvaluation::Failed(error) => return self.fail(continuation, error, None).await,
                WaitEvaluation::Ready => {}
            }
        }
        if !is_stale_owned && continuation.resume_at().is_some_and(|due_at| due_at > now) {
            return Ok(FlowRuntimeResult::new(continuation.state().clone()));
        }
        if let Some(running) = self.claim(continuation).await? {
            if running.state().status() == FlowStatus::Compensating {
                self.compensate(running, None).await
            } else {
                self.drive(running).await
            }
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
        let wait = continuation.wait().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow is not waiting for child results",
            )
        })?;
        if !wait.accepts_child(child_id) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "child result does not belong to this flow wait",
            ));
        }
        if !wait.accepts_payload_len(payload.len()) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow child result payload exceeds the supported bound",
            ));
        }
        if !self
            .store
            .record_wait_success(flow_id, continuation.state().version(), child_id, payload)
            .await?
        {
            return self.current_result(flow_id).await;
        }
        self.resume(flow_id).await
    }

    /// Records a successful child result using only its parent wait correlation identity.
    ///
    /// Message consumers can use this when a child completion crosses a transport boundary and
    /// does not otherwise know the parent's flow identity. The store performs an indexed lookup;
    /// the existing version-fenced completion path still owns payload validation, duplicate
    /// detection, and parent resumption.
    pub async fn record_wait_success_by_correlation(
        &self,
        correlation_id: &str,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get_by_wait_correlation(correlation_id).await? else {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "flow wait correlation does not exist",
            ));
        };
        self.record_wait_success(continuation.state().id(), child_id, payload)
            .await
    }

    /// Records a failed child result using only its parent wait correlation identity.
    ///
    /// This has the same indexed lookup and version fencing as
    /// [`Self::record_wait_success_by_correlation`]. The persisted wait policy decides whether
    /// the parent keeps waiting or transitions into its ordinary failure and compensation path.
    pub async fn record_wait_failure_by_correlation(
        &self,
        correlation_id: &str,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<FlowRuntimeResult> {
        let Some(continuation) = self.store.get_by_wait_correlation(correlation_id).await? else {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "flow wait correlation does not exist",
            ));
        };
        self.record_wait_failure(continuation.state().id(), child_id, error)
            .await
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
        let wait = continuation.wait().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow is not waiting for child results",
            )
        })?;
        if !wait.accepts_child(child_id) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "child result does not belong to this flow wait",
            ));
        }
        if !self
            .store
            .record_wait_failure(flow_id, continuation.state().version(), child_id, error)
            .await?
        {
            return self.current_result(flow_id).await;
        }
        self.resume(flow_id).await
    }

    /// Launches every currently unlaunched stable child of `flow_id` without retaining tasks.
    ///
    /// The parent continuation records the child identities before this method invokes `launcher`.
    /// Each launch transitions through an owner-bound, expiring durable claim. A crash after the
    /// external launcher accepts a child can therefore cause a later call to launch the same
    /// identity again; [`FlowChildLauncher`] implementations must de-duplicate that stable pair.
    /// At most one launch future is active at a time and the method retains no child result.
    pub async fn launch_waiting_children<L>(
        &self,
        flow_id: &str,
        launcher: &L,
    ) -> CatgaResult<usize>
    where
        L: FlowChildLauncher + ?Sized,
    {
        let mut launched = 0_usize;
        loop {
            let Some((child_id, correlation_id)) = self.claim_next_wait_child(flow_id).await?
            else {
                return Ok(launched);
            };
            match launcher.launch(flow_id, &child_id, &correlation_id).await {
                Ok(()) => {
                    self.finish_wait_child_claim(flow_id, &child_id, true)
                        .await?;
                    launched = launched.saturating_add(1);
                }
                Err(error) => {
                    if let Err(release_error) = self
                        .finish_wait_child_claim(flow_id, &child_id, false)
                        .await
                    {
                        tracing::warn!(
                            flow_id,
                            child_id = child_id.as_ref(),
                            error = ?release_error,
                            "child launch failed and its durable launch claim could not be released"
                        );
                    }
                    return Err(error);
                }
            }
        }
    }

    /// Refreshes the caller's durable execution lease without changing its business version.
    ///
    /// The runtime automatically calls this while a registered step future is pending. It remains
    /// public for caller-owned work performed outside that execution loop.
    pub async fn heartbeat(&self, flow_id: &str, version: i64) -> CatgaResult<bool> {
        let Some(continuation) = self.store.get(flow_id).await? else {
            return Ok(false);
        };
        self.ensure_definition(&continuation)?;
        self.store.heartbeat(flow_id, &self.owner, version).await
    }

    /// Registers missing scheduler identities for delayed suspensions in one caller-owned batch.
    ///
    /// The method inspects at most `max_scan` continuations and attempts to reconcile at most
    /// `max_results` suspended continuations belonging to this runtime's definition. It does not
    /// spawn background work. Callers decide when to invoke it, such as after process recovery or
    /// before polling due work. Repeated calls are safe when the scheduler implements the
    /// [`FlowScheduler::schedule_resume`] idempotency contract for `(flow_id, state_id)`.
    ///
    /// Returns the number of schedule identities durably recorded. A concurrent state change is
    /// left for a later bounded call, preserving the newer continuation without overwriting it.
    /// Returns [`ErrorCode::Unsupported`] when the selected store cannot perform bounded
    /// continuation discovery.
    pub async fn reconcile_delayed_suspensions(
        &self,
        max_results: usize,
        max_scan: usize,
    ) -> CatgaResult<usize> {
        let query = FlowQuery::new(max_results, max_scan)?
            .with_status(FlowStatus::Suspended)
            .with_flow_type(self.definition.name());
        let summaries = self.store.query(&query).await?;
        let mut reconciled = 0_usize;
        for summary in summaries {
            let Some(continuation) = self.store.get(summary.id()).await? else {
                continue;
            };
            continuation.validate()?;
            if continuation.state().status() != FlowStatus::Suspended
                || continuation.state().flow_type() != self.definition.name()
                || continuation.schedule_id().is_some()
                || continuation.resume_at().is_none()
            {
                continue;
            }
            if self
                .persist_delayed_schedule_identity(continuation)
                .await?
                .is_some()
            {
                reconciled = reconciled.saturating_add(1);
            }
        }
        Ok(reconciled)
    }

    async fn drive(&self, mut continuation: FlowContinuation) -> CatgaResult<FlowRuntimeResult> {
        let mut execution = self
            .metrics
            .begin_execution(continuation.state().id(), self.definition.name());
        loop {
            let state = continuation.state().clone();
            let mut step = execution.begin_step(continuation.step_name());
            let result = self
                .execute_step_with_heartbeat(&continuation, state.clone(), step.span())
                .await;
            let step_outcome = if matches!(&result, Ok(FlowStepOutcome::Fail(_)) | Err(_)) {
                "failure"
            } else {
                "success"
            };
            step.complete(step_outcome);
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(error) => return self.fail(continuation, error, Some(&mut execution)).await,
            };
            match outcome {
                FlowStepOutcome::Advance => {
                    let compensated = self.record_step_compensation(&continuation)?;
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self.complete(continuation, state, &mut execution).await;
                    };
                    let flow_id: Box<str> = continuation.state().id().into();
                    if let Some(running) = self.transition_to(compensated, state, next_step).await?
                    {
                        continuation = running;
                    } else {
                        return self.current_result(&flow_id).await;
                    }
                }
                FlowStepOutcome::Goto(next_step) => {
                    let compensated = self.record_step_compensation(&continuation)?;
                    if !self.definition.has_step(&next_step) {
                        return self
                            .fail(
                                compensated,
                                CatgaError::new(
                                    ErrorCode::NotFound,
                                    "a flow transition references an unregistered step",
                                ),
                                Some(&mut execution),
                            )
                            .await;
                    }
                    let flow_id: Box<str> = continuation.state().id().into();
                    if let Some(running) =
                        self.transition_to(compensated, state, &next_step).await?
                    {
                        continuation = running;
                    } else {
                        return self.current_result(&flow_id).await;
                    }
                }
                FlowStepOutcome::SuspendUntil(resume_at) => {
                    let compensated = self.record_step_compensation(&continuation)?;
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self
                            .fail(
                                compensated,
                                CatgaError::new(
                                    ErrorCode::Validation,
                                    "a delayed flow step requires a following step",
                                ),
                                Some(&mut execution),
                            )
                            .await;
                    };
                    let completed_steps = state.step().saturating_add(1);
                    let expected_version = state.version();
                    let pending = compensated
                        .clone()
                        .at_step(next_step)
                        .with_state(state.at_step(completed_steps).suspended().next_version()?)
                        .delayed_until(resume_at);
                    self.persist(expected_version, pending.clone()).await?;
                    let suspended = match self
                        .persist_delayed_schedule_identity(pending.clone())
                        .await
                    {
                        Ok(Some(suspended)) => suspended,
                        Ok(None) => pending.clone(),
                        Err(error) => {
                            tracing::warn!(
                                flow_id = pending.state().id(),
                                state_id = pending.step_name(),
                                error = ?error,
                                "delayed flow suspension persisted, but its schedule identity will be reconciled later"
                            );
                            pending.clone()
                        }
                    };
                    execution.complete("suspended");
                    return Ok(FlowRuntimeResult::new(suspended.state().clone()));
                }
                FlowStepOutcome::Wait(wait) => {
                    let compensated = self.record_step_compensation(&continuation)?;
                    let Some(next_step) = self.definition.next_step_name(continuation.step_name())
                    else {
                        return self
                            .fail(
                                compensated,
                                CatgaError::new(
                                    ErrorCode::Validation,
                                    "a waiting flow step requires a following step",
                                ),
                                Some(&mut execution),
                            )
                            .await;
                    };
                    let completed_steps = state.step().saturating_add(1);
                    let expected_version = state.version();
                    if let Err(error) = wait.validate() {
                        return self.fail(compensated, error, Some(&mut execution)).await;
                    }
                    let suspended = compensated
                        .at_step(next_step)
                        .with_state(state.at_step(completed_steps).suspended().next_version()?)
                        .with_wait(wait);
                    self.persist(expected_version, suspended.clone()).await?;
                    execution.complete("suspended");
                    return Ok(FlowRuntimeResult::new(suspended.state().clone()));
                }
                FlowStepOutcome::Complete => {
                    return self.complete(continuation, state, &mut execution).await;
                }
                FlowStepOutcome::Fail(error) => {
                    return self.fail(continuation, error, Some(&mut execution)).await;
                }
            }
        }
    }

    async fn execute_step_with_heartbeat(
        &self,
        continuation: &FlowContinuation,
        state: FlowState,
        span: tracing::Span,
    ) -> CatgaResult<FlowStepOutcome> {
        let Some(tag) = self.definition.step_tag(continuation.step_name()) else {
            return self
                .execute_step_attempt(continuation, state, span, None)
                .await;
        };
        let Some(tag_policy) = self.tag_policy.as_ref() else {
            return self
                .execute_step_attempt(continuation, state, span, None)
                .await;
        };
        let timeout = tag_policy.timeout_for(tag);
        let retries = tag_policy.retries_for(tag);
        for attempt in 0..=retries {
            match self
                .execute_step_attempt(continuation, state.clone(), span.clone(), Some(timeout))
                .await
            {
                Err(error) if error.code() == ErrorCode::Transient && attempt < retries => {
                    if !self
                        .store
                        .heartbeat(
                            continuation.state().id(),
                            &self.owner,
                            continuation.state().version(),
                        )
                        .await?
                    {
                        return Err(CatgaError::new(
                            ErrorCode::Conflict,
                            "flow execution ownership was lost before a tagged retry",
                        ));
                    }
                }
                result => return result,
            }
        }
        Err(CatgaError::new(
            ErrorCode::Internal,
            "tagged retry loop completed without an execution result",
        ))
    }

    async fn execute_step_attempt(
        &self,
        continuation: &FlowContinuation,
        state: FlowState,
        span: tracing::Span,
        timeout: Option<Duration>,
    ) -> CatgaResult<FlowStepOutcome> {
        let execution = self
            .definition
            .execute(continuation.step_name(), state)
            .instrument(span);
        if self.stale_after.is_zero() {
            return match timeout {
                Some(timeout) => tokio::time::timeout(timeout, execution)
                    .await
                    .map_err(|_| {
                        CatgaError::new(ErrorCode::Timeout, "tagged flow step timed out")
                    })?,
                None => execution.await,
            };
        }
        tokio::pin!(execution);
        let deadline = async move {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        let heartbeat_interval = (self.stale_after / 2).max(Duration::from_nanos(1));
        loop {
            tokio::select! {
                result = &mut execution => return result,
                _ = &mut deadline => {
                    return Err(CatgaError::new(ErrorCode::Timeout, "tagged flow step timed out"));
                }
                _ = tokio::time::sleep(heartbeat_interval) => {
                    if !self.store.heartbeat(
                        continuation.state().id(),
                        &self.owner,
                        continuation.state().version(),
                    ).await? {
                        return Err(CatgaError::new(
                            ErrorCode::Conflict,
                            "flow execution ownership was lost while its step was running",
                        ));
                    }
                }
            }
        }
    }

    async fn complete(
        &self,
        continuation: FlowContinuation,
        state: FlowState,
        execution: &mut crate::metrics::FlowExecution,
    ) -> CatgaResult<FlowRuntimeResult> {
        let completed_steps = state.step().saturating_add(1);
        let done = continuation
            .clone()
            .with_state(state.done(completed_steps).next_version()?);
        self.persist(continuation.state().version(), done.clone())
            .await?;
        self.metrics.record_completed(continuation.created_at());
        execution.complete("success");
        Ok(FlowRuntimeResult::new(done.state().clone()))
    }

    async fn fail(
        &self,
        continuation: FlowContinuation,
        error: CatgaError,
        execution: Option<&mut crate::metrics::FlowExecution>,
    ) -> CatgaResult<FlowRuntimeResult> {
        if !continuation.compensation_steps().is_empty() {
            let compensating = continuation.clone().ready().with_state(
                continuation
                    .state()
                    .clone()
                    .with_error(error)
                    .compensating()
                    .next_version()?,
            );
            self.persist(continuation.state().version(), compensating.clone())
                .await?;
            return self.compensate(compensating, execution).await;
        }
        let failed = continuation
            .clone()
            .ready()
            .with_state(continuation.state().clone().failed(error).next_version()?);
        self.persist(continuation.state().version(), failed.clone())
            .await?;
        self.metrics.record_failed(continuation.created_at());
        if let Some(execution) = execution {
            execution.complete("failure");
        }
        Ok(FlowRuntimeResult::new(failed.state().clone()))
    }

    async fn compensate(
        &self,
        mut continuation: FlowContinuation,
        execution: Option<&mut crate::metrics::FlowExecution>,
    ) -> CatgaResult<FlowRuntimeResult> {
        while let Some(step_name) = continuation.next_compensation().map(str::to_owned) {
            self.execute_compensation_with_heartbeat(
                &continuation,
                continuation.state().clone(),
                &step_name,
            )
            .await?;
            let next = continuation
                .clone()
                .complete_compensation()
                .with_state(continuation.state().clone().compensating().next_version()?);
            self.persist(continuation.state().version(), next.clone())
                .await?;
            continuation = next;
        }
        let error = continuation.state().error().cloned().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "a compensating flow is missing its original failure",
            )
        })?;
        let failed = continuation
            .clone()
            .ready()
            .with_state(continuation.state().clone().failed(error).next_version()?);
        self.persist(continuation.state().version(), failed.clone())
            .await?;
        self.metrics.record_failed(continuation.created_at());
        if let Some(execution) = execution {
            execution.complete("failure");
        }
        Ok(FlowRuntimeResult::new(failed.state().clone()))
    }

    async fn execute_compensation_with_heartbeat(
        &self,
        continuation: &FlowContinuation,
        state: FlowState,
        step_name: &str,
    ) -> CatgaResult<()> {
        let compensation = self.definition.compensate(step_name, state);
        if self.stale_after.is_zero() {
            return compensation.await;
        }
        tokio::pin!(compensation);
        let heartbeat_interval = (self.stale_after / 2).max(Duration::from_nanos(1));
        loop {
            tokio::select! {
                result = &mut compensation => return result,
                _ = tokio::time::sleep(heartbeat_interval) => {
                    if !self.store.heartbeat(
                        continuation.state().id(),
                        &self.owner,
                        continuation.state().version(),
                    ).await? {
                        return Err(CatgaError::new(
                            ErrorCode::Conflict,
                            "flow compensation ownership was lost while its action was running",
                        ));
                    }
                }
            }
        }
    }

    async fn transition_to(
        &self,
        continuation: FlowContinuation,
        state: FlowState,
        next_step: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        let completed_steps = state.step().saturating_add(1);
        let next = continuation
            .clone()
            .at_step(next_step)
            .with_state(state.at_step(completed_steps).suspended().next_version()?);
        self.persist(continuation.state().version(), next.clone())
            .await?;
        self.claim(next).await
    }

    fn record_step_compensation(
        &self,
        continuation: &FlowContinuation,
    ) -> CatgaResult<FlowContinuation> {
        if self.definition.has_compensation(continuation.step_name()) {
            return continuation
                .clone()
                .record_compensation(continuation.step_name());
        }
        Ok(continuation.clone())
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

    async fn persist_delayed_schedule_identity(
        &self,
        continuation: FlowContinuation,
    ) -> CatgaResult<Option<FlowContinuation>> {
        let Some(resume_at) = continuation.resume_at() else {
            return Ok(None);
        };
        let schedule_id = self
            .scheduler
            .schedule_resume(
                continuation.state().id(),
                continuation.step_name(),
                resume_at,
            )
            .await?;
        let scheduled = continuation
            .clone()
            .with_state(continuation.state().clone().next_version()?)
            .with_schedule_id(schedule_id);
        Ok(self
            .store
            .update(continuation.state().version(), scheduled.clone())
            .await?
            .then_some(scheduled))
    }

    async fn claim(&self, continuation: FlowContinuation) -> CatgaResult<Option<FlowContinuation>> {
        let claimed_state = continuation.state().clone().claimed_by(self.owner.clone());
        let claimed_state = if continuation.state().status() == FlowStatus::Compensating {
            claimed_state.compensating()
        } else {
            claimed_state.running()
        };
        let running = continuation
            .clone()
            .ready()
            .with_state(claimed_state.next_version()?);
        Ok(self
            .store
            .claim(&continuation, running.clone())
            .await?
            .then_some(running))
    }

    async fn claim_next_wait_child(
        &self,
        flow_id: &str,
    ) -> CatgaResult<Option<(Box<str>, Box<str>)>> {
        for _ in 0..MAX_CHILD_LAUNCH_CAS_RETRIES {
            let Some(continuation) = self.store.get(flow_id).await? else {
                return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
            };
            self.ensure_definition(&continuation)?;
            if continuation.state().status().is_terminal() {
                return Ok(None);
            }
            let Some(wait) = continuation.wait() else {
                return Ok(None);
            };
            wait.validate()?;
            let now = SystemTime::now();
            let claim_for = self.stale_after.max(Duration::from_nanos(1));
            let Some((child_id, claimed_wait)) =
                wait.claim_next_child(self.owner.clone(), now, claim_for)
            else {
                return Ok(None);
            };
            let correlation_id: Box<str> = wait.correlation_id().into();
            let next = continuation
                .clone()
                .with_wait(claimed_wait)
                .with_state(continuation.state().clone().suspended().next_version()?);
            if self.store.claim(&continuation, next).await? {
                return Ok(Some((child_id, correlation_id)));
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "flow child launch claim did not stabilize",
        ))
    }

    async fn finish_wait_child_claim(
        &self,
        flow_id: &str,
        child_id: &str,
        launched: bool,
    ) -> CatgaResult<()> {
        for _ in 0..MAX_CHILD_LAUNCH_CAS_RETRIES {
            let Some(continuation) = self.store.get(flow_id).await? else {
                return Err(CatgaError::new(ErrorCode::NotFound, "flow does not exist"));
            };
            self.ensure_definition(&continuation)?;
            if continuation.state().status().is_terminal() {
                return Ok(());
            }
            let Some(wait) = continuation.wait() else {
                return Ok(());
            };
            let next_wait = if launched {
                wait.mark_child_launched(child_id, &self.owner)
            } else {
                wait.release_child_claim(child_id, &self.owner)
            };
            let Some(next_wait) = next_wait else {
                return Ok(());
            };
            let next = continuation
                .clone()
                .with_wait(next_wait)
                .with_state(continuation.state().clone().suspended().next_version()?);
            if self.store.claim(&continuation, next).await? {
                return Ok(());
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "flow child launch completion did not stabilize",
        ))
    }

    async fn current_result(&self, flow_id: &str) -> CatgaResult<FlowRuntimeResult> {
        let continuation = self
            .store
            .get(flow_id)
            .await?
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "flow does not exist"))?;
        continuation.validate()?;
        Ok(FlowRuntimeResult::new(continuation.state().clone()))
    }

    fn ensure_definition(&self, continuation: &FlowContinuation) -> CatgaResult<()> {
        continuation.validate()?;
        self.definition.validate()?;
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
