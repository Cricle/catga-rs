use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio_util::sync::CancellationToken;

use crate::{FlowResult, FlowState, FlowStatus, FlowStore};

/// Executes durable flows with optimistic ownership and explicit heartbeats.
pub struct FlowExecutor<S: ?Sized> {
    store: Arc<S>,
    owner: Box<str>,
    stale_after: Duration,
}

/// Caller-selected policy for supervised durable-flow heartbeats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowHeartbeatOptions {
    /// Maximum time between ownership heartbeats while a work future is pending.
    pub interval: Duration,
}

impl FlowHeartbeatOptions {
    /// Validates and creates a heartbeat policy.
    pub fn new(interval: Duration) -> CatgaResult<Self> {
        if interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow heartbeat interval must be greater than zero",
            ));
        }
        Ok(Self { interval })
    }
}

/// Bounded caller-owned stale-flow recovery policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowRecoveryOptions {
    /// Maximum stale flow claims attempted per recovery sweep.
    pub max_claims: usize,
    /// Delay between successful recovery sweeps.
    pub poll_interval: Duration,
}

impl FlowRecoveryOptions {
    /// Validates and creates a bounded recovery policy.
    pub fn new(max_claims: usize, poll_interval: Duration) -> CatgaResult<Self> {
        if max_claims == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow recovery max_claims must be greater than zero",
            ));
        }
        if poll_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow recovery poll_interval must be greater than zero",
            ));
        }
        Ok(Self {
            max_claims,
            poll_interval,
        })
    }
}

enum ExistingFlow {
    Run(FlowState),
    Complete(FlowResult),
}

impl<S> FlowExecutor<S>
where
    S: FlowStore + ?Sized,
{
    /// Creates an executor owned by `owner`.
    pub fn new(store: Arc<S>, owner: impl Into<Box<str>>, stale_after: Duration) -> Self {
        Self {
            store,
            owner: owner.into(),
            stale_after,
        }
    }

    /// Executes `run` at least once for a newly-created or stale-and-claimed flow.
    ///
    /// Call [`Self::heartbeat`] more often than `stale_after` while a long-running action is
    /// active, and make action side effects idempotent. A process that stops heartbeating can
    /// have its flow claimed and retried by another executor.
    pub async fn execute<Run, RunFuture>(
        &self,
        id: impl Into<Box<str>>,
        flow_type: impl Into<Box<str>>,
        data: impl Into<Arc<[u8]>>,
        run: Run,
    ) -> CatgaResult<FlowResult>
    where
        Run: FnOnce(FlowState) -> RunFuture,
        RunFuture: Future<Output = CatgaResult<FlowResult>>,
    {
        let initial = FlowState::new(id, flow_type, data, self.owner.clone());
        let state = if self.store.create(initial.clone()).await? {
            initial
        } else {
            match self.claim_or_load(&initial).await? {
                ExistingFlow::Run(state) => state,
                ExistingFlow::Complete(result) => return Ok(result),
            }
        };

        let result = self.run_work(state.clone(), run).await;
        self.persist_result(state, result).await
    }

    /// Executes durable work while this caller awaits both the work and periodic heartbeats.
    ///
    /// No task is spawned. Cancellation drops the work future and returns a cancellation error;
    /// callers retain supervision and may later recover the still-running flow after its lease
    /// becomes stale.
    pub async fn execute_with_heartbeat<Run, RunFuture>(
        &self,
        id: impl Into<Box<str>>,
        flow_type: impl Into<Box<str>>,
        data: impl Into<Arc<[u8]>>,
        options: FlowHeartbeatOptions,
        cancellation: CancellationToken,
        run: Run,
    ) -> CatgaResult<FlowResult>
    where
        Run: FnOnce(FlowState) -> RunFuture,
        RunFuture: Future<Output = CatgaResult<FlowResult>>,
    {
        let initial = FlowState::new(id, flow_type, data, self.owner.clone());
        let state = if self.store.create(initial.clone()).await? {
            initial
        } else {
            match self.claim_or_load(&initial).await? {
                ExistingFlow::Run(state) => state,
                ExistingFlow::Complete(result) => return Ok(result),
            }
        };
        let work = run(state.clone());
        tokio::pin!(work);
        let heartbeat = tokio::time::sleep(options.interval);
        tokio::pin!(heartbeat);
        let result = loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(CatgaError::new(ErrorCode::Cancelled, "durable flow work was cancelled"));
                }
                result = &mut work => break match result {
                    Ok(result) => result,
                    Err(error) => FlowResult::failure(state.step(), error),
                },
                _ = &mut heartbeat => {
                    if !self.heartbeat(state.id(), state.version()).await? {
                        return Err(CatgaError::new(ErrorCode::Conflict, "durable flow ownership was lost while heartbeating"));
                    }
                    heartbeat.as_mut().reset(tokio::time::Instant::now() + options.interval);
                }
            }
        };
        self.persist_result(state, result).await
    }

    /// Claims and completes at most `options.max_claims` stale running flows of `flow_type`.
    ///
    /// The caller supplies idempotent work and controls when sweeps occur. A claim race simply
    /// ends the current bounded sweep because the store only returns states this executor owns.
    pub async fn recover_stale<Run, RunFuture>(
        &self,
        flow_type: &str,
        options: FlowRecoveryOptions,
        mut run: Run,
    ) -> CatgaResult<usize>
    where
        Run: FnMut(FlowState) -> RunFuture,
        RunFuture: Future<Output = CatgaResult<FlowResult>>,
    {
        let mut recovered = 0_usize;
        while recovered < options.max_claims {
            let Some(state) = self
                .store
                .try_claim(flow_type, &self.owner, self.stale_after)
                .await?
            else {
                break;
            };
            let result = self.run_work(state.clone(), &mut run).await;
            self.persist_result(state, result).await?;
            recovered = recovered.saturating_add(1);
        }
        Ok(recovered)
    }

    /// Repeats bounded stale-flow recovery until `cancellation` is cancelled.
    ///
    /// The loop is awaited by its caller and creates no background task. A work or store error is
    /// returned to that caller for its normal supervision policy.
    pub async fn run_recovery_loop<Run, RunFuture>(
        &self,
        flow_type: &str,
        options: FlowRecoveryOptions,
        cancellation: CancellationToken,
        mut run: Run,
    ) -> CatgaResult<()>
    where
        Run: FnMut(FlowState) -> RunFuture,
        RunFuture: Future<Output = CatgaResult<FlowResult>>,
    {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.recover_stale(flow_type, options, &mut run).await?;
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(options.poll_interval) => {}
            }
        }
    }

    async fn run_work<Run, RunFuture>(&self, state: FlowState, run: Run) -> FlowResult
    where
        Run: FnOnce(FlowState) -> RunFuture,
        RunFuture: Future<Output = CatgaResult<FlowResult>>,
    {
        match run(state.clone()).await {
            Ok(result) => result,
            Err(error) => FlowResult::failure(state.step(), error),
        }
    }

    async fn persist_result(
        &self,
        state: FlowState,
        result: FlowResult,
    ) -> CatgaResult<FlowResult> {
        let version = state.version();
        let mut current = state;
        loop {
            let next = terminal_state(&current, &result);
            if self.store.update(version, next).await? {
                return Ok(result);
            }

            let Some(observed) = self.store.get(current.id()).await? else {
                return Err(CatgaError::new(
                    ErrorCode::Transient,
                    "flow disappeared while saving its result",
                ));
            };
            if observed.status().is_terminal() {
                if let Some(terminal) = terminal_result(&observed) {
                    return terminal;
                }
                return Err(CatgaError::new(
                    ErrorCode::Internal,
                    "terminal flow state has no result",
                ));
            }
            if observed.status() == FlowStatus::Running
                && observed.owner() == Some(self.owner.as_ref())
                && observed.version() == version
            {
                // A heartbeat renews the physical store record without changing this logical
                // version. Rebuild from it and retry so that the terminal transition retains
                // the renewed heartbeat rather than treating retained ownership as a conflict.
                current = observed;
                continue;
            }
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "flow ownership changed before its result was saved",
            ));
        }
    }

    /// Records liveness for this executor's currently owned state.
    pub async fn heartbeat(&self, id: &str, version: i64) -> CatgaResult<bool> {
        self.store.heartbeat(id, &self.owner, version).await
    }

    async fn claim_or_load(&self, expected: &FlowState) -> CatgaResult<ExistingFlow> {
        let Some(current) = self.store.get(expected.id()).await? else {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "flow disappeared before it could be loaded",
            ));
        };
        if current.flow_type() != expected.flow_type() {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "a flow with this identity has a different type",
            ));
        }
        if current.status().is_terminal()
            && let Some(result) = terminal_result(&current)
        {
            return result.map(ExistingFlow::Complete);
        }
        if !is_stale(current.heartbeat(), self.stale_after) {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "flow is already running on another owner",
            ));
        }
        let claimed = current
            .clone()
            .claimed_by(self.owner.clone())
            .next_version();
        if self
            .store
            .update(current.version(), claimed.clone())
            .await?
        {
            Ok(ExistingFlow::Run(claimed))
        } else {
            self.load_running_or_terminal(expected.id()).await
        }
    }

    async fn load_running_or_terminal(&self, id: &str) -> CatgaResult<ExistingFlow> {
        let Some(current) = self.store.get(id).await? else {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "flow disappeared while ownership was changing",
            ));
        };
        if current.status().is_terminal()
            && let Some(result) = terminal_result(&current)
        {
            return result.map(ExistingFlow::Complete);
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "flow ownership changed before execution began",
        ))
    }
}

fn is_stale(heartbeat: SystemTime, stale_after: Duration) -> bool {
    SystemTime::now()
        .duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}

fn terminal_state(state: &FlowState, result: &FlowResult) -> FlowState {
    match result.error() {
        Some(error) => state
            .clone()
            .at_step(result.completed_steps())
            .failed(error.clone())
            .next_version(),
        None => state.clone().done(result.completed_steps()).next_version(),
    }
}

fn terminal_result(state: &FlowState) -> Option<CatgaResult<FlowResult>> {
    match state.status() {
        FlowStatus::Done => Some(Ok(FlowResult::success(state.step()))),
        FlowStatus::Failed => Some(match state.error() {
            Some(error) => Ok(FlowResult::failure(state.step(), error.clone())),
            None => Err(CatgaError::new(
                ErrorCode::Internal,
                "a failed flow has no stored error",
            )),
        }),
        FlowStatus::Cancelled => Some(Ok(FlowResult::failure(
            state.step(),
            CatgaError::new(ErrorCode::Cancelled, "flow was cancelled"),
        ))),
        FlowStatus::Running | FlowStatus::Compensating | FlowStatus::Suspended => None,
    }
}
