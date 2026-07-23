use std::{
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::{FlowResult, FlowState, FlowStatus, FlowStore};

/// Executes durable flows with optimistic ownership and explicit heartbeats.
pub struct FlowExecutor<S: ?Sized> {
    store: Arc<S>,
    owner: Box<str>,
    stale_after: Duration,
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

        let result = match run(state.clone()).await {
            Ok(result) => result,
            Err(error) => FlowResult::failure(state.step(), error),
        };
        let next = match result.error() {
            Some(error) => state
                .clone()
                .at_step(result.completed_steps())
                .failed(error.clone())
                .next_version(),
            None => state.clone().done(result.completed_steps()).next_version(),
        };
        if self.store.update(state.version(), next).await? {
            return Ok(result);
        }
        self.load_terminal(state.id()).await
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

    async fn load_terminal(&self, id: &str) -> CatgaResult<FlowResult> {
        let Some(current) = self.store.get(id).await? else {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "flow disappeared while saving its result",
            ));
        };
        terminal_result(&current).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "flow ownership changed before its result was saved",
            )
        })?
    }
}

fn is_stale(heartbeat: SystemTime, stale_after: Duration) -> bool {
    SystemTime::now()
        .duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
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
        FlowStatus::Running | FlowStatus::Compensating | FlowStatus::Suspended => None,
    }
}
