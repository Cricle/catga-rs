use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::CatgaError;
use serde::{Deserialize, Serialize};

use crate::FlowState;

/// The policy used to decide when a wait condition is complete.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WaitPolicy {
    /// Every expected child must succeed.
    All,
    /// The first successful child completes the condition.
    Any,
}

/// One immutable child result recorded against a wait condition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WaitResult {
    child_id: Box<str>,
    payload: Option<Arc<[u8]>>,
    error: Option<CatgaError>,
}

impl WaitResult {
    fn success(child_id: impl Into<Box<str>>, payload: impl Into<Arc<[u8]>>) -> Self {
        Self {
            child_id: child_id.into(),
            payload: Some(payload.into()),
            error: None,
        }
    }

    fn failure(child_id: impl Into<Box<str>>, error: CatgaError) -> Self {
        Self {
            child_id: child_id.into(),
            payload: None,
            error: Some(error),
        }
    }

    /// Returns the child identity that produced this result.
    pub fn child_id(&self) -> &str {
        &self.child_id
    }

    /// Returns whether the child completed successfully.
    pub const fn is_success(&self) -> bool {
        self.error.is_none()
    }

    /// Returns the successful result payload without copying it.
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    /// Returns shared ownership of the successful payload.
    pub fn shared_payload(&self) -> Option<Arc<[u8]>> {
        self.payload.as_ref().map(Arc::clone)
    }

    /// Returns the failure associated with this child, when any.
    pub fn error(&self) -> Option<&CatgaError> {
        self.error.as_ref()
    }
}

/// Immutable persisted state for a set of child-flow results.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WaitCondition {
    correlation_id: Box<str>,
    policy: WaitPolicy,
    expected_count: u32,
    results: Arc<[WaitResult]>,
    created_at: SystemTime,
    timeout: Duration,
}

impl WaitCondition {
    /// Creates an empty condition waiting for `expected_count` distinct child results.
    pub fn new(
        correlation_id: impl Into<Box<str>>,
        policy: WaitPolicy,
        expected_count: u32,
        created_at: SystemTime,
        timeout: Duration,
    ) -> Self {
        Self {
            correlation_id: correlation_id.into(),
            policy,
            expected_count,
            results: Arc::from([]),
            created_at,
            timeout,
        }
    }

    /// Returns the stable condition identity.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// Returns the configured completion policy.
    pub const fn policy(&self) -> WaitPolicy {
        self.policy
    }

    /// Returns the number of distinct child results expected.
    pub const fn expected_count(&self) -> u32 {
        self.expected_count
    }

    /// Returns the number of distinct child results recorded.
    pub fn completed_count(&self) -> u32 {
        u32::try_from(self.results.len()).unwrap_or(u32::MAX)
    }

    /// Returns recorded child results in insertion order.
    pub fn results(&self) -> &[WaitResult] {
        &self.results
    }

    /// Returns when the condition was created.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the maximum time child results may take.
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Returns whether the condition is expired at `now`.
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        now.duration_since(self.created_at)
            .is_ok_and(|elapsed| elapsed >= self.timeout)
    }

    /// Adds a successful child result unless the child has already reported.
    pub fn record_success(
        &self,
        child_id: impl Into<Box<str>>,
        payload: impl Into<Arc<[u8]>>,
    ) -> Self {
        let child_id = child_id.into();
        if self
            .results
            .iter()
            .any(|result| result.child_id == child_id)
        {
            return self.clone();
        }
        let mut results = Vec::with_capacity(self.results.len().saturating_add(1));
        results.extend(self.results.iter().cloned());
        results.push(WaitResult::success(child_id, payload));
        Self {
            results: Arc::from(results),
            ..self.clone()
        }
    }

    /// Adds a failed child result unless the child has already reported.
    pub fn record_failure(&self, child_id: impl Into<Box<str>>, error: CatgaError) -> Self {
        let child_id = child_id.into();
        if self
            .results
            .iter()
            .any(|result| result.child_id == child_id)
        {
            return self.clone();
        }
        let mut results = Vec::with_capacity(self.results.len().saturating_add(1));
        results.extend(self.results.iter().cloned());
        results.push(WaitResult::failure(child_id, error));
        Self {
            results: Arc::from(results),
            ..self.clone()
        }
    }
}

/// Immutable flow state plus the named step needed to resume it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlowContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<SystemTime>,
    schedule_id: Option<Box<str>>,
    created_at: SystemTime,
}

impl FlowContinuation {
    pub(crate) fn from_legacy(
        state: FlowState,
        step_name: Box<str>,
        wait: Option<WaitCondition>,
        resume_at: Option<SystemTime>,
        schedule_id: Option<Box<str>>,
    ) -> Self {
        Self {
            created_at: state.heartbeat(),
            state,
            step_name,
            wait,
            resume_at,
            schedule_id,
        }
    }

    /// Creates a continuation ready to execute `step_name`.
    pub fn new(state: FlowState, step_name: impl Into<Box<str>>) -> Self {
        Self {
            state,
            step_name: step_name.into(),
            wait: None,
            resume_at: None,
            schedule_id: None,
            created_at: SystemTime::now(),
        }
    }

    /// Creates a continuation suspended on `wait` at `step_name`.
    pub fn waiting(state: FlowState, step_name: impl Into<Box<str>>, wait: WaitCondition) -> Self {
        Self {
            state,
            step_name: step_name.into(),
            wait: Some(wait),
            resume_at: None,
            schedule_id: None,
            created_at: SystemTime::now(),
        }
    }

    /// Returns the immutable durable flow state.
    pub fn state(&self) -> &FlowState {
        &self.state
    }

    /// Returns the registered step name to execute or resume.
    pub fn step_name(&self) -> &str {
        &self.step_name
    }

    /// Returns the active wait condition, when this continuation is waiting.
    pub fn wait(&self) -> Option<&WaitCondition> {
        self.wait.as_ref()
    }

    /// Returns the requested resume time, when this continuation is delayed.
    pub const fn resume_at(&self) -> Option<SystemTime> {
        self.resume_at
    }

    /// Returns when this durable continuation was created.
    pub const fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the cancellation identity of the delayed resume, when it has been scheduled.
    pub fn schedule_id(&self) -> Option<&str> {
        self.schedule_id.as_deref()
    }

    /// Returns a delayed copy that will resume at `resume_at`.
    pub fn delayed_until(self, resume_at: SystemTime) -> Self {
        Self {
            resume_at: Some(resume_at),
            wait: None,
            schedule_id: None,
            ..self
        }
    }

    pub(crate) fn with_schedule_id(self, schedule_id: impl Into<Box<str>>) -> Self {
        Self {
            schedule_id: Some(schedule_id.into()),
            ..self
        }
    }

    /// Returns a copy waiting on `wait`.
    pub fn with_wait(self, wait: WaitCondition) -> Self {
        Self {
            wait: Some(wait),
            resume_at: None,
            schedule_id: None,
            ..self
        }
    }

    /// Returns a ready-to-run copy at the next registered step.
    pub fn at_step(self, step_name: impl Into<Box<str>>) -> Self {
        Self {
            step_name: step_name.into(),
            wait: None,
            resume_at: None,
            schedule_id: None,
            ..self
        }
    }

    /// Clears delay and wait metadata before executing a ready continuation.
    pub fn ready(self) -> Self {
        Self {
            wait: None,
            resume_at: None,
            schedule_id: None,
            ..self
        }
    }

    /// Returns a copy whose flow state has been atomically advanced by a store.
    pub fn with_state(self, state: FlowState) -> Self {
        Self { state, ..self }
    }
}
