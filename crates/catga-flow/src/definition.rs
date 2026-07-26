use std::{
    future::Future,
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::future::BoxFuture;

use crate::{FlowState, WaitCondition};

type StepHandler =
    Box<dyn Fn(FlowState) -> BoxFuture<'static, CatgaResult<FlowStepOutcome>> + Send + Sync>;
type StepCompensation = Box<dyn Fn(FlowState) -> BoxFuture<'static, CatgaResult<()>> + Send + Sync>;

/// The outcome returned by one registered durable flow step.
#[derive(Clone, Debug)]
pub enum FlowStepOutcome {
    /// Continue with the following registered step.
    Advance,
    /// Persist and execute the specified registered step.
    Goto(Box<str>),
    /// Persist the next step until `resume_at`.
    SuspendUntil(std::time::SystemTime),
    /// Persist the next step until a wait condition is complete.
    Wait(WaitCondition),
    /// Mark the flow successfully completed.
    Complete,
    /// Mark the flow failed with this business error.
    Fail(CatgaError),
}

impl FlowStepOutcome {
    /// Creates a durable transition to a named registered step.
    pub fn goto(step_name: impl Into<Box<str>>) -> Self {
        Self::Goto(step_name.into())
    }

    /// Creates an absolute, durable scheduled-resume outcome.
    ///
    /// This is the Rust counterpart of a `ScheduleAt` flow step. The runtime
    /// persists the following named step and delegates the wake-up to its
    /// [`crate::FlowScheduler`], rather than retaining a process-local timer.
    /// The supplied time is expressed as [`SystemTime`] so callers must choose
    /// the desired wall-clock policy explicitly.
    pub const fn suspend_until(resume_at: std::time::SystemTime) -> Self {
        Self::SuspendUntil(resume_at)
    }

    /// Creates a durable delayed suspension relative to the current wall clock.
    ///
    /// This is the Rust counterpart of a `Delay` flow step. A zero duration
    /// advances immediately, avoiding an unnecessary durable write and
    /// scheduler operation. A positive duration is converted to an absolute
    /// scheduled resume and is therefore recoverable after process restart.
    pub fn delay(duration: Duration) -> CatgaResult<Self> {
        if duration.is_zero() {
            return Ok(Self::Advance);
        }
        let resume_at = SystemTime::now().checked_add(duration).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "flow delay exceeds the supported system time range",
            )
        })?;
        Ok(Self::SuspendUntil(resume_at))
    }

    /// Creates a successful terminal outcome.
    pub const fn complete() -> Self {
        Self::Complete
    }

    /// Creates an external-result wait outcome.
    pub fn wait(condition: WaitCondition) -> Self {
        Self::Wait(condition)
    }
}

/// An ordered, process-local registry of durable step handlers.
pub struct FlowDefinition {
    name: Box<str>,
    steps: Vec<RegisteredStep>,
    invalid_step_names: bool,
}

struct RegisteredStep {
    name: Box<str>,
    tag: Option<Box<str>>,
    handler: StepHandler,
    compensation: Option<StepCompensation>,
}

impl FlowDefinition {
    /// Creates an empty named flow definition.
    pub fn new(name: impl Into<Box<str>>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            invalid_step_names: false,
        }
    }

    /// Registers one named step handler.
    pub fn step<Handler, HandlerFuture>(self, name: impl Into<Box<str>>, handler: Handler) -> Self
    where
        Handler: Fn(FlowState) -> HandlerFuture + Send + Sync + 'static,
        HandlerFuture: Future<Output = CatgaResult<FlowStepOutcome>> + Send + 'static,
    {
        self.register(RegisteredStep {
            name: name.into(),
            tag: None,
            handler: Box::new(move |state| Box::pin(handler(state))),
            compensation: None,
        })
    }

    /// Registers one named durable step and its idempotent rollback action.
    ///
    /// After the forward action reports a successful non-terminal transition, the runtime
    /// records this step name in the continuation. If a later step fails, rollback actions run
    /// in reverse completion order. A rollback failure leaves the continuation in its durable
    /// compensating phase so a later stale-owner recovery retries the same action.
    pub fn step_with_compensation<Handler, HandlerFuture, Compensate, CompensateFuture>(
        self,
        name: impl Into<Box<str>>,
        handler: Handler,
        compensate: Compensate,
    ) -> Self
    where
        Handler: Fn(FlowState) -> HandlerFuture + Send + Sync + 'static,
        HandlerFuture: Future<Output = CatgaResult<FlowStepOutcome>> + Send + 'static,
        Compensate: Fn(FlowState) -> CompensateFuture + Send + Sync + 'static,
        CompensateFuture: Future<Output = CatgaResult<()>> + Send + 'static,
    {
        self.register(RegisteredStep {
            name: name.into(),
            tag: None,
            handler: Box::new(move |state| Box::pin(handler(state))),
            compensation: Some(Box::new(move |state| Box::pin(compensate(state)))),
        })
    }

    /// Registers one named durable step with a static policy tag.
    ///
    /// Tags select explicit [`crate::FlowTagPolicy`] timeout and retry rules at execution time.
    /// They do not make a durable transition optional: every [`FlowRuntime`](crate::FlowRuntime)
    /// transition remains persisted so restart recovery cannot silently skip work.
    pub fn step_with_tag<Handler, HandlerFuture>(
        self,
        name: impl Into<Box<str>>,
        tag: impl Into<Box<str>>,
        handler: Handler,
    ) -> Self
    where
        Handler: Fn(FlowState) -> HandlerFuture + Send + Sync + 'static,
        HandlerFuture: Future<Output = CatgaResult<FlowStepOutcome>> + Send + 'static,
    {
        self.register(RegisteredStep {
            name: name.into(),
            tag: Some(tag.into()),
            handler: Box::new(move |state| Box::pin(handler(state))),
            compensation: None,
        })
    }

    /// Returns the registered durable flow type.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn first_step_name(&self) -> Option<&str> {
        self.steps.first().map(|step| step.name.as_ref())
    }

    /// Validates the definition before it is used to create or resume durable state.
    ///
    /// Named continuations resolve both a handler and its successor by name, so names must be
    /// non-empty and unique. The builder records this condition while registering steps, making
    /// runtime validation constant-time and allocation-free.
    pub(crate) fn validate(&self) -> CatgaResult<()> {
        if self.name.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "a flow definition requires a non-empty name",
            ));
        }
        if self.invalid_step_names {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow definition step names must be non-empty and unique",
            ));
        }
        Ok(())
    }

    pub(crate) fn next_step_name(&self, name: &str) -> Option<&str> {
        self.steps
            .iter()
            .position(|step| step.name.as_ref() == name)
            .and_then(|index| self.steps.get(index.saturating_add(1)))
            .map(|step| step.name.as_ref())
    }

    /// Returns whether a step with `name` is registered.
    pub fn has_step(&self, name: &str) -> bool {
        self.steps.iter().any(|step| step.name.as_ref() == name)
    }

    pub(crate) fn step_tag(&self, name: &str) -> Option<&str> {
        self.steps
            .iter()
            .find(|step| step.name.as_ref() == name)
            .and_then(|step| step.tag.as_deref())
    }

    pub(crate) async fn execute(
        &self,
        name: &str,
        state: FlowState,
    ) -> CatgaResult<FlowStepOutcome> {
        let Some(step) = self.steps.iter().find(|step| step.name.as_ref() == name) else {
            return Err(CatgaError::new(
                catga_core::ErrorCode::NotFound,
                "flow continuation references an unregistered step",
            ));
        };
        (step.handler)(state).await
    }

    pub(crate) fn has_compensation(&self, name: &str) -> bool {
        self.steps
            .iter()
            .find(|step| step.name.as_ref() == name)
            .is_some_and(|step| step.compensation.is_some())
    }

    pub(crate) async fn compensate(&self, name: &str, state: FlowState) -> CatgaResult<()> {
        let Some(step) = self.steps.iter().find(|step| step.name.as_ref() == name) else {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "flow compensation references an unregistered step",
            ));
        };
        let Some(compensation) = step.compensation.as_ref() else {
            return Err(CatgaError::new(
                ErrorCode::NotFound,
                "flow compensation handler is no longer registered for a completed step",
            ));
        };
        compensation(state).await
    }

    fn register(mut self, step: RegisteredStep) -> Self {
        self.invalid_step_names |= step.name.is_empty()
            || self
                .steps
                .iter()
                .any(|registered| registered.name == step.name);
        self.steps.push(step);
        self
    }
}
