use std::future::Future;

use catga_core::{CatgaError, CatgaResult};
use futures::future::BoxFuture;

use crate::{FlowState, WaitCondition};

type StepHandler =
    Box<dyn Fn(FlowState) -> BoxFuture<'static, CatgaResult<FlowStepOutcome>> + Send + Sync>;

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

    /// Creates a delayed-suspension outcome.
    pub const fn suspend_until(resume_at: std::time::SystemTime) -> Self {
        Self::SuspendUntil(resume_at)
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
}

struct RegisteredStep {
    name: Box<str>,
    handler: StepHandler,
}

impl FlowDefinition {
    /// Creates an empty named flow definition.
    pub fn new(name: impl Into<Box<str>>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
        }
    }

    /// Registers one named step handler.
    pub fn step<Handler, HandlerFuture>(
        mut self,
        name: impl Into<Box<str>>,
        handler: Handler,
    ) -> Self
    where
        Handler: Fn(FlowState) -> HandlerFuture + Send + Sync + 'static,
        HandlerFuture: Future<Output = CatgaResult<FlowStepOutcome>> + Send + 'static,
    {
        self.steps.push(RegisteredStep {
            name: name.into(),
            handler: Box::new(move |state| Box::pin(handler(state))),
        });
        self
    }

    /// Returns the registered durable flow type.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn first_step_name(&self) -> Option<&str> {
        self.steps.first().map(|step| step.name.as_ref())
    }

    pub(crate) fn next_step_name(&self, name: &str) -> Option<&str> {
        self.steps
            .iter()
            .position(|step| step.name.as_ref() == name)
            .and_then(|index| self.steps.get(index.saturating_add(1)))
            .map(|step| step.name.as_ref())
    }

    pub(crate) fn has_step(&self, name: &str) -> bool {
        self.steps.iter().any(|step| step.name.as_ref() == name)
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
}
