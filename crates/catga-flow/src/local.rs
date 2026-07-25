use std::future::Future;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::future::BoxFuture;

type Action = Box<dyn Fn() -> BoxFuture<'static, CatgaResult<()>> + Send + Sync>;

/// The outcome of local or durable flow execution.
#[derive(Clone, Debug)]
pub struct FlowResult {
    completed_steps: u32,
    error: Option<CatgaError>,
}

impl FlowResult {
    /// Creates a successful result after `completed_steps` actions.
    pub const fn success(completed_steps: u32) -> Self {
        Self {
            completed_steps,
            error: None,
        }
    }

    /// Creates a failed result retaining the operation error.
    pub fn failure(completed_steps: u32, error: CatgaError) -> Self {
        Self {
            completed_steps,
            error: Some(error),
        }
    }

    /// Returns whether all actions completed successfully.
    pub const fn is_success(&self) -> bool {
        self.error.is_none()
    }

    /// Returns how many forward actions completed before this result.
    pub const fn completed_steps(&self) -> u32 {
        self.completed_steps
    }

    /// Returns the operation error when execution failed.
    pub fn error(&self) -> Option<&CatgaError> {
        self.error.as_ref()
    }
}

/// A local sequence of operations with reverse-order compensation.
pub struct Flow {
    name: Box<str>,
    steps: Vec<FlowStep>,
}

struct FlowStep {
    run: Action,
    compensate: Action,
}

impl Flow {
    /// Starts a named local flow.
    pub fn new(name: impl Into<Box<str>>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
        }
    }

    /// Appends a forward action and the action that undoes it on later failure.
    pub fn step<Run, Compensate, RunFuture, CompensateFuture>(
        mut self,
        run: Run,
        compensate: Compensate,
    ) -> Self
    where
        Run: Fn() -> RunFuture + Send + Sync + 'static,
        Compensate: Fn() -> CompensateFuture + Send + Sync + 'static,
        RunFuture: Future<Output = CatgaResult<()>> + Send + 'static,
        CompensateFuture: Future<Output = CatgaResult<()>> + Send + 'static,
    {
        self.steps.push(FlowStep {
            run: Box::new(move || Box::pin(run())),
            compensate: Box::new(move || Box::pin(compensate())),
        });
        self
    }

    /// Executes forward actions and compensates completed actions after a failure.
    pub async fn run(self) -> FlowResult {
        self.run_from(0, usize::MAX).await
    }

    /// Resumes forward execution at `start_step` and compensates at most `max_compensations`
    /// successful steps from this invocation after a later failure.
    ///
    /// Steps before `start_step` are caller-owned prior work and are never compensated here. An
    /// out-of-range restart point returns a validation failure without invoking any action.
    pub async fn run_from(self, start_step: usize, max_compensations: usize) -> FlowResult {
        if start_step > self.steps.len() {
            return FlowResult::failure(
                u32::try_from(self.steps.len()).unwrap_or(u32::MAX),
                CatgaError::new(ErrorCode::Validation, "flow restart step is out of range"),
            );
        }
        let mut completed = Vec::with_capacity(self.steps.len());
        for (index, step) in self.steps.iter().enumerate().skip(start_step) {
            match (step.run)().await {
                Ok(()) => completed.push(index),
                Err(error) => {
                    for index in completed.into_iter().rev().take(max_compensations) {
                        if let Err(compensation_error) = (self.steps[index].compensate)().await {
                            tracing::warn!(
                                flow = self.name.as_ref(),
                                error = compensation_error.message(),
                                "flow compensation failed"
                            );
                        }
                    }
                    return FlowResult::failure(u32::try_from(index).unwrap_or(u32::MAX), error);
                }
            }
        }
        FlowResult::success(u32::try_from(self.steps.len()).unwrap_or(u32::MAX))
    }
}
