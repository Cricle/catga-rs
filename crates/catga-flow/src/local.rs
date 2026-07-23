use std::future::Future;

use catga_core::{CatgaError, CatgaResult};
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
        let mut completed = Vec::with_capacity(self.steps.len());
        for (index, step) in self.steps.iter().enumerate() {
            match (step.run)().await {
                Ok(()) => completed.push(index),
                Err(error) => {
                    for index in completed.into_iter().rev() {
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
        FlowResult::success(u32::try_from(completed.len()).unwrap_or(u32::MAX))
    }
}
