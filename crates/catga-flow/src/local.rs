use std::{
    future::Future,
    time::{Duration, Instant},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

type Action = Box<dyn Fn() -> BoxFuture<'static, CatgaResult<()>> + Send + Sync>;

/// The outcome of local or durable flow execution.
#[derive(Clone, Debug)]
pub struct FlowResult {
    completed_steps: u32,
    error: Option<CatgaError>,
    elapsed: Duration,
}

impl FlowResult {
    /// Creates a successful result after `completed_steps` actions.
    pub const fn success(completed_steps: u32) -> Self {
        Self {
            completed_steps,
            error: None,
            elapsed: Duration::ZERO,
        }
    }

    /// Creates a failed result retaining the operation error.
    pub fn failure(completed_steps: u32, error: CatgaError) -> Self {
        Self {
            completed_steps,
            error: Some(error),
            elapsed: Duration::ZERO,
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

    /// Returns the caller-observed duration of this flow execution, including compensations.
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    fn with_elapsed(mut self, elapsed: Duration) -> Self {
        self.elapsed = elapsed;
        self
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

    /// Appends a forward action and compensation that share cloneable caller-owned context.
    ///
    /// This is equivalent to [`Self::step`], but clones `context` for each forward or
    /// compensation invocation. It keeps a local flow's resource ownership explicit while
    /// avoiding repeated `Arc::clone` plumbing in application code that uses the same state for
    /// both operations.
    ///
    /// ```
    /// use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    /// use catga_flow::Flow;
    ///
    /// let reserved = Arc::new(AtomicBool::new(false));
    /// let flow = Flow::new("reserve")
    ///     .step_with(
    ///         Arc::clone(&reserved),
    ///         |reserved| async move { reserved.store(true, Ordering::Release); Ok(()) },
    ///         |reserved| async move { reserved.store(false, Ordering::Release); Ok(()) },
    ///     );
    /// # let _ = flow;
    /// ```
    pub fn step_with<Context, Run, Compensate, RunFuture, CompensateFuture>(
        self,
        context: Context,
        run: Run,
        compensate: Compensate,
    ) -> Self
    where
        Context: Clone + Send + Sync + 'static,
        Run: Fn(Context) -> RunFuture + Send + Sync + 'static,
        Compensate: Fn(Context) -> CompensateFuture + Send + Sync + 'static,
        RunFuture: Future<Output = CatgaResult<()>> + Send + 'static,
        CompensateFuture: Future<Output = CatgaResult<()>> + Send + 'static,
    {
        let run_context = context.clone();
        self.step(
            move || run(run_context.clone()),
            move || compensate(context.clone()),
        )
    }

    /// Executes forward actions and compensates completed actions after a failure.
    pub async fn run(self) -> FlowResult {
        self.run_from(0, usize::MAX).await
    }

    /// Executes forward actions until `cancellation` is cancelled, compensating completed actions
    /// in reverse order when cancellation wins.
    ///
    /// This is the explicit cooperative-cancellation form of [`Self::run`]. It does not spawn a
    /// task: cancellation drops the currently running action future, runs only compensations for
    /// actions that already succeeded, and returns an [`ErrorCode::Cancelled`] result. Dropping
    /// the returned future directly retains the ordinary Rust cancellation semantics and does not
    /// invoke compensations because no caller-owned cancellation signal was observed.
    pub async fn run_until_cancelled(self, cancellation: CancellationToken) -> FlowResult {
        self.run_from_with_cancellation(0, usize::MAX, Some(&cancellation))
            .await
    }

    /// Resumes forward execution at `start_step` and compensates at most `max_compensations`
    /// successful steps from this invocation after a later failure.
    ///
    /// Steps before `start_step` are caller-owned prior work and are never compensated here. An
    /// out-of-range restart point returns a validation failure without invoking any action.
    pub async fn run_from(self, start_step: usize, max_compensations: usize) -> FlowResult {
        self.run_from_with_cancellation(start_step, max_compensations, None)
            .await
    }

    async fn run_from_with_cancellation(
        self,
        start_step: usize,
        max_compensations: usize,
        cancellation: Option<&CancellationToken>,
    ) -> FlowResult {
        let started = Instant::now();
        if start_step > self.steps.len() {
            return FlowResult::failure(
                u32::try_from(self.steps.len()).unwrap_or(u32::MAX),
                CatgaError::new(ErrorCode::Validation, "flow restart step is out of range"),
            )
            .with_elapsed(started.elapsed());
        }
        let mut completed = Vec::with_capacity(self.steps.len());
        for (index, step) in self.steps.iter().enumerate().skip(start_step) {
            if cancellation.is_some_and(CancellationToken::is_cancelled) {
                self.compensate(completed, max_compensations).await;
                return FlowResult::failure(
                    u32::try_from(index).unwrap_or(u32::MAX),
                    CatgaError::new(ErrorCode::Cancelled, "local flow execution was cancelled"),
                )
                .with_elapsed(started.elapsed());
            }
            let result = match cancellation {
                Some(cancellation) => {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => Err(CatgaError::new(
                            ErrorCode::Cancelled,
                            "local flow execution was cancelled",
                        )),
                        result = (step.run)() => result,
                    }
                }
                None => (step.run)().await,
            };
            match result {
                Ok(()) => completed.push(index),
                Err(error) => {
                    self.compensate(completed, max_compensations).await;
                    return FlowResult::failure(u32::try_from(index).unwrap_or(u32::MAX), error)
                        .with_elapsed(started.elapsed());
                }
            }
        }
        FlowResult::success(u32::try_from(self.steps.len()).unwrap_or(u32::MAX))
            .with_elapsed(started.elapsed())
    }

    async fn compensate(&self, completed: Vec<usize>, max_compensations: usize) {
        for index in completed.into_iter().rev().take(max_compensations) {
            if let Err(compensation_error) = (self.steps[index].compensate)().await {
                tracing::warn!(
                    flow = self.name.as_ref(),
                    error = compensation_error.message(),
                    "flow compensation failed"
                );
            }
        }
    }
}
