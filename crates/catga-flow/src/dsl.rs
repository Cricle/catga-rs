//! Typed, state-owning flow DSL primitives.

use catga_core::CatgaResult;
use futures::future::BoxFuture;

type Action<S> = Box<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
type Condition<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;

enum Step<S> {
    Action(Action<S>),
    If {
        condition: Condition<S>,
        then_branch: DslFlow<S>,
        else_branch: DslFlow<S>,
    },
}

/// Composable stateful DSL flow with deterministic conditional branches.
pub struct DslFlow<S> {
    steps: Vec<Step<S>>,
}

impl<S: Send> DslFlow<S> {
    /// Creates an empty DSL flow.
    pub const fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Appends one state-mutating asynchronous action.
    pub fn action<F>(mut self, action: F) -> Self
    where
        F: for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        self.steps.push(Step::Action(Box::new(action)));
        self
    }

    /// Appends a branch that runs exactly one nested flow against the same state.
    pub fn if_else<C>(mut self, condition: C, then_branch: Self, else_branch: Self) -> Self
    where
        C: Fn(&S) -> bool + Send + Sync + 'static,
    {
        self.steps.push(Step::If {
            condition: Box::new(condition),
            then_branch,
            else_branch,
        });
        self
    }

    /// Runs all selected steps against one mutable state value.
    pub fn run<'a>(&'a self, state: &'a mut S) -> BoxFuture<'a, CatgaResult<()>> {
        Box::pin(async move {
            for step in &self.steps {
                match step {
                    Step::Action(action) => action(state).await?,
                    Step::If {
                        condition,
                        then_branch,
                        else_branch,
                    } => {
                        if condition(state) {
                            then_branch.run(state).await?;
                        } else {
                            else_branch.run(state).await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }
}

impl<S: Send> Default for DslFlow<S> {
    fn default() -> Self {
        Self::new()
    }
}
