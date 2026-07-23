//! Typed, state-owning flow DSL primitives.

use std::sync::Arc;

use catga_core::CatgaResult;
use futures::future::BoxFuture;

type Action<S> = Box<dyn for<'a> Fn(&'a mut S) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync>;
type Condition<S> = Box<dyn Fn(&S) -> bool + Send + Sync>;
type Merge<S> = Box<dyn Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync>;
type CloneState<S> = fn(&S) -> S;

enum Step<S> {
    Action(Action<S>),
    If {
        condition: Condition<S>,
        then_branch: DslFlow<S>,
        else_branch: DslFlow<S>,
    },
    Parallel {
        branches: Vec<DslFlow<S>>,
        clone_state: CloneState<S>,
        merge: Merge<S>,
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

    /// Appends branches that run concurrently on isolated state copies.
    ///
    /// The merge closure receives branch states in declaration order only after every branch
    /// succeeds. A failed branch leaves the original state unchanged.
    pub fn parallel<I, M>(mut self, branches: I, merge: M) -> Self
    where
        S: Clone,
        I: IntoIterator<Item = Self>,
        M: Fn(&mut S, Vec<S>) -> CatgaResult<()> + Send + Sync + 'static,
    {
        self.steps.push(Step::Parallel {
            branches: branches.into_iter().collect(),
            clone_state: Clone::clone,
            merge: Box::new(merge),
        });
        self
    }

    /// Appends an action that runs sequentially for every item selected from the state.
    ///
    /// The selector returns an owned collection before item actions begin, so no immutable state
    /// borrow remains while an action mutates the state asynchronously.
    pub fn for_each<T, Select, F>(mut self, select: Select, action: F) -> Self
    where
        T: Send + 'static,
        Select: Fn(&S) -> Vec<T> + Send + Sync + 'static,
        F: for<'a> Fn(&'a mut S, T) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static,
    {
        let select = Arc::new(select);
        let action = Arc::new(action);
        self.steps.push(Step::Action(Box::new(move |state| {
            let select = Arc::clone(&select);
            let action = Arc::clone(&action);
            Box::pin(async move {
                for item in select(state) {
                    action(state, item).await?;
                }
                Ok(())
            })
        })));
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
                    Step::Parallel {
                        branches,
                        clone_state,
                        merge,
                    } => {
                        let mut branch_states = branches
                            .iter()
                            .map(|_| clone_state(state))
                            .collect::<Vec<_>>();
                        let results = futures::future::join_all(
                            branches
                                .iter()
                                .zip(branch_states.iter_mut())
                                .map(|(branch, branch_state)| branch.run(branch_state)),
                        )
                        .await;

                        for result in results {
                            result?;
                        }
                        merge(state, branch_states)?;
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
