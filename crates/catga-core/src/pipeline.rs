use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{CatgaError, CatgaResult, Command, ErrorCode, Request};

/// Maximum number of behaviors that can wrap one request dispatch.
///
/// This preserves the upstream's fixed recursion bound and prevents an accidentally generated
/// behavior list from consuming unbounded startup memory or overflowing the dispatch chain.
pub const MAX_PIPELINE_DEPTH: usize = 100;

type Continuation<M, R> = dyn Fn(M) -> BoxFuture<'static, CatgaResult<R>> + Send + Sync;

/// One wrapped behavior stage shared by every dispatch through a pipeline.
///
/// Request behaviors ([`Behavior`]) and command behaviors ([`CommandBehavior`]) are adapted to
/// this single closure shape when they are added to a [`Pipeline`], so both message roles reuse
/// one dispatch implementation.
type Stage<M, R> = Arc<dyn Fn(M, Next<M, R>) -> BoxFuture<'static, CatgaResult<R>> + Send + Sync>;

/// Internal position of one pipeline dispatch.
///
/// `Terminal` holds the registered handler continuation. `Chain` shares the immutable stage
/// slice and the current depth, so cloning a [`Next`] or wrapping a pipeline performs cheap
/// reference-count increments instead of allocating a fresh closure chain per dispatch.
enum NextInner<M, R> {
    Terminal(Arc<Continuation<M, R>>),
    Chain {
        stages: Arc<[Stage<M, R>]>,
        index: usize,
        terminal: Arc<Continuation<M, R>>,
    },
}

impl<M, R> Clone for NextInner<M, R> {
    fn clone(&self) -> Self {
        match self {
            NextInner::Terminal(continuation) => NextInner::Terminal(Arc::clone(continuation)),
            NextInner::Chain {
                stages,
                index,
                terminal,
            } => NextInner::Chain {
                stages: Arc::clone(stages),
                index: *index,
                terminal: Arc::clone(terminal),
            },
        }
    }
}

/// Invokes the next behavior or the registered handler in a pipeline.
///
/// `R` is the response produced at the end of the chain: [`Request::Response`] for request
/// pipelines (the default) and `()` for command pipelines, where [`CommandNext`] binds this
/// same type to the unit response.
pub struct Next<M, R = <M as Request>::Response> {
    inner: NextInner<M, R>,
}

impl<M, R> Clone for Next<M, R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M, R> Next<M, R> {
    pub(crate) fn new(
        continuation: impl Fn(M) -> BoxFuture<'static, CatgaResult<R>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: NextInner::Terminal(Arc::new(continuation)),
        }
    }

    /// Continues processing with the supplied message.
    pub fn run(&self, message: M) -> BoxFuture<'static, CatgaResult<R>> {
        match &self.inner {
            NextInner::Terminal(continuation) => continuation(message),
            NextInner::Chain {
                stages,
                index,
                terminal,
            } => {
                let Some(stage) = stages.get(*index) else {
                    return terminal(message);
                };
                let stage = Arc::clone(stage);
                let next = Next {
                    inner: NextInner::Chain {
                        stages: Arc::clone(stages),
                        index: *index + 1,
                        terminal: Arc::clone(terminal),
                    },
                };
                stage(message, next)
            }
        }
    }
}

/// The dispatch handle handed to [`CommandBehavior`] implementations.
///
/// This is the unit-response specialization of [`Next`]: command chains always complete with
/// `CatgaResult<()>`.
pub type CommandNext<C> = Next<C, ()>;

/// Wraps typed request processing before and after the next pipeline stage.
///
/// Command dispatch shares the same pipeline machinery through the unit-response
/// [`CommandBehavior`] contract.
#[async_trait]
pub trait Behavior<M: Request>: Send + Sync {
    /// Handles a request and optionally invokes the next behavior or request handler.
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response>;
}

/// Wraps typed command processing before and after the next pipeline stage.
///
/// Commands produce no response, so this contract fixes the pipeline response to `()`. It
/// remains a distinct trait to keep a [`Command`] from being represented as an artificial
/// `Request<Response = ()>` and to keep handler registration type-safe; dispatch itself reuses
/// the [`Behavior`] machinery through [`CommandPipeline`].
#[async_trait]
pub trait CommandBehavior<C: Command>: Send + Sync {
    /// Handles a command and optionally invokes the next behavior or command handler.
    async fn handle(&self, command: C, next: CommandNext<C>) -> CatgaResult<()>;
}

/// Adapts one shared request behavior to the generic pipeline stage shape.
fn request_stage<M: Request>(behavior: Arc<dyn Behavior<M>>) -> Stage<M, M::Response> {
    Arc::new(move |message, next| {
        let behavior = Arc::clone(&behavior);
        Box::pin(async move { behavior.handle(message, next).await })
    })
}

/// Adapts one shared command behavior to the generic pipeline stage shape.
fn command_stage<C: Command>(behavior: Arc<dyn CommandBehavior<C>>) -> Stage<C, ()> {
    Arc::new(move |command, next| {
        let behavior = Arc::clone(&behavior);
        Box::pin(async move { behavior.handle(command, next).await })
    })
}

fn depth_exceeded() -> CatgaError {
    CatgaError::new(
        ErrorCode::Validation,
        "pipeline depth exceeds the supported maximum",
    )
}

/// An immutable, typed sequence of request behaviors built during application startup.
///
/// `R` is the response produced at the end of the chain and defaults to
/// [`Request::Response`], so `Pipeline<M>` describes a request pipeline. Command dispatch
/// binds the same machinery to a unit response through [`CommandPipeline`].
///
/// ```
/// use std::time::Duration;
/// use catga_core::{Pipeline, RetryBehavior, TimeoutBehavior, Message, Request};
///
///
/// #[derive(Clone)]
/// struct MyRequest;
/// impl Message for MyRequest {}
/// impl Request for MyRequest { type Response = (); }
///
/// let pipeline: Pipeline<MyRequest> = Pipeline::new()
///     .with(RetryBehavior::new(2, Duration::from_millis(10)))
///     .with(TimeoutBehavior::new(Duration::from_secs(1)));
/// assert_eq!(pipeline.len(), 2);
/// assert!(!pipeline.is_empty());
/// ```
pub struct Pipeline<M, R = <M as Request>::Response> {
    stages: Vec<Stage<M, R>>,
    /// Shared behavior chain materialized lazily on the first dispatch.
    ///
    /// Wrapping the pipeline into a [`Next`] clones only this slice's reference count, so
    /// per-request dispatch does not reallocate the behavior chain.
    chain: OnceLock<Arc<[Stage<M, R>]>>,
}

impl<M, R> Default for Pipeline<M, R> {
    fn default() -> Self {
        Self {
            stages: Vec::new(),
            chain: OnceLock::new(),
        }
    }
}

impl<M: 'static, R: 'static> Pipeline<M, R> {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of configured behaviors.
    pub const fn len(&self) -> usize {
        self.stages.len()
    }

    /// Returns whether this pipeline has no configured behaviors.
    pub const fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    pub(crate) fn wrap(&self, terminal: Next<M, R>) -> Next<M, R> {
        let stages = Arc::clone(self.chain.get_or_init(|| Arc::from(self.stages.clone())));
        if stages.is_empty() {
            return terminal;
        }
        let terminal = match terminal.inner {
            NextInner::Terminal(continuation) => continuation,
            NextInner::Chain { .. } => Arc::new(move |message| terminal.run(message)),
        };
        Next {
            inner: NextInner::Chain {
                stages,
                index: 0,
                terminal,
            },
        }
    }
}

impl<M: Request> Pipeline<M, M::Response> {
    /// Adds a behavior after the existing stages.
    pub fn with<B>(mut self, behavior: B) -> Self
    where
        B: Behavior<M> + 'static,
    {
        self.stages.push(request_stage(Arc::new(behavior)));
        self
    }

    /// Adds a shared behavior after the existing stages.
    pub fn with_shared(mut self, behavior: Arc<dyn Behavior<M>>) -> Self {
        self.stages.push(request_stage(behavior));
        self
    }

    /// Adds a behavior while rejecting a pipeline deeper than [`MAX_PIPELINE_DEPTH`].
    ///
    /// Prefer this fallible builder for generated or configuration-driven pipelines. The legacy
    /// [`Self::with`] builder remains available for source compatibility; dispatch still rejects
    /// any oversized legacy pipeline before invoking a behavior or handler.
    pub fn try_with<B>(self, behavior: B) -> CatgaResult<Self>
    where
        B: Behavior<M> + 'static,
    {
        self.try_with_shared(Arc::new(behavior))
    }

    /// Adds a shared behavior while enforcing [`MAX_PIPELINE_DEPTH`].
    pub fn try_with_shared(mut self, behavior: Arc<dyn Behavior<M>>) -> CatgaResult<Self> {
        if self.stages.len() >= MAX_PIPELINE_DEPTH {
            return Err(depth_exceeded());
        }
        self.stages.push(request_stage(behavior));
        Ok(self)
    }
}

/// An immutable, typed sequence of command behaviors built during application startup.
///
/// This is the command-shaped view over [`Pipeline<C, ()>`]: commands produce no response, so
/// this wrapper accepts [`CommandBehavior`] stages while dispatch reuses the same generic
/// pipeline machinery as requests.
pub struct CommandPipeline<C: Command> {
    inner: Pipeline<C, ()>,
}

impl<C: Command> Default for CommandPipeline<C> {
    fn default() -> Self {
        Self {
            inner: Pipeline::new(),
        }
    }
}

impl<C: Command> CommandPipeline<C> {
    /// Creates an empty command pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a command behavior after the existing stages.
    pub fn with<B>(mut self, behavior: B) -> Self
    where
        B: CommandBehavior<C> + 'static,
    {
        self.inner.stages.push(command_stage(Arc::new(behavior)));
        self
    }

    /// Adds a shared command behavior after the existing stages.
    pub fn with_shared(mut self, behavior: Arc<dyn CommandBehavior<C>>) -> Self {
        self.inner.stages.push(command_stage(behavior));
        self
    }

    /// Adds a command behavior while enforcing [`MAX_PIPELINE_DEPTH`].
    pub fn try_with<B>(self, behavior: B) -> CatgaResult<Self>
    where
        B: CommandBehavior<C> + 'static,
    {
        self.try_with_shared(Arc::new(behavior))
    }

    /// Adds a shared command behavior while enforcing [`MAX_PIPELINE_DEPTH`].
    pub fn try_with_shared(mut self, behavior: Arc<dyn CommandBehavior<C>>) -> CatgaResult<Self> {
        if self.inner.stages.len() >= MAX_PIPELINE_DEPTH {
            return Err(depth_exceeded());
        }
        self.inner.stages.push(command_stage(behavior));
        Ok(self)
    }

    /// Returns the number of configured command behaviors.
    pub const fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns whether this pipeline has no configured command behaviors.
    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn wrap(&self, terminal: CommandNext<C>) -> CommandNext<C> {
        self.inner.wrap(terminal)
    }
}
