use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{CatgaError, CatgaResult, Command, ErrorCode, Request};

/// Maximum number of behaviors that can wrap one request dispatch.
///
/// This preserves the upstream's fixed recursion bound and prevents an accidentally generated
/// behavior list from consuming unbounded startup memory or overflowing the dispatch chain.
pub const MAX_PIPELINE_DEPTH: usize = 100;

type Continuation<M> =
    dyn Fn(M) -> BoxFuture<'static, CatgaResult<<M as Request>::Response>> + Send + Sync;

/// Internal position of one pipeline dispatch.
///
/// `Terminal` holds the registered handler continuation. `Chain` shares the immutable behavior
/// slice and the current depth, so cloning a [`Next`] or wrapping a pipeline performs cheap
/// reference-count increments instead of allocating a fresh closure chain per dispatch.
enum NextInner<M: Request> {
    Terminal(Arc<Continuation<M>>),
    Chain {
        behaviors: Arc<[Arc<dyn Behavior<M>>]>,
        index: usize,
        terminal: Arc<Continuation<M>>,
    },
}

impl<M: Request> Clone for NextInner<M> {
    fn clone(&self) -> Self {
        match self {
            NextInner::Terminal(continuation) => NextInner::Terminal(Arc::clone(continuation)),
            NextInner::Chain {
                behaviors,
                index,
                terminal,
            } => NextInner::Chain {
                behaviors: Arc::clone(behaviors),
                index: *index,
                terminal: Arc::clone(terminal),
            },
        }
    }
}

/// Invokes the next behavior or the registered request handler in a pipeline.
pub struct Next<M: Request> {
    inner: NextInner<M>,
}

impl<M: Request> Clone for Next<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: Request> Next<M> {
    pub(crate) fn new(
        continuation: impl Fn(M) -> BoxFuture<'static, CatgaResult<M::Response>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: NextInner::Terminal(Arc::new(continuation)),
        }
    }

    /// Continues request processing with the supplied message.
    pub fn run(&self, message: M) -> BoxFuture<'static, CatgaResult<M::Response>> {
        match &self.inner {
            NextInner::Terminal(continuation) => continuation(message),
            NextInner::Chain {
                behaviors,
                index,
                terminal,
            } => {
                let Some(behavior) = behaviors.get(*index) else {
                    return terminal(message);
                };
                let behavior = Arc::clone(behavior);
                let next = Next {
                    inner: NextInner::Chain {
                        behaviors: Arc::clone(behaviors),
                        index: *index + 1,
                        terminal: Arc::clone(terminal),
                    },
                };
                Box::pin(async move { behavior.handle(message, next).await })
            }
        }
    }
}

/// Wraps typed request processing before and after the next pipeline stage.
#[async_trait]
pub trait Behavior<M: Request>: Send + Sync {
    /// Handles a request and optionally invokes the next behavior or request handler.
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response>;
}

/// An immutable, typed sequence of request behaviors built during application startup.
///
/// ```
/// use std::time::Duration;
/// use catga_core::{Pipeline, RetryBehavior, TimeoutBehavior, Message, MessageTypeId, Request};
///
/// struct MyRequestTypeId;
/// impl MessageTypeId for MyRequestTypeId { const NAME: &'static str = "MyRequest"; }
///
/// #[derive(Clone)]
/// struct MyRequest;
/// impl Message for MyRequest {}
/// impl Request for MyRequest { type Response = (); type TypeId = MyRequestTypeId; }
///
/// let pipeline: Pipeline<MyRequest> = Pipeline::new()
///     .with(RetryBehavior::new(2, Duration::from_millis(10)))
///     .with(TimeoutBehavior::new(Duration::from_secs(1)));
/// assert_eq!(pipeline.len(), 2);
/// assert!(!pipeline.is_empty());
/// ```
pub struct Pipeline<M: Request> {
    behaviors: Vec<Arc<dyn Behavior<M>>>,
    /// Shared behavior chain materialized lazily on the first dispatch.
    ///
    /// Wrapping the pipeline into a [`Next`] clones only this slice's reference count, so
    /// per-request dispatch does not reallocate the behavior chain.
    chain: OnceLock<Arc<[Arc<dyn Behavior<M>>]>>,
}

impl<M: Request> Default for Pipeline<M> {
    fn default() -> Self {
        Self {
            behaviors: Vec::new(),
            chain: OnceLock::new(),
        }
    }
}

impl<M: Request> Pipeline<M> {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a behavior after the existing stages.
    pub fn with<B>(mut self, behavior: B) -> Self
    where
        B: Behavior<M> + 'static,
    {
        self.behaviors.push(Arc::new(behavior));
        self
    }

    /// Adds a shared behavior after the existing stages.
    pub fn with_shared(mut self, behavior: Arc<dyn Behavior<M>>) -> Self {
        self.behaviors.push(behavior);
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
        if self.behaviors.len() >= MAX_PIPELINE_DEPTH {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "pipeline depth exceeds the supported maximum",
            ));
        }
        self.behaviors.push(behavior);
        Ok(self)
    }

    /// Returns the number of configured behaviors.
    pub const fn len(&self) -> usize {
        self.behaviors.len()
    }

    /// Returns whether this pipeline has no configured behaviors.
    pub const fn is_empty(&self) -> bool {
        self.behaviors.is_empty()
    }

    pub(crate) fn wrap(&self, terminal: Next<M>) -> Next<M> {
        let behaviors = Arc::clone(self.chain.get_or_init(|| Arc::from(self.behaviors.clone())));
        if behaviors.is_empty() {
            return terminal;
        }
        let terminal = match terminal.inner {
            NextInner::Terminal(continuation) => continuation,
            NextInner::Chain { .. } => Arc::new(move |message| terminal.run(message)),
        };
        Next {
            inner: NextInner::Chain {
                behaviors,
                index: 0,
                terminal,
            },
        }
    }
}

type CommandContinuation<C> = dyn Fn(C) -> BoxFuture<'static, CatgaResult<()>> + Send + Sync;

/// Internal position of one command pipeline dispatch.
enum CommandNextInner<C: Command> {
    Terminal(Arc<CommandContinuation<C>>),
    Chain {
        behaviors: Arc<[Arc<dyn CommandBehavior<C>>]>,
        index: usize,
        terminal: Arc<CommandContinuation<C>>,
    },
}

impl<C: Command> Clone for CommandNextInner<C> {
    fn clone(&self) -> Self {
        match self {
            CommandNextInner::Terminal(continuation) => {
                CommandNextInner::Terminal(Arc::clone(continuation))
            }
            CommandNextInner::Chain {
                behaviors,
                index,
                terminal,
            } => CommandNextInner::Chain {
                behaviors: Arc::clone(behaviors),
                index: *index,
                terminal: Arc::clone(terminal),
            },
        }
    }
}

/// Invokes the next behavior or the registered command handler in a command pipeline.
pub struct CommandNext<C: Command> {
    inner: CommandNextInner<C>,
}

impl<C: Command> Clone for CommandNext<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<C: Command> CommandNext<C> {
    pub(crate) fn new(
        continuation: impl Fn(C) -> BoxFuture<'static, CatgaResult<()>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: CommandNextInner::Terminal(Arc::new(continuation)),
        }
    }

    /// Continues command processing with the supplied command.
    pub fn run(&self, command: C) -> BoxFuture<'static, CatgaResult<()>> {
        match &self.inner {
            CommandNextInner::Terminal(continuation) => continuation(command),
            CommandNextInner::Chain {
                behaviors,
                index,
                terminal,
            } => {
                let Some(behavior) = behaviors.get(*index) else {
                    return terminal(command);
                };
                let behavior = Arc::clone(behavior);
                let next = CommandNext {
                    inner: CommandNextInner::Chain {
                        behaviors: Arc::clone(behaviors),
                        index: *index + 1,
                        terminal: Arc::clone(terminal),
                    },
                };
                Box::pin(async move { behavior.handle(command, next).await })
            }
        }
    }
}

/// Wraps typed command processing before and after the next pipeline stage.
///
/// Command pipelines remain separate from request pipelines because commands produce no
/// response. This prevents a `Command` from being represented as an artificial
/// `Request<Response = ()>` and keeps handler registration type-safe.
#[async_trait]
pub trait CommandBehavior<C: Command>: Send + Sync {
    /// Handles a command and optionally invokes the next behavior or command handler.
    async fn handle(&self, command: C, next: CommandNext<C>) -> CatgaResult<()>;
}

/// An immutable, typed sequence of command behaviors built during application startup.
pub struct CommandPipeline<C: Command> {
    behaviors: Vec<Arc<dyn CommandBehavior<C>>>,
    /// Shared command behavior chain materialized lazily on the first dispatch.
    chain: OnceLock<Arc<[Arc<dyn CommandBehavior<C>>]>>,
}

impl<C: Command> Default for CommandPipeline<C> {
    fn default() -> Self {
        Self {
            behaviors: Vec::new(),
            chain: OnceLock::new(),
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
        self.behaviors.push(Arc::new(behavior));
        self
    }

    /// Adds a shared command behavior after the existing stages.
    pub fn with_shared(mut self, behavior: Arc<dyn CommandBehavior<C>>) -> Self {
        self.behaviors.push(behavior);
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
        if self.behaviors.len() >= MAX_PIPELINE_DEPTH {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "pipeline depth exceeds the supported maximum",
            ));
        }
        self.behaviors.push(behavior);
        Ok(self)
    }

    /// Returns the number of configured command behaviors.
    pub const fn len(&self) -> usize {
        self.behaviors.len()
    }

    /// Returns whether this pipeline has no configured command behaviors.
    pub const fn is_empty(&self) -> bool {
        self.behaviors.is_empty()
    }

    pub(crate) fn wrap(&self, terminal: CommandNext<C>) -> CommandNext<C> {
        let behaviors = Arc::clone(self.chain.get_or_init(|| Arc::from(self.behaviors.clone())));
        if behaviors.is_empty() {
            return terminal;
        }
        let terminal = match terminal.inner {
            CommandNextInner::Terminal(continuation) => continuation,
            CommandNextInner::Chain { .. } => Arc::new(move |command| terminal.run(command)),
        };
        CommandNext {
            inner: CommandNextInner::Chain {
                behaviors,
                index: 0,
                terminal,
            },
        }
    }
}
