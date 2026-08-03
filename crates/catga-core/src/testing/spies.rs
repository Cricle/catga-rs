//! Spies for intercepting handler and event handler calls in tests.

use std::future::Future;
use std::sync::Arc;

use dashmap::DashMap;
use futures::future::BoxFuture;

use crate::{CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Request};

/// A request-handler wrapper that retains received messages for assertions.
///
/// ```no_run
/// use catga_core::{catga_request, CatgaResult, Handler};
/// use async_trait::async_trait;
///
/// #[derive(Clone)]
/// #[catga_request(response = &'static str)]
/// struct Ping;
///
/// struct PingHandler;
/// #[async_trait]
/// impl Handler<Ping> for PingHandler {
///     async fn handle(&self, _: Ping) -> CatgaResult<&'static str> { Ok("pong") }
/// }
///
/// let spy: catga_core::testing::HandlerSpy<Ping, _> = catga_core::testing::HandlerSpy::new(PingHandler);
/// assert_eq!(spy.call_count(), 0);
/// ```
pub struct HandlerSpy<M, H> {
    inner: H,
    calls: Arc<DashMap<u64, M>>,
    next: std::sync::atomic::AtomicU64,
}

type HandlerSpyAction<M> =
    Box<dyn Fn(M) -> BoxFuture<'static, CatgaResult<<M as Request>::Response>> + Send + Sync>;

/// A request handler backed by an async test action.
///
/// This adapter lets tests provide a compact closure instead of declaring a
/// one-off handler type. It is intended for test fixtures; production handlers
/// should use a concrete type so their dependencies remain explicit.
pub struct SpyActionHandler<M: Request> {
    action: HandlerSpyAction<M>,
}

/// A zero-sized request handler that reports an intentionally unconfigured spy.
///
/// Construct it through [`HandlerSpy::without_handler`] when a test needs to
/// assert the no-handler failure path without relying on a null sentinel.
pub struct MissingSpyHandler<M: Request> {
    marker: std::marker::PhantomData<fn(M)>,
}

impl<M: Request> MissingSpyHandler<M> {
    const fn new() -> Self {
        Self {
            marker: std::marker::PhantomData,
        }
    }
}

impl<M: Request> SpyActionHandler<M> {
    /// Creates a handler that delegates each request to `action`.
    pub fn new<F, Fut>(action: F) -> Self
    where
        F: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CatgaResult<M::Response>> + Send + 'static,
    {
        Self {
            action: Box::new(move |request| Box::pin(action(request))),
        }
    }
}

#[async_trait::async_trait]
impl<M: Request> Handler<M> for SpyActionHandler<M> {
    async fn handle(&self, message: M) -> CatgaResult<M::Response> {
        (self.action)(message).await
    }
}

#[async_trait::async_trait]
impl<M: Request> Handler<M> for MissingSpyHandler<M> {
    async fn handle(&self, _: M) -> CatgaResult<M::Response> {
        Err(CatgaError::new(
            ErrorCode::NotFound,
            "handler spy has no configured handler or action",
        ))
    }
}

impl<M, H> HandlerSpy<M, H> {
    /// Wraps one request handler.
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            calls: Arc::new(DashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns a snapshot of recorded calls in unspecified concurrent insertion order.
    pub fn calls(&self) -> Vec<M>
    where
        M: Clone,
    {
        let mut calls: Vec<_> = self
            .calls
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        calls.sort_unstable_by_key(|(sequence, _)| *sequence);
        calls.into_iter().map(|(_, message)| message).collect()
    }

    /// Returns the number of recorded calls.
    pub fn call_count(&self) -> usize {
        self.calls.len()
    }

    /// Returns the most recently recorded request, or `None` when no request has arrived.
    ///
    /// Calls receive a monotonically increasing sequence before they are stored. During
    /// concurrent handler execution, this method selects the greatest sequence that has
    /// completed recording, so it never exposes a reserved but not-yet-stored value.
    /// This is an assertion helper rather than a dispatch hot path; use
    /// [`Self::call_count`] when only the count is needed.
    pub fn last_call(&self) -> Option<M>
    where
        M: Clone,
    {
        self.calls
            .iter()
            .max_by_key(|entry| *entry.key())
            .map(|entry| entry.value().clone())
    }
}

impl<M: Request> HandlerSpy<M, SpyActionHandler<M>> {
    /// Creates a request spy backed by an async action.
    ///
    /// The request is recorded before `action` runs, including when the action
    /// returns an error. This mirrors [`HandlerSpy::new`] while avoiding a
    /// dedicated handler type in each test.
    pub fn with_action<F, Fut>(action: F) -> Self
    where
        F: Fn(M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CatgaResult<M::Response>> + Send + 'static,
    {
        Self::new(SpyActionHandler::new(action))
    }
}

impl<M: Request> HandlerSpy<M, MissingSpyHandler<M>> {
    /// Creates a request spy that records each request then reports no handler.
    ///
    /// This is the Rust replacement for an optional C# handler reference: it
    /// has no null state and returns a structured [`ErrorCode::NotFound`].
    pub fn without_handler() -> Self {
        Self::new(MissingSpyHandler::new())
    }
}

#[async_trait::async_trait]
impl<M, H> Handler<M> for HandlerSpy<M, H>
where
    M: Request + Clone,
    H: Handler<M>,
{
    async fn handle(&self, message: M) -> CatgaResult<M::Response> {
        let key = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.calls.insert(key, message.clone());
        self.inner.handle(message).await
    }
}

type EventSpyAction<E> = Box<dyn Fn(E) -> BoxFuture<'static, CatgaResult<()>> + Send + Sync>;

/// An event-handler wrapper that retains every received event for assertions.
///
/// [`Self::new`] records events without side effects. [`Self::with_handler`] additionally
/// delegates every event to a real handler after recording it, preserving the handler's result.
pub struct EventHandlerSpy<E> {
    action: Option<EventSpyAction<E>>,
    calls: Arc<DashMap<u64, E>>,
    next: std::sync::atomic::AtomicU64,
}

impl<E> EventHandlerSpy<E> {
    /// Creates a side-effect-free event spy.
    pub fn new() -> Self {
        Self {
            action: None,
            calls: Arc::new(DashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Wraps one event handler while retaining every delivered event.
    pub fn with_handler<H>(handler: H) -> Self
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        let handler = Arc::new(handler);
        Self {
            action: Some(Box::new(move |event| {
                let handler = Arc::clone(&handler);
                Box::pin(async move { handler.handle(event).await })
            })),
            calls: Arc::new(DashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Creates an event spy backed by an async action.
    ///
    /// The event is recorded before `action` runs, including when the action
    /// returns an error. This keeps assertion data available for failed event
    /// deliveries without a separate handler type.
    pub fn with_action<F, Fut>(action: F) -> Self
    where
        E: Event,
        F: Fn(E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CatgaResult<()>> + Send + 'static,
    {
        Self {
            action: Some(Box::new(move |event| Box::pin(action(event)))),
            calls: Arc::new(DashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns all recorded events in handler invocation order.
    pub fn calls(&self) -> Vec<E>
    where
        E: Clone,
    {
        let mut calls: Vec<_> = self
            .calls
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        calls.sort_unstable_by_key(|(sequence, _)| *sequence);
        calls.into_iter().map(|(_, event)| event).collect()
    }

    /// Returns the number of events recorded so far.
    pub fn call_count(&self) -> usize {
        self.calls.len()
    }

    /// Returns the most recently recorded event, or `None` when no event has arrived.
    ///
    /// Calls receive a monotonically increasing sequence before they are stored. During
    /// concurrent handler execution, this method selects the greatest sequence that has
    /// completed recording, so it never exposes a reserved but not-yet-stored value.
    /// This is an assertion helper rather than a dispatch hot path; use
    /// [`Self::call_count`] when only the count is needed.
    pub fn last_call(&self) -> Option<E>
    where
        E: Clone,
    {
        self.calls
            .iter()
            .max_by_key(|entry| *entry.key())
            .map(|entry| entry.value().clone())
    }
}

impl<E> Default for EventHandlerSpy<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl<E> EventHandler<E> for EventHandlerSpy<E>
where
    E: Event + Clone,
{
    async fn handle(&self, event: E) -> CatgaResult<()> {
        let key = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.calls.insert(key, event.clone());
        match &self.action {
            Some(action) => action(event).await,
            None => Ok(()),
        }
    }
}
