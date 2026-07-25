#![forbid(unsafe_code)]
//! Lightweight test helpers for Catga applications.

mod aggregate;
mod flow;
mod harness;

use std::{future::Future, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Request};
use dashmap::DashMap;
use futures::future::BoxFuture;

pub use aggregate::{AggregateScenario, ReplayedAggregate};
pub use flow::FlowTestContext;
pub use harness::{CatgaTestHarness, RunningCatgaTestHarness};

/// A request-handler wrapper that retains received messages for assertions.
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
    marker: PhantomData<fn(M)>,
}

impl<M: Request> MissingSpyHandler<M> {
    const fn new() -> Self {
        Self {
            marker: PhantomData,
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

#[async_trait]
impl<M: Request> Handler<M> for SpyActionHandler<M> {
    async fn handle(&self, message: M) -> CatgaResult<M::Response> {
        (self.action)(message).await
    }
}

#[async_trait]
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

#[async_trait]
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

#[async_trait]
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

/// Concurrently safe message capture for test assertions.
#[derive(Default)]
pub struct MessageCapture<T> {
    published: DashMap<u64, T>,
    consumed: DashMap<u64, T>,
    next: std::sync::atomic::AtomicU64,
}

/// Returns the successful value or panics with the Catga error details.
pub fn assert_success<T>(result: CatgaResult<T>) -> T {
    result.unwrap_or_else(|error| {
        panic!(
            "expected success, got {:?}: {}",
            error.code(),
            error.message()
        )
    })
}

/// Returns the structured error after asserting that a Catga operation failed.
///
/// This is intended for tests. It panics with the unexpected successful value's
/// type when the operation succeeds, keeping production APIs panic-free while
/// making failed-test diagnostics concise.
pub fn assert_failure<T>(result: CatgaResult<T>) -> catga_core::CatgaError {
    match result {
        Ok(_) => panic!(
            "expected Catga operation returning {} to fail",
            std::any::type_name::<T>()
        ),
        Err(error) => error,
    }
}

/// Returns the successful value after asserting it equals `expected`.
///
/// The value is moved out of the result rather than cloned. This is useful for
/// asserting non-`Clone` response types in tests.
pub fn assert_value<T>(result: CatgaResult<T>, expected: T) -> T
where
    T: std::fmt::Debug + PartialEq,
{
    match result {
        Ok(value) if value == expected => value,
        Ok(value) => panic!("expected successful value {expected:?}, got {value:?}"),
        Err(error) => panic!(
            "expected successful value {expected:?}, got {:?}: {}",
            error.code(),
            error.message()
        ),
    }
}

/// Returns every value matching `predicate`, panicking when no value matches.
///
/// The supplied iterator is consumed exactly once. Matching values are moved
/// into the returned vector, so the helper does not require `Clone`.
pub fn assert_contains<T, I, Predicate>(values: I, mut predicate: Predicate) -> Vec<T>
where
    I: IntoIterator<Item = T>,
    Predicate: FnMut(&T) -> bool,
{
    let matches: Vec<_> = values
        .into_iter()
        .filter(|value| predicate(value))
        .collect();
    assert!(
        !matches.is_empty(),
        "expected at least one matching {}",
        std::any::type_name::<T>()
    );
    matches
}

/// Returns the error after asserting its stable Catga error code.
pub fn assert_error_code<T>(
    result: CatgaResult<T>,
    expected: catga_core::ErrorCode,
) -> catga_core::CatgaError {
    let error = match result {
        Ok(_) => panic!("expected Catga operation to fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), expected, "unexpected Catga error code");
    error
}

impl<T> MessageCapture<T> {
    /// Records a published message.
    pub fn record_published(&self, value: T) {
        self.published.insert(
            self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            value,
        );
    }
    /// Records a consumed message.
    pub fn record_consumed(&self, value: T) {
        self.consumed.insert(
            self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            value,
        );
    }
    /// Returns published values in recording order.
    pub fn published(&self) -> Vec<T>
    where
        T: Clone,
    {
        captured_in_order(&self.published)
    }

    /// Returns consumed values in recording order.
    pub fn consumed(&self) -> Vec<T>
    where
        T: Clone,
    {
        captured_in_order(&self.consumed)
    }

    /// Clears captured values without resetting the concurrent sequence source.
    pub fn clear(&self) {
        self.published.clear();
        self.consumed.clear();
    }
}

fn captured_in_order<T>(values: &DashMap<u64, T>) -> Vec<T>
where
    T: Clone,
{
    let mut values: Vec<_> = values
        .iter()
        .map(|entry| (*entry.key(), entry.value().clone()))
        .collect();
    values.sort_unstable_by_key(|(sequence, _)| *sequence);
    values.into_iter().map(|(_, value)| value).collect()
}
