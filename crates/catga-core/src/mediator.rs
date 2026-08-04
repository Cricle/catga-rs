//! Typed mediator for request, command, and event dispatch.
//!
//! The mediator maps incoming messages to registered handlers without coupling
//! senders to concrete implementations. It supports three message types:
//!
//! - **Requests** ([`Request`]) — typed request/response with one handler
//! - **Commands** ([`Command`]) — fire-and-forget with one handler
//! - **Events** ([`Event`]) — broadcast to zero or more handlers
//!
//! # Usage
//!
//! Construct a [`Registry`] at startup with all handlers, then create one [`Mediator`]:
//!
//! ```no_run
//! use catga_core::{Mediator, Handler, Request, Message, MessageTypeId};
//!
//! struct QueryTypeId;
//! impl MessageTypeId for QueryTypeId { const NAME: &'static str = "Query"; }
//!
//! struct Query;
//! impl Message for Query {}
//! impl Request for Query { type Response = String; type TypeId = QueryTypeId; }
//!
//! # async fn run() -> catga_core::CatgaResult<()> {
//! let mediator = Mediator::new(catga_core::Registry::new());
//! let result = mediator.send(Query).await;
//! # Ok(())
//! # }
//! ```
//!
//! # Batch Operations
//!
//! [`Mediator::send_batch`] processes up to [`MAX_MEDIATOR_BATCH_SIZE`] requests in parallel.
//! For unbounded producers, use [`Mediator::send_stream`] instead.

use std::{
    any::{Any, TypeId},
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, OnceLock},
    time::Instant,
};

use futures::{FutureExt, Stream, StreamExt, stream};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::cancellation::until_cancelled;
use crate::{
    CatgaError, CatgaResult, Command, CommandNext, CommandPipeline, ErrorCode, Event,
    MAX_PIPELINE_DEPTH, Next, Pipeline, Registry, Request, observability, scope_cancellation,
};

/// Maximum number of requests retained by one [`Mediator::send_batch`] call.
///
/// Use [`Mediator::send_stream`] when the producer is unbounded or the caller does not need all
/// responses retained in one result vector.
pub const MAX_MEDIATOR_BATCH_SIZE: usize = 1024;

/// Dispatches typed requests, commands, and events through an immutable handler registry.
pub struct Mediator {
    registry: Arc<Registry>,
}

/// An explicit, once-bound handle that lets startup-constructed components use a mediator.
///
/// Clone this handle into handlers while building a [`Registry`], build the [`Mediator`], and
/// then call [`Self::bind`] once.  Successful dispatch reads a completed [`OnceLock`] and adds no
/// allocation, mutex, or global lookup.  Calling [`Self::send`], [`Self::send_command`], or
/// [`Self::publish`] before binding returns [`ErrorCode::Unavailable`].
///
/// ```
/// use catga_core::MediatorHandle;
///
/// let handle = MediatorHandle::new();
/// assert!(!handle.is_bound());
/// ```
#[derive(Clone, Default)]
pub struct MediatorHandle {
    mediator: Arc<OnceLock<Arc<Mediator>>>,
}

impl MediatorHandle {
    /// Creates an empty handle that must be bound after registry startup.
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds this handle to the application's immutable mediator exactly once.
    ///
    /// A second call returns [`ErrorCode::Conflict`] and leaves the initial binding unchanged.
    pub fn bind(&self, mediator: Arc<Mediator>) -> CatgaResult<()> {
        self.mediator.set(mediator).map_err(|_| {
            CatgaError::new(
                ErrorCode::Conflict,
                "mediator handle has already been bound",
            )
        })
    }

    /// Returns whether startup has bound this handle to a mediator.
    pub fn is_bound(&self) -> bool {
        self.mediator.get().is_some()
    }

    /// Sends one request through the bound mediator.
    pub async fn send<M: Request>(&self, message: M) -> CatgaResult<M::Response> {
        self.bound()?.send(message).await
    }

    /// Sends one request through the bound mediator with cooperative cancellation.
    pub async fn send_with_cancellation<M: Request>(
        &self,
        message: M,
        cancellation: CancellationToken,
    ) -> CatgaResult<M::Response> {
        self.bound()?
            .send_with_cancellation(message, cancellation)
            .await
    }

    /// Sends one command through the bound mediator.
    pub async fn send_command<C: Command>(&self, command: C) -> CatgaResult<()> {
        self.bound()?.send_command(command).await
    }

    /// Sends one command through the bound mediator with cooperative cancellation.
    pub async fn send_command_with_cancellation<C: Command>(
        &self,
        command: C,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        self.bound()?
            .send_command_with_cancellation(command, cancellation)
            .await
    }

    /// Publishes one event through the bound mediator.
    pub async fn publish<E: Event + Clone>(&self, event: E) -> CatgaResult<()> {
        self.bound()?.publish(event).await
    }

    /// Publishes one event through the bound mediator with cooperative cancellation.
    pub async fn publish_with_cancellation<E: Event>(
        &self,
        event: E,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        self.bound()?
            .publish_with_cancellation(event, cancellation)
            .await
    }

    fn bound(&self) -> CatgaResult<&Arc<Mediator>> {
        self.mediator.get().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Unavailable,
                "mediator handle is not bound during application startup",
            )
        })
    }
}

impl Mediator {
    /// Creates a mediator from an explicit registry built during application startup.
    pub fn new(registry: Registry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    /// Routes a request to its sole registered handler.
    ///
    /// A handler panic is returned as [`ErrorCode::Internal`] when the Rust strategy is
    /// unwinding. Builds configured with `panic = "abort"` terminate instead and cannot be
    /// recovered by this method.
    pub async fn send<M: Request>(&self, message: M) -> CatgaResult<M::Response> {
        let span = observability::request_span(std::any::type_name::<M>());
        let started = Instant::now();
        if span.is_disabled() {
            let result = isolate_mediator_panic(Self::dispatch(&self.registry, message)).await;
            observability::record_request(&span, std::any::type_name::<M>(), started.elapsed(), &result);
            return result;
        }
        observability::record_message_tags(&span, &message);
        let result = Self::dispatch(&self.registry, message)
            .instrument(span.clone())
            .await;
        observability::record_response_tags::<M>(&span, &result);
        observability::record_request(&span, std::any::type_name::<M>(), started.elapsed(), &result);
        result
    }

    /// Routes a request while making `cancellation` available to the handler and behaviors.
    ///
    /// Cancellation before dispatch prevents handler invocation. Cancellation during dispatch
    /// drops the active future and returns [`ErrorCode::Cancelled`]; code that must release an
    /// external resource can obtain the same token with [`crate::current_cancellation`].
    pub async fn send_with_cancellation<M: Request>(
        &self,
        message: M,
        cancellation: CancellationToken,
    ) -> CatgaResult<M::Response> {
        let operation = scope_cancellation(cancellation.clone(), self.send(message));
        until_cancelled(cancellation, operation).await
    }

    /// Routes a command to its sole registered handler.
    ///
    /// A handler panic is returned as [`ErrorCode::Internal`] when the Rust strategy is
    /// unwinding. Builds configured with `panic = "abort"` terminate instead and cannot be
    /// recovered by this method.
    pub async fn send_command<C: Command>(&self, command: C) -> CatgaResult<()> {
        let span = observability::command_span(std::any::type_name::<C>());
        let started = Instant::now();
        if span.is_disabled() {
            let result = isolate_mediator_panic(Self::dispatch_command(&self.registry, command)).await;
            observability::record_command(&span, std::any::type_name::<C>(), started.elapsed(), &result);
            return result;
        }
        observability::record_message_tags(&span, &command);
        let result = Self::dispatch_command(&self.registry, command)
            .instrument(span.clone())
            .await;
        observability::record_command(&span, std::any::type_name::<C>(), started.elapsed(), &result);
        result
    }

    /// Routes a command while making `cancellation` available to its handler.
    pub async fn send_command_with_cancellation<C: Command>(
        &self,
        command: C,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        let operation = scope_cancellation(cancellation.clone(), self.send_command(command));
        until_cancelled(cancellation, operation).await
    }

    /// Routes a command through a typed pipeline before its registered handler.
    ///
    /// This is the command counterpart to [`Self::send_with`]. Command behavior remains a
    /// separate type-safe contract because a command has no response value.
    pub async fn send_command_with<C: Command>(
        &self,
        command: C,
        pipeline: &CommandPipeline<C>,
    ) -> CatgaResult<()> {
        self.dispatch_command_pipeline(command, pipeline, None)
            .await
    }

    /// Routes a command through `pipeline` with explicit cooperative cancellation.
    pub async fn send_command_with_cancellation_and_pipeline<C: Command>(
        &self,
        command: C,
        pipeline: &CommandPipeline<C>,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        self.dispatch_command_pipeline(command, pipeline, Some(cancellation))
            .await
    }

    async fn dispatch_command_pipeline<C: Command>(
        &self,
        command: C,
        pipeline: &CommandPipeline<C>,
        cancellation: Option<CancellationToken>,
    ) -> CatgaResult<()> {
        let command_type = std::any::type_name::<C>();
        let span = observability::command_span(command_type);
        observability::record_message_tags(&span, &command);
        let started = Instant::now();
        if pipeline.len() > MAX_PIPELINE_DEPTH {
            let result = Err(CatgaError::new(
                ErrorCode::Validation,
                "pipeline depth exceeds the supported maximum",
            ));
            let elapsed = started.elapsed();
            observability::record_command(&span, command_type, elapsed, &result);
            observability::record_pipeline(&span, "command", pipeline.len(), elapsed, &result);
            return result;
        }
        let registry = Arc::clone(&self.registry);
        let terminal = CommandNext::new(move |command| {
            let registry = Arc::clone(&registry);
            Box::pin(async move { Self::dispatch_command(&registry, command).await })
        });
        let operation = isolate_mediator_panic(
            pipeline
                .wrap(terminal)
                .run(command)
                .instrument(span.clone()),
        );
        let result = match cancellation {
            Some(cancellation) => {
                scope_cancellation(
                    cancellation.clone(),
                    until_cancelled(cancellation, operation),
                )
                .await
            }
            None => operation.await,
        };
        let elapsed = started.elapsed();
        observability::record_command(&span, command_type, elapsed, &result);
        observability::record_pipeline(&span, "command", pipeline.len(), elapsed, &result);
        result
    }

    /// Routes a request through a typed pipeline before its registered handler.
    ///
    /// A panic in either a behavior or the terminal handler is returned as
    /// [`ErrorCode::Internal`] when the Rust panic strategy is unwinding. Builds configured with
    /// `panic = "abort"` terminate instead and cannot be recovered by this method.
    pub async fn send_with<M: Request>(
        &self,
        message: M,
        pipeline: &Pipeline<M>,
    ) -> CatgaResult<M::Response> {
        self.dispatch_request_pipeline(message, pipeline, None)
            .await
    }

    /// Routes a request through `pipeline` with explicit cooperative cancellation.
    ///
    /// This is the cancellation-aware counterpart to [`Self::send_with`]. Behaviors and the
    /// terminal handler share one task-local token from [`crate::current_cancellation`].
    pub async fn send_with_cancellation_and_pipeline<M: Request>(
        &self,
        message: M,
        pipeline: &Pipeline<M>,
        cancellation: CancellationToken,
    ) -> CatgaResult<M::Response> {
        self.dispatch_request_pipeline(message, pipeline, Some(cancellation))
            .await
    }

    async fn dispatch_request_pipeline<M: Request>(
        &self,
        message: M,
        pipeline: &Pipeline<M>,
        cancellation: Option<CancellationToken>,
    ) -> CatgaResult<M::Response> {
        let request_type = std::any::type_name::<M>();
        let span = observability::request_span(request_type);
        observability::record_message_tags(&span, &message);
        let started = Instant::now();
        if pipeline.len() > MAX_PIPELINE_DEPTH {
            let result = Err(CatgaError::new(
                ErrorCode::Validation,
                "pipeline depth exceeds the supported maximum",
            ));
            let elapsed = started.elapsed();
            observability::record_request(&span, request_type, elapsed, &result);
            observability::record_pipeline(&span, "request", pipeline.len(), elapsed, &result);
            return result;
        }
        let registry = Arc::clone(&self.registry);
        let terminal = Next::new(move |message| {
            let registry = Arc::clone(&registry);
            Box::pin(async move { Self::dispatch(&registry, message).await })
        });
        let operation = isolate_mediator_panic(
            pipeline
                .wrap(terminal)
                .run(message)
                .instrument(span.clone()),
        );
        let result = match cancellation {
            Some(cancellation) => {
                scope_cancellation(
                    cancellation.clone(),
                    until_cancelled(cancellation, operation),
                )
                .await
            }
            None => operation.await,
        };
        let elapsed = started.elapsed();
        observability::record_response_tags::<M>(&span, &result);
        observability::record_request(&span, request_type, elapsed, &result);
        observability::record_pipeline(&span, "request", pipeline.len(), elapsed, &result);
        result
    }

    async fn dispatch<M: Request>(registry: &Registry, message: M) -> CatgaResult<M::Response> {
        let type_id = TypeId::of::<M>();
        let slot = registry.find_request(type_id).ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "request handler is not registered")
        })?;
        let response = slot.handler.handle(Box::new(message)).await?;
        response
            .downcast::<M::Response>()
            .map(|response| *response)
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "request handler returned an invalid response type",
                )
            })
    }

    async fn dispatch_command<C: Command>(registry: &Registry, command: C) -> CatgaResult<()> {
        let type_id = TypeId::of::<C>();
        let slot = registry.find_command(type_id).ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "command handler is not registered")
        })?;
        slot.handler.handle(Box::new(command)).await
    }

    /// Routes a bounded request batch concurrently while preserving input order.
    ///
    /// This method retains every response in its returned vector, so it accepts at most
    /// [`MAX_MEDIATOR_BATCH_SIZE`] messages. Larger inputs return [`ErrorCode::Validation`]
    /// before any request is dispatched; use [`Self::send_stream`] for unbounded producers.
    ///
    /// Batch dispatch bypasses per-message observability spans for throughput; a single
    /// batch-level span covers the entire operation when a subscriber is active.
    pub async fn send_batch<M>(
        &self,
        messages: impl IntoIterator<Item = M>,
        concurrency_limit: usize,
    ) -> CatgaResult<Vec<CatgaResult<M::Response>>>
    where
        M: Request,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch concurrency limit must be greater than zero",
            ));
        }

        let mut bounded = Vec::with_capacity(MAX_MEDIATOR_BATCH_SIZE);
        for message in messages
            .into_iter()
            .take(MAX_MEDIATOR_BATCH_SIZE.saturating_add(1))
        {
            bounded.push(message);
        }
        if bounded.len() > MAX_MEDIATOR_BATCH_SIZE {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch request count exceeds the configured limit; use send_stream",
            ));
        }

        let registry = &self.registry;
        Ok(stream::iter(bounded)
            .map(|message| isolate_mediator_panic(Self::dispatch(registry, message)))
            .buffered(concurrency_limit)
            .collect()
            .await)
    }

    /// Lazily routes every request produced by a stream.
    pub fn send_stream<'a, M, S>(
        &'a self,
        messages: S,
    ) -> impl Stream<Item = CatgaResult<M::Response>> + 'a
    where
        M: Request,
        S: Stream<Item = M> + Send + 'a,
    {
        messages.then(move |message| self.send(message))
    }

    /// Publishes events with bounded concurrency and waits for every event that was supplied.
    ///
    /// Unlike collecting one future per input, this keeps at most `concurrency_limit` publish
    /// futures alive at once. Every input event is attempted even when another event fails; the
    /// first observed failure is returned after all work has completed.
    pub async fn publish_batch<E>(
        &self,
        events: impl IntoIterator<Item = E>,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        E: Event,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch concurrency limit must be greater than zero",
            ));
        }

        let mut publishes = stream::iter(events)
            .map(|event| self.publish(event))
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = publishes.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Delivers one event to its handlers with a bounded number of concurrent invocations.
    ///
    /// The sequential [`Self::publish`] path moves the final event instance and is optimal for
    /// small fan-outs. This method trades one clone per handler for concurrent handler execution;
    /// it waits for every started handler and returns the first observed failure.
    pub async fn publish_with_concurrency<E>(
        &self,
        event: E,
        concurrency_limit: usize,
    ) -> CatgaResult<()>
    where
        E: Event,
    {
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "event handler concurrency limit must be greater than zero",
            ));
        }
        let event_type = std::any::type_name::<E>();
        let event_type_id = TypeId::of::<E>();
        let handler_count = self
            .registry
            .find_event(event_type_id)
            .map_or(0, |slot| slot.handlers.len());
        let span = observability::event_span(event_type, handler_count);
        if span.is_disabled() {
            return self
                .publish_concurrency_inner(event, event_type_id, concurrency_limit)
                .await;
        }
        observability::record_message_tags(&span, &event);
        let started = Instant::now();
        let result = self
            .publish_concurrency_inner(event, event_type_id, concurrency_limit)
            .instrument(span.clone())
            .await;
        observability::record_event(&span, event_type, started.elapsed(), &result);
        result
    }

    async fn publish_concurrency_inner<E: Event + Clone>(
        &self,
        event: E,
        event_type_id: TypeId,
        concurrency_limit: usize,
    ) -> CatgaResult<()> {
        let Some(slot) = self.registry.find_event(event_type_id) else {
            return Ok(());
        };
        let mut deliveries = stream::iter(slot.handlers.iter())
            .map(|handler| {
                isolate_mediator_panic(
                    handler.handle(Box::new(event.clone()) as Box<dyn Any + Send>),
                )
            })
            .buffer_unordered(concurrency_limit);
        let mut first_error = None;
        while let Some(result) = deliveries.next().await {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Delivers an event to every registered handler in registration order.
    ///
    /// Every handler receives the event even when an earlier handler fails. The first observed
    /// failure is returned after fan-out completes. This sequential path moves the final event
    /// instance into its handler, avoiding an unnecessary clone for the common small fan-out.
    pub async fn publish<E: Event + Clone>(&self, event: E) -> CatgaResult<()> {
        let event_type = std::any::type_name::<E>();
        let event_type_id = TypeId::of::<E>();
        let handler_count = self
            .registry
            .find_event(event_type_id)
            .map_or(0, |slot| slot.handlers.len());
        let span = observability::event_span(event_type, handler_count);
        let started = Instant::now();
        if span.is_disabled() {
            let result = self.publish_inner(event).await;
            observability::record_event(&span, event_type, started.elapsed(), &result);
            return result;
        }
        observability::record_message_tags(&span, &event);
        let result = self.publish_inner(event)
            .instrument(span.clone())
            .await;
        observability::record_event(&span, event_type, started.elapsed(), &result);
        result
    }

    async fn publish_inner<E: Event + Clone>(&self, event: E) -> CatgaResult<()> {
        let event_type_id = TypeId::of::<E>();
        if let Some(slot) = self.registry.find_event(event_type_id) {
            let handlers = &slot.handlers;
            let Some((last_handler, preceding_handlers)) = handlers.split_last() else {
                return Ok(());
            };
            let mut first_error = None;
            for handler in preceding_handlers {
                if let Err(error) = isolate_mediator_panic(
                    handler.handle(Box::new(event.clone()) as Box<dyn Any + Send>),
                )
                .await
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            if let Err(error) =
                isolate_mediator_panic(last_handler.handle(Box::new(event) as Box<dyn Any + Send>))
                    .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
            if let Some(error) = first_error {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Delivers an event while making `cancellation` available to every handler.
    pub async fn publish_with_cancellation<E: Event>(
        &self,
        event: E,
        cancellation: CancellationToken,
    ) -> CatgaResult<()> {
        let operation = scope_cancellation(cancellation.clone(), self.publish(event));
        until_cancelled(cancellation, operation).await
    }
}

/// Converts a recoverable unwind from mediator processing into a structured framework error.
///
/// Rust builds configured with `panic = "abort"` cannot recover from panics; this boundary only
/// isolates the normal unwinding panic strategy. Keeping the boundary around the complete future
/// also covers registered request or command handlers and every pipeline behavior.
async fn isolate_mediator_panic<T>(
    operation: impl Future<Output = CatgaResult<T>>,
) -> CatgaResult<T> {
    match AssertUnwindSafe(operation).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(CatgaError::new(
            ErrorCode::Internal,
            "mediator processing panicked",
        )),
    }
}
