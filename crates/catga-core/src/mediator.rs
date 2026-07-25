use std::{
    any::{Any, TypeId},
    sync::{Arc, OnceLock},
    time::Instant,
};

use futures::{Stream, StreamExt, stream};
use tracing::Instrument;

use crate::{
    CatgaError, CatgaResult, ErrorCode, Event, Next, Pipeline, Registry, Request, observability,
};

/// Dispatches typed requests and events through an immutable handler registry.
pub struct Mediator {
    registry: Arc<Registry>,
}

/// An explicit, once-bound handle that lets startup-constructed components use a mediator.
///
/// Clone this handle into handlers while building a [`Registry`], build the [`Mediator`], and
/// then call [`Self::bind`] once.  Successful dispatch reads a completed [`OnceLock`] and adds no
/// allocation, mutex, or global lookup.  Calling [`Self::send`] or [`Self::publish`] before
/// binding returns [`ErrorCode::Unavailable`].
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

    /// Publishes one event through the bound mediator.
    pub async fn publish<E: Event>(&self, event: E) -> CatgaResult<()> {
        self.bound()?.publish(event).await
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
    pub async fn send<M: Request>(&self, message: M) -> CatgaResult<M::Response> {
        let request_type = std::any::type_name::<M>();
        let span = observability::request_span(request_type);
        observability::record_message_tags(&span, &message);
        let started = Instant::now();
        let result = Self::dispatch(&self.registry, message)
            .instrument(span.clone())
            .await;
        observability::record_request(&span, request_type, started.elapsed(), &result);
        result
    }

    /// Routes a request through a typed pipeline before its registered handler.
    pub async fn send_with<M: Request>(
        &self,
        message: M,
        pipeline: &Pipeline<M>,
    ) -> CatgaResult<M::Response> {
        let registry = Arc::clone(&self.registry);
        let terminal = Next::new(move |message| {
            let registry = Arc::clone(&registry);
            Box::pin(async move { Self::dispatch(&registry, message).await })
        });

        let request_type = std::any::type_name::<M>();
        let span = observability::request_span(request_type);
        observability::record_message_tags(&span, &message);
        let started = Instant::now();
        let result = pipeline
            .wrap(terminal)
            .run(message)
            .instrument(span.clone())
            .await;
        observability::record_request(&span, request_type, started.elapsed(), &result);
        result
    }

    async fn dispatch<M: Request>(registry: &Registry, message: M) -> CatgaResult<M::Response> {
        let handler = registry.requests.get(&TypeId::of::<M>()).ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "request handler is not registered")
        })?;
        let response = handler.handle(Box::new(message)).await?;
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

    /// Routes requests concurrently while preserving their input order.
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

        Ok(stream::iter(messages)
            .map(|message| self.send(message))
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
        let handler_count = self
            .registry
            .events
            .get(&TypeId::of::<E>())
            .map_or(0, Vec::len);
        let span = observability::event_span(event_type, handler_count);
        observability::record_message_tags(&span, &event);
        let started = Instant::now();
        let result = async {
            let Some(handlers) = self.registry.events.get(&TypeId::of::<E>()) else {
                return Ok(());
            };
            let mut deliveries = stream::iter(handlers.iter())
                .map(|handler| handler.handle(Box::new(event.clone()) as Box<dyn Any + Send>))
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
        .instrument(span.clone())
        .await;
        observability::record_event(&span, event_type, started.elapsed(), &result);
        result
    }

    /// Delivers an event to every registered handler in registration order.
    pub async fn publish<E: Event>(&self, event: E) -> CatgaResult<()> {
        let event_type = std::any::type_name::<E>();
        let handler_count = self
            .registry
            .events
            .get(&TypeId::of::<E>())
            .map_or(0, Vec::len);
        let span = observability::event_span(event_type, handler_count);
        observability::record_message_tags(&span, &event);
        let started = Instant::now();
        let result = async {
            if let Some(handlers) = self.registry.events.get(&TypeId::of::<E>()) {
                let Some((last_handler, preceding_handlers)) = handlers.split_last() else {
                    return Ok(());
                };
                for handler in preceding_handlers {
                    handler
                        .handle(Box::new(event.clone()) as Box<dyn Any + Send>)
                        .await?;
                }
                last_handler
                    .handle(Box::new(event) as Box<dyn Any + Send>)
                    .await?;
            }
            Ok(())
        }
        .instrument(span.clone())
        .await;
        observability::record_event(&span, event_type, started.elapsed(), &result);
        result
    }
}
