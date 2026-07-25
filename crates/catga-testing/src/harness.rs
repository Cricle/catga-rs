use std::{
    any::{Any, TypeId},
    collections::HashSet,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use catga_core::{CatgaResult, Event, EventHandler, Handler, Mediator, Registry, Request};
use catga_flow::MemoryFlowScheduler;
use catga_memory::{MemorySuspendedFlows, MemoryTransport};
use dashmap::DashMap;

const DEFAULT_TRANSPORT_CAPACITY: usize = 64;

type CapturedValue = Box<dyn Any + Send + Sync>;

/// Builds a typed, in-process Catga test environment.
///
/// Register handlers while building, then call [`Self::start`] to obtain a
/// [`RunningCatgaTestHarness`]. This deliberately keeps registration separate
/// from execution instead of reproducing a reflection-based service container.
pub struct CatgaTestHarness {
    registry: Registry,
    capture: Arc<CapturedMessages>,
    captured_event_types: HashSet<TypeId>,
    suspended_flows: Arc<MemorySuspendedFlows>,
    flow_scheduler: Arc<MemoryFlowScheduler>,
    transport: MemoryTransport,
}

impl CatgaTestHarness {
    /// Creates a harness with a bounded in-memory transport.
    pub fn new() -> CatgaResult<Self> {
        Self::with_transport_capacity(DEFAULT_TRANSPORT_CAPACITY)
    }

    /// Creates a harness with an explicit in-memory transport capacity.
    pub fn with_transport_capacity(capacity: usize) -> CatgaResult<Self> {
        Ok(Self {
            registry: Registry::new(),
            capture: Arc::new(CapturedMessages::default()),
            captured_event_types: HashSet::new(),
            suspended_flows: Arc::new(MemorySuspendedFlows::default()),
            flow_scheduler: Arc::new(MemoryFlowScheduler::default()),
            transport: MemoryTransport::new(capacity)?,
        })
    }

    /// Registers a request handler without retaining request values.
    pub fn register_request<M, H>(&mut self, handler: H) -> CatgaResult<()>
    where
        M: Request,
        H: Handler<M> + 'static,
    {
        self.registry.register_request::<M, H>(handler)
    }

    /// Registers a request handler and captures each consumed request.
    ///
    /// Rust requests are moved into their handler, so capture requires `Clone`.
    /// The non-capturing [`Self::register_request`] remains available for move-only
    /// request types.
    pub fn register_captured_request<M, H>(&mut self, handler: H) -> CatgaResult<()>
    where
        M: Request + Clone,
        H: Handler<M> + 'static,
    {
        self.registry
            .register_request::<M, _>(CapturingRequestHandler {
                capture: Arc::clone(&self.capture),
                inner: handler,
                marker: PhantomData,
            })
    }

    /// Registers an event handler and captures each publication once before its handlers run.
    pub fn register_event<E, H>(&mut self, handler: H)
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        self.capture_event::<E>();
        self.registry.register_event::<E, H>(handler);
    }

    /// Captures each publication of `E`, including publications with no application handler.
    pub fn capture_event<E>(&mut self)
    where
        E: Event,
    {
        if self.captured_event_types.insert(TypeId::of::<E>()) {
            self.registry
                .register_event::<E, _>(CaptureOnlyEventHandler {
                    capture: Arc::clone(&self.capture),
                    marker: PhantomData,
                });
        }
    }

    /// Finishes registration and exposes the running in-process environment.
    pub fn start(self) -> RunningCatgaTestHarness {
        RunningCatgaTestHarness {
            mediator: Arc::new(Mediator::new(self.registry)),
            capture: self.capture,
            suspended_flows: self.suspended_flows,
            flow_scheduler: self.flow_scheduler,
            transport: self.transport,
        }
    }
}

/// A started in-process harness with typed message capture assertions.
pub struct RunningCatgaTestHarness {
    mediator: Arc<Mediator>,
    capture: Arc<CapturedMessages>,
    suspended_flows: Arc<MemorySuspendedFlows>,
    flow_scheduler: Arc<MemoryFlowScheduler>,
    transport: MemoryTransport,
}

impl RunningCatgaTestHarness {
    /// Returns the immutable, shareable typed mediator.
    pub fn mediator(&self) -> Arc<Mediator> {
        Arc::clone(&self.mediator)
    }

    /// Returns a clone of the bounded in-memory transport.
    pub fn transport(&self) -> MemoryTransport {
        self.transport.clone()
    }

    /// Returns the durable in-memory continuation store used by Flow runtime tests.
    pub fn suspended_flows(&self) -> Arc<MemorySuspendedFlows> {
        Arc::clone(&self.suspended_flows)
    }

    /// Returns the deterministic in-memory scheduler used by Flow runtime tests.
    pub fn flow_scheduler(&self) -> Arc<MemoryFlowScheduler> {
        Arc::clone(&self.flow_scheduler)
    }

    /// Returns captured published values of one event type in dispatch order.
    pub fn published_of<T>(&self) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.capture.published_of()
    }

    /// Returns captured consumed values of one request type in dispatch order.
    pub fn consumed_of<T>(&self) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.capture.consumed_of()
    }

    /// Removes all captured messages without changing the harness registrations.
    pub fn clear_captures(&self) {
        self.capture.clear();
    }
}

struct CapturingRequestHandler<M, H> {
    capture: Arc<CapturedMessages>,
    inner: H,
    marker: PhantomData<fn(M)>,
}

#[async_trait]
impl<M, H> Handler<M> for CapturingRequestHandler<M, H>
where
    M: Request + Clone,
    H: Handler<M> + 'static,
{
    async fn handle(&self, request: M) -> CatgaResult<M::Response> {
        self.capture.record_consumed(request.clone());
        self.inner.handle(request).await
    }
}

struct CaptureOnlyEventHandler<E> {
    capture: Arc<CapturedMessages>,
    marker: PhantomData<fn(E)>,
}

#[async_trait]
impl<E> EventHandler<E> for CaptureOnlyEventHandler<E>
where
    E: Event,
{
    async fn handle(&self, event: E) -> CatgaResult<()> {
        self.capture.record_published(event.clone());
        Ok(())
    }
}

#[derive(Default)]
struct CapturedMessages {
    published: DashMap<u64, CapturedValue>,
    consumed: DashMap<u64, CapturedValue>,
    next: AtomicU64,
}

impl CapturedMessages {
    fn record_published<T>(&self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.published
            .insert(self.next.fetch_add(1, Ordering::Relaxed), Box::new(value));
    }

    fn record_consumed<T>(&self, value: T)
    where
        T: Send + Sync + 'static,
    {
        self.consumed
            .insert(self.next.fetch_add(1, Ordering::Relaxed), Box::new(value));
    }

    fn published_of<T>(&self) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        captured_of(&self.published)
    }

    fn consumed_of<T>(&self) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        captured_of(&self.consumed)
    }

    fn clear(&self) {
        self.published.clear();
        self.consumed.clear();
    }
}

fn captured_of<T>(values: &DashMap<u64, CapturedValue>) -> Vec<T>
where
    T: Clone + Send + Sync + 'static,
{
    let mut captured: Vec<_> = values
        .iter()
        .filter_map(|entry| {
            entry
                .value()
                .downcast_ref::<T>()
                .map(|value| (*entry.key(), value.clone()))
        })
        .collect();
    captured.sort_unstable_by_key(|(sequence, _)| *sequence);
    captured.into_iter().map(|(_, value)| value).collect()
}
