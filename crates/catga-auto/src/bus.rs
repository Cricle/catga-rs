//! A declarative bus facade: compose many receive endpoints into one runtime with unified startup
//! and shutdown.
//!
//! This is the Rust counterpart to MassTransit's `IBusControl`, kept deliberately thin. [`Bus`]
//! introduces no new pipe or context abstraction; it only composes the existing
//! [`CompetingConsumer`], [`TypedDeliveryHandler`], and a shutdown token. Cross-cutting middleware
//! (retry, dead-letter, circuit breaking) stays the responsibility of `catga-core`'s `Behavior`
//! family rather than a parallel pipe layer.
//!
//! # Endpoint Registration
//!
//! Each endpoint requires four arguments: a name, a handler, a codec, and concurrency. The handler
//! must implement [`TypedDeliveryHandler<M>`] where `M` is the message type. Use a struct with
//! `#[async_trait]` to define the handler, then register it in `Arc`:
//!
//! ```no_run
//! use std::sync::Arc;
//! use catga_auto::Bus;
//! use catga_codec_memorypack::MemoryPackCodec;
//! use catga_memory::MemoryTransport;
//! use catga_core::{CatgaResult, Message, TypedDeliveryHandler};
//!
//! #[derive(catga_codec_memorypack::MemoryPackable)]
//! struct OrderPlaced { order_id: u32 }
//! impl Message for OrderPlaced {}
//!
//! struct OrderHandler;
//!
//! #[async_trait::async_trait]
//! impl TypedDeliveryHandler<OrderPlaced> for OrderHandler {
//!     async fn handle(&self, event: &OrderPlaced) -> CatgaResult<()> {
//!         println!("order {} placed", event.order_id);
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> CatgaResult<()> {
//! let transport = Arc::new(MemoryTransport::new(64)?);
//! let bus = Bus::builder(transport)
//!     .endpoint("orders", Arc::new(OrderHandler), Arc::new(MemoryPackCodec::default()), 8)?
//!     .build();
//! bus.run_until_cancelled().await?;
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CompetingConsumer, ConsumerRun, DeadLetterStore, Delivery,
    DeliveryHandler, Destination, DestinationTransport, DistributedIdGenerator, Envelope,
    ErrorCode, Fault, Message, MessageDestinationRouter, MessageTransport, PayloadDecoder,
    PayloadEncoder, RemoteRequest, RequestTransport, ShutdownCoordinator, SnowflakeIdGenerator,
    SnowflakeLayout, TypedDeliveryHandler, TypedTransport,
};
use futures::future::LocalBoxFuture;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// A receive endpoint that a [`Bus`] can drive, behind a type-erased boundary.
///
/// The consume loop's future is intentionally not `Send` (the delivery handle is deliberately not
/// `Sync`, expressing acknowledgement uniqueness through ownership), so `run` returns a local
/// future and the whole bus must be driven within a single task (see
/// [`Bus::run_until_cancelled`]).
trait EndpointRunner: Send + Sync {
    /// Returns the endpoint's configured name.
    fn name(&self) -> &str;

    /// Drives the endpoint's consume loop until `shutdown` is cancelled.
    fn run(&self, shutdown: CancellationToken) -> LocalBoxFuture<'_, CatgaResult<ConsumerRun>>;
}

/// Wraps one concrete consumer as a named endpoint.
struct ConsumerEndpoint<T: MessageTransport, H: ?Sized> {
    name: String,
    consumer: CompetingConsumer<T, H>,
}

impl<T, H> EndpointRunner for ConsumerEndpoint<T, H>
where
    T: MessageTransport,
    H: ?Sized + DeliveryHandler,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn run(&self, shutdown: CancellationToken) -> LocalBoxFuture<'_, CatgaResult<ConsumerRun>> {
        Box::pin(self.consumer.run_until_cancelled(shutdown))
    }
}

/// Declaratively builds one immutable [`Bus`].
pub struct BusBuilder<T: MessageTransport> {
    transport: Arc<T>,
    endpoints: Vec<Arc<dyn EndpointRunner>>,
    shutdown: ShutdownCoordinator,
    router: MessageDestinationRouter,
}

impl<T: MessageTransport + 'static> BusBuilder<T> {
    /// Starts a builder around one shared transport used by every endpoint.
    pub fn new(transport: Arc<T>) -> Self {
        Self {
            transport,
            endpoints: Vec::new(),
            shutdown: ShutdownCoordinator::default(),
            router: MessageDestinationRouter::new(),
        }
    }

    /// Uses an application-owned shutdown coordinator instead of a fresh one.
    pub fn with_shutdown(mut self, shutdown: ShutdownCoordinator) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Adds one typed receive endpoint that decodes each envelope into `M` before calling `handler`.
    ///
    /// `concurrency` bounds simultaneous handler calls (it must be positive). Acknowledgement
    /// ownership stays with the consumer: a handler error requests redelivery, never a premature
    /// acknowledgement.
    pub fn endpoint<M, H, C>(
        mut self,
        name: impl Into<String>,
        handler: Arc<H>,
        decoder: Arc<C>,
        concurrency: usize,
    ) -> CatgaResult<Self>
    where
        M: Message,
        H: TypedDeliveryHandler<M> + 'static,
        C: PayloadDecoder<M> + 'static,
    {
        let consumer =
            CompetingConsumer::typed(self.transport.clone(), handler, decoder, concurrency)?;
        self.endpoints.push(Arc::new(ConsumerEndpoint {
            name: name.into(),
            consumer,
        }));
        Ok(self)
    }

    /// Adds one typed receive endpoint driven by an explicit transport instance.
    ///
    /// Unlike [`Self::endpoint`], which uses the builder's shared transport, this variant lets
    /// each endpoint consume from its own transport. This is useful for test harnesses that
    /// route messages by type through separate queues.
    pub fn endpoint_on<M, H, C>(
        mut self,
        transport: Arc<T>,
        name: impl Into<String>,
        handler: Arc<H>,
        decoder: Arc<C>,
        concurrency: usize,
    ) -> CatgaResult<Self>
    where
        M: Message,
        H: TypedDeliveryHandler<M> + 'static,
        C: PayloadDecoder<M> + 'static,
    {
        let consumer = CompetingConsumer::typed(transport, handler, decoder, concurrency)?;
        self.endpoints.push(Arc::new(ConsumerEndpoint {
            name: name.into(),
            consumer,
        }));
        Ok(self)
    }

    /// Adds one typed receive endpoint with a terminal dead-letter policy.
    ///
    /// A delivery still failing once the backend reports at least `max_attempts` attempts is
    /// written to `dead_letters` and then acknowledged, so a poison message cannot redeliver
    /// forever. On transports without redelivery (such as the in-memory transport) the first
    /// failure already counts as attempt one, so `max_attempts = 1` dead-letters it immediately.
    pub fn endpoint_with_dead_letters<M, H, C, S>(
        mut self,
        name: impl Into<String>,
        handler: Arc<H>,
        decoder: Arc<C>,
        concurrency: usize,
        max_attempts: u32,
        dead_letters: Arc<S>,
    ) -> CatgaResult<Self>
    where
        M: Message,
        H: TypedDeliveryHandler<M> + 'static,
        C: PayloadDecoder<M> + 'static,
        S: DeadLetterStore + 'static,
    {
        let consumer =
            CompetingConsumer::typed(self.transport.clone(), handler, decoder, concurrency)?
                .with_dead_letters(max_attempts, dead_letters)?;
        self.endpoints.push(Arc::new(ConsumerEndpoint {
            name: name.into(),
            consumer,
        }));
        Ok(self)
    }

    /// Adds one envelope-level receive endpoint for handlers that inspect raw envelopes.
    pub fn endpoint_raw<H>(
        mut self,
        name: impl Into<String>,
        handler: Arc<H>,
        concurrency: usize,
    ) -> CatgaResult<Self>
    where
        H: DeliveryHandler + 'static,
    {
        let consumer = CompetingConsumer::new(self.transport.clone(), handler, concurrency)?;
        self.endpoints.push(Arc::new(ConsumerEndpoint {
            name: name.into(),
            consumer,
        }));
        Ok(self)
    }

    /// Adds one typed receive endpoint that consumes from its own named destination.
    ///
    /// The endpoint name becomes the destination. A route is registered so that
    /// [`BusPublisher::publish`] sends messages of type `M` to this destination. Duplicate
    /// message-type routes return [`ErrorCode::Validation`]. Backends that require asynchronous
    /// resource provisioning, such as NATS JetStream, must provision the destination before this
    /// method is called; an unprovisioned destination returns [`ErrorCode::NotFound`] here rather
    /// than failing after the bus starts.
    pub fn routed_endpoint<M, H, C>(
        mut self,
        name: impl Into<String>,
        handler: Arc<H>,
        decoder: Arc<C>,
        concurrency: usize,
    ) -> CatgaResult<Self>
    where
        T: DestinationTransport,
        M: Message,
        H: TypedDeliveryHandler<M> + 'static,
        C: PayloadDecoder<M> + 'static,
    {
        let name = name.into();
        let destination = Destination::parse(name.clone())?;
        self.transport.declare_destination(&destination)?;
        let view = Arc::new(DestinationView {
            transport: Arc::clone(&self.transport),
            destination: destination.clone(),
        });
        let consumer = CompetingConsumer::typed(view, handler, decoder, concurrency)?;
        self.endpoints
            .push(Arc::new(ConsumerEndpoint { name, consumer }));
        self.router
            .add_route(std::any::type_name::<M>(), destination)?;
        Ok(self)
    }

    /// Builds the bus and a typed publisher that routes via the registered topology.
    ///
    /// The publisher encodes messages with `codec` and sends them to the destination
    /// registered for their message type. Publishing an unrouted type returns
    /// [`ErrorCode::NotFound`].
    pub fn build_with_publisher<C>(self, codec: C) -> CatgaResult<(Bus, BusPublisher<T, C>)>
    where
        T: DestinationTransport,
        C: Default + Send + Sync + 'static,
    {
        let router = Arc::new(self.router);
        let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default())?);
        let typed = TypedTransport::new_with_codec(
            Arc::clone(&self.transport),
            ids as Arc<dyn DistributedIdGenerator>,
            codec,
        );
        let publisher = BusPublisher {
            inner: typed,
            router: Arc::clone(&router),
        };
        let bus = Bus {
            endpoints: self.endpoints,
            shutdown: self.shutdown,
        };
        Ok((bus, publisher))
    }

    /// Builds the immutable bus.
    pub fn build(self) -> Bus {
        Bus {
            endpoints: self.endpoints,
            shutdown: self.shutdown,
        }
    }
}

/// An immutable bus that owns all receive endpoints and starts/stops them together.
pub struct Bus {
    endpoints: Vec<Arc<dyn EndpointRunner>>,
    shutdown: ShutdownCoordinator,
}

impl Bus {
    /// Starts a builder around one shared transport.
    pub fn builder<T: MessageTransport + 'static>(transport: Arc<T>) -> BusBuilder<T> {
        BusBuilder::new(transport)
    }

    /// Returns the configured endpoint names in registration order.
    pub fn endpoint_names(&self) -> Vec<&str> {
        self.endpoints
            .iter()
            .map(|endpoint| endpoint.name())
            .collect()
    }

    /// Returns a token cancelled when shutdown is requested.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.token()
    }

    /// Idempotently requests shutdown of every endpoint.
    pub fn shutdown(&self) {
        self.shutdown.request_shutdown();
    }

    /// Drives every endpoint concurrently within the current task until shutdown is requested.
    ///
    /// Returns each endpoint's run statistics in registration order. The returned future is not
    /// `Send`; await it inside a `current_thread` runtime or a `LocalSet` task rather than spawning
    /// it onto a multi-threaded pool.
    pub async fn run_until_cancelled(&self) -> CatgaResult<Vec<ConsumerRun>> {
        let span = tracing::info_span!(
            target: catga_core::TRACING_TARGET,
            "catga.bus.run",
            catga_kind = "bus",
            endpoints = self.endpoints.len(),
            outcome = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
        );
        let started = std::time::Instant::now();
        metrics::gauge!("catga.bus.endpoints").set(self.endpoints.len() as f64);

        let result = async {
            let token = self.shutdown.token();
            futures::future::try_join_all(
                self.endpoints
                    .iter()
                    .map(|endpoint| endpoint.run(token.clone())),
            )
            .await
        }
        .instrument(span.clone())
        .await;

        let elapsed = started.elapsed().as_millis() as f64;
        span.record("duration_ms", elapsed);
        metrics::histogram!("catga.bus.run.duration").record(elapsed);

        match &result {
            Ok(runs) => {
                span.record("outcome", "success");
                for (endpoint, run) in self.endpoints.iter().zip(runs.iter()) {
                    metrics::counter!(
                        "catga.bus.messages.consumed",
                        "endpoint" => endpoint.name().to_owned()
                    )
                    .increment(run.acknowledged() as u64);
                }
            }
            Err(_) => {
                span.record("outcome", "failure");
                self.shutdown.request_shutdown();
            }
        }
        result
    }
}

/// Routes `MessageTransport` operations through one named destination of a
/// [`DestinationTransport`], giving each routed endpoint an isolated queue.
struct DestinationView<T: ?Sized> {
    transport: Arc<T>,
    destination: Destination,
}

#[async_trait]
impl<T> MessageTransport for DestinationView<T>
where
    T: DestinationTransport + ?Sized,
{
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        self.transport.send_to(&self.destination, envelope).await
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        self.transport.receive_from(&self.destination).await
    }

    async fn ack(&self, delivery: Delivery) -> CatgaResult<()> {
        self.transport.ack(delivery).await
    }
}

/// A typed publisher that routes messages through the bus topology.
///
/// Created by [`BusBuilder::build_with_publisher`]. Each `publish` call encodes the message,
/// resolves its destination from the topology registered during endpoint configuration, and
/// sends it to that destination.
pub struct BusPublisher<T: ?Sized, C> {
    inner: TypedTransport<T, C>,
    router: Arc<MessageDestinationRouter>,
}

impl<T, C> BusPublisher<T, C>
where
    T: DestinationTransport + ?Sized,
{
    /// Publishes one message to the destination registered for its message type.
    ///
    /// Returns [`ErrorCode::NotFound`] when no endpoint was registered for the message type.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        self.inner.send_routed(&self.router, message).await
    }

    /// Returns the shared topology router for constructing request clients.
    pub fn router(&self) -> &Arc<MessageDestinationRouter> {
        &self.router
    }

    /// Publishes a message after a delay.
    ///
    /// The caller owns the timing: await this future to block until delivery, or spawn it
    /// for non-blocking scheduled publication. Transport-native scheduling (e.g. NATS
    /// delayed Nak) is not used; this is a portable timer-based approach.
    pub async fn schedule<M>(&self, message: &M, delay: Duration) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        tokio::time::sleep(delay).await;
        self.publish(message).await
    }
}

/// A late-bound, cloneable handle to a [`BusPublisher`].
///
/// Create the handle before building the bus, give clones to handlers that need to publish,
/// then call [`Self::bind`] with the publisher returned by [`BusBuilder::build_with_publisher`].
/// All clones share the same binding slot, so one `bind` activates every handle.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use catga_auto::{Bus, PublisherHandle};
/// # use catga_codec_memorypack::MemoryPackCodec;
/// # use catga_memory::MemoryTransport;
/// let handle = PublisherHandle::<MemoryTransport, MemoryPackCodec>::new();
/// let clone_for_handler = handle.clone();
/// // ... pass clone_for_handler to a handler, build the bus ...
/// // let (bus, publisher) = builder.build_with_publisher(codec)?;
/// // handle.bind(publisher);
/// // clone_for_handler is now active too.
/// ```
pub struct PublisherHandle<T: ?Sized, C> {
    slot: Arc<OnceLock<BusPublisher<T, C>>>,
}

impl<T: ?Sized, C> Clone for PublisherHandle<T, C> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<T: ?Sized, C> Default for PublisherHandle<T, C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ?Sized, C> PublisherHandle<T, C> {
    /// Creates an unbound handle. Publishing before [`Self::bind`] returns
    /// [`ErrorCode::Unavailable`].
    pub fn new() -> Self {
        Self {
            slot: Arc::new(OnceLock::new()),
        }
    }

    /// Returns whether the handle has been bound to a publisher.
    pub fn is_bound(&self) -> bool {
        self.slot.get().is_some()
    }
}

impl<T, C> PublisherHandle<T, C>
where
    T: DestinationTransport + ?Sized,
{
    /// Binds the publisher, activating this handle and all its clones.
    ///
    /// Returns the publisher back if the handle was already bound (idempotent).
    pub fn bind(&self, publisher: BusPublisher<T, C>) -> Option<BusPublisher<T, C>> {
        self.slot.set(publisher).err()
    }

    /// Publishes one message through the bound publisher.
    ///
    /// Returns [`ErrorCode::Unavailable`] if [`Self::bind`] has not been called.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        match self.slot.get() {
            Some(publisher) => publisher.publish(message).await,
            None => Err(CatgaError::new(
                ErrorCode::Unavailable,
                "publisher handle is not bound; call bind() after building the bus",
            )),
        }
    }

    /// Publishes a message after a delay through the bound publisher.
    ///
    /// Returns [`ErrorCode::Unavailable`] if [`Self::bind`] has not been called.
    pub async fn schedule<M>(&self, message: &M, delay: Duration) -> CatgaResult<()>
    where
        M: Message,
        C: PayloadEncoder<M>,
    {
        match self.slot.get() {
            Some(publisher) => publisher.schedule(message, delay).await,
            None => Err(CatgaError::new(
                ErrorCode::Unavailable,
                "publisher handle is not bound; call bind() after building the bus",
            )),
        }
    }
}

/// A typed request/reply client that routes requests through the bus topology.
///
/// Unlike [`catga_core::EnvelopeRequestClient`] which is bound to one fixed destination, this
/// client resolves the destination per message type from the shared [`MessageDestinationRouter`].
/// One client serves all registered request types.
///
/// Create it from a [`BusPublisher`]'s router after building the bus:
/// ```no_run
/// # use std::{sync::Arc, time::Duration};
/// # use catga_auto::{Bus, BusRequestClient};
/// # use catga_codec_memorypack::MemoryPackCodec;
/// # use catga_nats::NatsRequestClient as NatsRpc;
/// # async fn example(rpc: Arc<NatsRpc>, router: Arc<catga_core::MessageDestinationRouter>) {
/// let client = BusRequestClient::new(rpc, router, MemoryPackCodec::default(), Duration::from_secs(5))
///     .expect("valid");
/// # }
/// ```
pub struct BusRequestClient<T: ?Sized, C> {
    transport: Arc<T>,
    router: Arc<MessageDestinationRouter>,
    codec: C,
    ids: Arc<dyn DistributedIdGenerator>,
    timeout: Duration,
}

impl<T, C> BusRequestClient<T, C>
where
    T: RequestTransport + ?Sized,
    C: Default,
{
    /// Creates a routed request client with a default codec instance.
    pub fn new(
        transport: Arc<T>,
        router: Arc<MessageDestinationRouter>,
        codec: C,
        timeout: Duration,
    ) -> CatgaResult<Self> {
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "request client timeout must be greater than zero",
            ));
        }
        let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default())?);
        Ok(Self {
            transport,
            router,
            codec,
            ids,
            timeout,
        })
    }
}

impl<T, C> BusRequestClient<T, C>
where
    T: RequestTransport + ?Sized,
{
    /// Sends a typed request to the destination registered for its message type.
    ///
    /// Returns [`ErrorCode::NotFound`] when no route is configured for the request type.
    /// The reply payload is decoded as a MemoryPack RPC response envelope; a remote failure
    /// is surfaced as the original [`CatgaError`].
    pub async fn request<M>(&self, message: &M) -> CatgaResult<M::Response>
    where
        M: RemoteRequest,
        M::Response: catga_codec_memorypack::MemoryPackDeserialize,
        C: PayloadEncoder<M>
            + PayloadDecoder<catga_codec_memorypack::MemoryPackRpcResponse<M::Response>>,
    {
        let destination = self.router.resolve(message.message_type()).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "no route is configured for this request type",
            )
        })?;
        let message_id = self.ids.next_id()?;
        let payload = self.codec.encode_payload(message)?;
        let envelope = Envelope::versioned(
            message_id,
            message.message_type(),
            payload,
            catga_core::MessageMetadata::new(message_id, Some(message_id)),
            message.schema_version(),
        );
        let reply = self
            .transport
            .request(destination.as_str(), envelope, self.timeout)
            .await?;
        let rpc_response: catga_codec_memorypack::MemoryPackRpcResponse<M::Response> =
            self.codec.decode_payload(reply.payload())?;
        match rpc_response {
            catga_codec_memorypack::MemoryPackRpcResponse::Success(value) => Ok(value),
            catga_codec_memorypack::MemoryPackRpcResponse::Failure(error) => Err(error),
        }
    }
}

/// Sink for bus-level fault notifications published when a handler fails.
///
/// Implementations receive the fault best-effort; publication failures are discarded by
/// [`FaultPublishingHandler`] and never mask the original handler error.
#[async_trait]
pub trait BusFaultPublisher<M: Message>: Send + Sync {
    /// Publishes one fault notification.
    async fn publish_fault(&self, fault: Fault<M>) -> CatgaResult<()>;
}

/// Wraps a handler and publishes [`Fault<M>`] best-effort on handler failure.
///
/// The original error is always returned unchanged; fault publication is fire-and-forget.
/// This is the Bus-path equivalent of the mediator pipeline's `FaultPublishingBehavior`.
pub struct FaultPublishingHandler<M, H: ?Sized, P: ?Sized> {
    inner: Arc<H>,
    publisher: Arc<P>,
    marker: PhantomData<fn(M)>,
}

impl<M, H: ?Sized, P: ?Sized> FaultPublishingHandler<M, H, P> {
    /// Wraps `inner` with fault publication via `publisher`.
    pub fn new(inner: Arc<H>, publisher: Arc<P>) -> Self {
        Self {
            inner,
            publisher,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<M, H, P> TypedDeliveryHandler<M> for FaultPublishingHandler<M, H, P>
where
    M: Message + Clone,
    H: ?Sized + TypedDeliveryHandler<M>,
    P: ?Sized + BusFaultPublisher<M>,
{
    async fn handle(&self, message: &M) -> CatgaResult<()> {
        match self.inner.handle(message).await {
            Err(error) => {
                let _ = self
                    .publisher
                    .publish_fault(Fault::new(message.clone(), error.clone()))
                    .await;
                Err(error)
            }
            ok => ok,
        }
    }
}

/// Adapts a [`StateMachineEventRouter`](catga_flow::StateMachineEventRouter) as a Bus endpoint handler.
///
/// Each delivered event is routed to the state-machine instance selected by the router's
/// registered instance-id resolver. Routing errors propagate as handler failures, triggering
/// the consumer's nack/dead-letter policy.
#[cfg(feature = "flow")]
pub struct StateMachineHandler<S, K, Store, E> {
    router: Arc<catga_flow::StateMachineEventRouter<S, K, Store>>,
    marker: PhantomData<fn(E)>,
}

#[cfg(feature = "flow")]
impl<S, K, Store, E> StateMachineHandler<S, K, Store, E> {
    /// Creates a handler that routes delivered events through `router`.
    pub fn new(router: Arc<catga_flow::StateMachineEventRouter<S, K, Store>>) -> Self {
        Self {
            router,
            marker: PhantomData,
        }
    }
}

#[cfg(feature = "flow")]
#[async_trait]
impl<S, K, Store, E> TypedDeliveryHandler<E> for StateMachineHandler<S, K, Store, E>
where
    S: catga_flow::StateMachineState<K>,
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
    Store: catga_flow::StateMachineStore<S> + 'static,
    E: catga_core::Event,
{
    async fn handle(&self, event: &E) -> CatgaResult<()> {
        self.router.route(event).await.map(|_| ())
    }
}

/// Forwards raw envelopes from one destination to another.
///
/// Useful for operational tasks: draining a dead-letter queue, migrating messages between
/// environments, or replaying events into a new projection. The forwarder acknowledges each
/// source delivery only after the target send succeeds, preventing message loss.
pub struct MessageForwarder<T: ?Sized> {
    transport: Arc<T>,
}

impl<T> MessageForwarder<T>
where
    T: DestinationTransport + ?Sized,
{
    /// Creates a forwarder over a destination-capable transport.
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    /// Moves up to `max` envelopes from `source` to `target`.
    ///
    /// Stops early when the source queue is exhausted (detected via a short receive timeout).
    /// Returns the number of envelopes successfully forwarded.
    pub async fn forward(
        &self,
        source: &Destination,
        target: &Destination,
        max: usize,
    ) -> CatgaResult<usize> {
        let mut count = 0;
        while count < max {
            let delivery = match tokio::time::timeout(
                Duration::from_millis(50),
                self.transport.receive_from(source),
            )
            .await
            {
                Ok(Ok(delivery)) => delivery,
                Ok(Err(error)) => return Err(error),
                Err(_) => break,
            };
            let envelope = delivery.envelope().clone();
            self.transport.send_to(target, envelope).await?;
            self.transport.ack(delivery).await?;
            count += 1;
        }
        Ok(count)
    }
}

/// Delivers only messages matching a predicate; non-matching messages are acknowledged as no-ops.
///
/// This is the Bus equivalent of MassTransit's consumer filter: the endpoint still receives
/// every message of type `M`, but the inner handler is invoked only when `predicate` returns
/// `true`. Filtered-out messages are acked immediately (not nacked), preventing redelivery loops.
pub struct FilteredHandler<M, H: ?Sized, F> {
    inner: Arc<H>,
    predicate: F,
    marker: PhantomData<fn(M)>,
}

impl<M, H: ?Sized, F> FilteredHandler<M, H, F>
where
    F: Fn(&M) -> bool + Send + Sync,
{
    /// Wraps `inner` so it only sees messages for which `predicate` returns true.
    pub fn new(inner: Arc<H>, predicate: F) -> Self {
        Self {
            inner,
            predicate,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<M, H, F> TypedDeliveryHandler<M> for FilteredHandler<M, H, F>
where
    M: Message,
    H: ?Sized + TypedDeliveryHandler<M>,
    F: Fn(&M) -> bool + Send + Sync,
{
    async fn handle(&self, message: &M) -> CatgaResult<()> {
        if (self.predicate)(message) {
            self.inner.handle(message).await
        } else {
            Ok(())
        }
    }
}
