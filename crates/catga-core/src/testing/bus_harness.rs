//! In-process bus test harness: drive real transport consumption without a broker.
//!
//! This complements [`crate::CatgaTestHarness`], which dispatches directly through the mediator.
//! `BusTestHarness` instead exercises the full consume path: a message is published to a bounded
//! in-memory transport, then a real [`Bus`] decodes, handles, and acknowledges each delivery
//! (including dead-letter behavior). It is the Rust counterpart to MassTransit's in-memory test
//! harness, and follows the same `new() -> start() -> Running` shape as [`crate::CatgaTestHarness`].
//!
//! Each registered endpoint owns a separate in-memory queue. Publishing routes the message to
//! the queue whose endpoint was registered for that message type, so consumers never receive
//! messages of an unexpected type.
//!
//! ```
//! use catga_codec_memorypack::MemoryPackable;
//! use crate::{CatgaResult, Message, TypedDeliveryHandler};
//! use catga_testing::BusTestHarness;
//!
//! #[derive(Clone, MemoryPackable)]
//! struct Ping(u32);
//! impl Message for Ping {}
//!
//! struct Record;
//! #[async_trait::async_trait]
//! impl TypedDeliveryHandler<Ping> for Record {
//!     async fn handle(&self, _: &Ping) -> CatgaResult<()> { Ok(()) }
//! }
//!
//! # async fn run() -> CatgaResult<()> {
//! let mut harness = BusTestHarness::new()?;
//! harness.endpoint("pings", Record)?;
//! let harness = harness.start();
//! harness.publish(&Ping(1)).await?;
//! harness.run_until_consumed::<Ping>(1).await?;
//! assert_eq!(harness.consumed::<Ping>().len(), 1);
//! # Ok(())
//! # }
//! ```

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackSerialize, MemoryPackTransport,
};
use crate::auto::{Bus, BusBuilder};
use crate::{
    CatgaError, CatgaResult, ErrorCode, Message, SnowflakeIdGenerator, SnowflakeLayout,
    TypedDeliveryHandler,
};
use catga_memory::MemoryTransport;

const DEFAULT_CAPACITY: usize = 64;
const DEFAULT_CONDITION_TIMEOUT: Duration = Duration::from_secs(10);
const CONDITION_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Type-erased publish facade so the running harness can route by [`TypeId`].
#[async_trait]
trait TypePublisher: Send + Sync {
    async fn publish_any(&self, message: &(dyn Any + Send + Sync)) -> CatgaResult<()>;
}

struct TypedPublisher<M: Message + MemoryPackSerialize> {
    inner: MemoryPackTransport<MemoryTransport>,
    marker: PhantomData<fn(M)>,
}

#[async_trait]
impl<M: Message + MemoryPackSerialize> TypePublisher for TypedPublisher<M> {
    async fn publish_any(&self, message: &(dyn Any + Send + Sync)) -> CatgaResult<()> {
        let message = message.downcast_ref::<M>().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "message type mismatch in harness publisher",
            )
        })?;
        self.inner.publish(message).await
    }
}

/// Builds an in-process bus harness: register endpoints, then call [`Self::start`].
pub struct BusTestHarness {
    capacity: usize,
    builder: BusBuilder<MemoryTransport>,
    publishers: HashMap<TypeId, Arc<dyn TypePublisher>>,
    consumed: Arc<ConsumedLog>,
    condition_timeout: Duration,
}

impl BusTestHarness {
    /// Creates a harness backed by bounded in-memory transports.
    pub fn new() -> CatgaResult<Self> {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Creates a harness with an explicit per-endpoint transport capacity.
    pub fn with_capacity(capacity: usize) -> CatgaResult<Self> {
        let placeholder = Arc::new(MemoryTransport::new(capacity)?);
        let builder = Bus::builder(placeholder);
        Ok(Self {
            capacity,
            builder,
            publishers: HashMap::new(),
            consumed: Arc::new(ConsumedLog::default()),
            condition_timeout: DEFAULT_CONDITION_TIMEOUT,
        })
    }

    /// Overrides how long `run_until*` waits for a condition before returning a timeout error.
    pub fn with_condition_timeout(mut self, timeout: Duration) -> Self {
        self.condition_timeout = timeout;
        self
    }

    /// Registers a typed consume endpoint that records each successfully consumed message.
    ///
    /// Each endpoint owns a separate in-memory queue; publishing a message of type `M` routes
    /// it exclusively to the endpoint registered for `M`. The endpoint runs with concurrency
    /// one so consumption order is deterministic for tests.
    pub fn endpoint<M, H>(&mut self, name: impl Into<String>, handler: H) -> CatgaResult<()>
    where
        M: Message + Clone + MemoryPackSerialize + MemoryPackDeserialize,
        H: TypedDeliveryHandler<M> + 'static,
    {
        let type_id = TypeId::of::<M>();
        if self.publishers.contains_key(&type_id) {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "an endpoint for this message type is already registered",
            ));
        }
        let transport = Arc::new(MemoryTransport::new(self.capacity)?);
        let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default())?);
        let publisher = MemoryPackTransport::new(Arc::clone(&transport), ids);
        self.publishers.insert(
            type_id,
            Arc::new(TypedPublisher::<M> {
                inner: publisher,
                marker: PhantomData,
            }),
        );
        let counting = CountingHandler {
            inner: handler,
            consumed: Arc::clone(&self.consumed),
            marker: PhantomData,
        };
        let builder = std::mem::replace(
            &mut self.builder,
            Bus::builder(Arc::new(MemoryTransport::new(self.capacity)?)),
        );
        self.builder = builder.endpoint_on(
            transport,
            name,
            Arc::new(counting),
            Arc::new(MemoryPackCodec::default()),
            1,
        )?;
        Ok(())
    }

    /// Builds the immutable bus and returns the running harness.
    pub fn start(self) -> RunningBusHarness {
        RunningBusHarness {
            publishers: self.publishers,
            bus: self.builder.build(),
            consumed: self.consumed,
            condition_timeout: self.condition_timeout,
        }
    }
}

/// A started harness: publish messages, drive consumption, and assert what was consumed.
pub struct RunningBusHarness {
    publishers: HashMap<TypeId, Arc<dyn TypePublisher>>,
    bus: Bus,
    consumed: Arc<ConsumedLog>,
    condition_timeout: Duration,
}

impl RunningBusHarness {
    /// Publishes one message to the endpoint registered for its type.
    pub async fn publish<M>(&self, message: &M) -> CatgaResult<()>
    where
        M: Message + MemoryPackSerialize,
    {
        let publisher = self.publishers.get(&TypeId::of::<M>()).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "no endpoint is registered for this message type",
            )
        })?;
        publisher.publish_any(message).await
    }

    /// Drives the bus until at least `count` values of `M` have been consumed, or times out.
    pub async fn run_until_consumed<M>(&self, count: usize) -> CatgaResult<()>
    where
        M: Message,
    {
        self.run_until(|consumed| consumed.count_of::<M>() >= count)
            .await
    }

    /// Drives the bus until `condition` observes the consumed log, or times out.
    ///
    /// The bus runs in the current task (its consume loop is intentionally not `Send`); this method
    /// returns once the condition holds and the bus has drained to a stop.
    pub async fn run_until<F>(&self, condition: F) -> CatgaResult<()>
    where
        F: Fn(&ConsumedLog) -> bool,
    {
        let token = self.bus.shutdown_token();
        let consumed = Arc::clone(&self.consumed);
        let timeout = self.condition_timeout;
        let driver = async move {
            let deadline = Instant::now() + timeout;
            while !condition(&consumed) {
                if Instant::now() > deadline {
                    token.cancel();
                    return Err(CatgaError::new(
                        ErrorCode::Timeout,
                        "bus harness condition was not met within the timeout",
                    ));
                }
                tokio::time::sleep(CONDITION_POLL_INTERVAL).await;
            }
            token.cancel();
            Ok(())
        };
        let (runs, driver_result) = tokio::join!(self.bus.run_until_cancelled(), driver);
        driver_result?;
        runs.map(|_| ())
    }

    /// Returns consumed values of one message type in consumption order.
    pub fn consumed<M>(&self) -> Vec<M>
    where
        M: Message + Clone,
    {
        self.consumed.of::<M>()
    }

    /// Returns how many values of one message type have been consumed.
    pub fn consumed_count<M>(&self) -> usize
    where
        M: Message,
    {
        self.consumed.count_of::<M>()
    }
}

/// Wraps an application handler and records each successfully consumed message.
struct CountingHandler<M, H> {
    inner: H,
    consumed: Arc<ConsumedLog>,
    marker: PhantomData<fn(M)>,
}

#[async_trait]
impl<M, H> TypedDeliveryHandler<M> for CountingHandler<M, H>
where
    M: Message + Clone,
    H: TypedDeliveryHandler<M>,
{
    async fn handle(&self, message: &M) -> CatgaResult<()> {
        let result = self.inner.handle(message).await;
        if result.is_ok() {
            self.consumed.record(message.clone());
        }
        result
    }
}

/// Type-keyed record of consumed messages, queryable per message type.
#[derive(Default)]
pub struct ConsumedLog {
    entries: Mutex<Vec<(TypeId, Box<dyn Any + Send + Sync>)>>,
}

impl ConsumedLog {
    fn record<M>(&self, value: M)
    where
        M: Message,
    {
        self.entries
            .lock()
            .expect("consumed log not poisoned")
            .push((TypeId::of::<M>(), Box::new(value)));
    }

    /// Returns how many values of `M` have been recorded.
    pub fn count_of<M>(&self) -> usize
    where
        M: Message,
    {
        let type_id = TypeId::of::<M>();
        self.entries
            .lock()
            .expect("consumed log not poisoned")
            .iter()
            .filter(|(id, _)| *id == type_id)
            .count()
    }

    /// Returns recorded values of `M` in record order.
    pub fn of<M>(&self) -> Vec<M>
    where
        M: Message + Clone,
    {
        let type_id = TypeId::of::<M>();
        self.entries
            .lock()
            .expect("consumed log not poisoned")
            .iter()
            .filter_map(|(id, value)| {
                if *id == type_id {
                    value.downcast_ref::<M>().cloned()
                } else {
                    None
                }
            })
            .collect()
    }
}
