//! Bounded competing-consumer execution for acknowledged transports.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterStore, Delivery, Envelope, ErrorCode,
    MessageTransport,
};

/// Handles one delivered envelope before the framework acknowledges it.
///
/// Implementations receive only an immutable envelope, so acknowledgement ownership remains in
/// [`CompetingConsumer`]. A successful result commits the delivery; an error requests backend
/// redelivery through [`MessageTransport::nack`]. This makes the acknowledgement rule explicit
/// and prevents a handler from accidentally acknowledging work before its side effects complete.
#[async_trait]
pub trait DeliveryHandler: Send + Sync {
    /// Processes one envelope.
    ///
    /// Returning `Ok(())` acknowledges the delivery. Returning an error negatively acknowledges
    /// it and counts it as rejected work; it does not stop the consumer, because a redelivery is a
    /// normal outcome for at-least-once transports.
    async fn handle(&self, envelope: &Envelope) -> CatgaResult<()>;
}

/// Counts the delivery outcomes observed by one [`CompetingConsumer`] run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConsumerRun {
    received: usize,
    acknowledged: usize,
    rejected: usize,
    dead_lettered: usize,
}

impl ConsumerRun {
    /// Returns how many deliveries the consumer accepted from its transport.
    pub const fn received(self) -> usize {
        self.received
    }

    /// Returns how many handler successes were acknowledged by the transport.
    pub const fn acknowledged(self) -> usize {
        self.acknowledged
    }

    /// Returns how many handler failures were negatively acknowledged for redelivery.
    pub const fn rejected(self) -> usize {
        self.rejected
    }

    /// Returns how many terminal handler failures were stored as dead letters.
    pub const fn dead_lettered(self) -> usize {
        self.dead_lettered
    }

    fn record(&mut self, outcome: DeliveryOutcome) {
        match outcome {
            DeliveryOutcome::Acknowledged => self.acknowledged += 1,
            DeliveryOutcome::Rejected => self.rejected += 1,
            DeliveryOutcome::DeadLettered => {
                self.acknowledged += 1;
                self.dead_lettered += 1;
            }
        }
    }
}

/// Runs a single transport consumer with a bounded number of in-flight handlers.
///
/// Competing-consumer membership belongs to the transport configuration: for example, Redis uses
/// a stream consumer group and NATS uses a durable JetStream consumer. Creating multiple runners
/// against the same configured group distributes deliveries without a framework-level broker
/// abstraction or background task. The caller owns the task and cancellation token, which keeps
/// shutdown ordering visible and testable.
pub struct CompetingConsumer<T: ?Sized, H: ?Sized> {
    transport: Arc<T>,
    handler: Arc<H>,
    concurrency: NonZeroUsize,
    dead_letters: Option<Arc<DeadLetterPolicy>>,
}

struct DeadLetterPolicy {
    max_attempts: NonZeroU32,
    store: Arc<dyn DeadLetterStore>,
}

impl<T: ?Sized, H: ?Sized> CompetingConsumer<T, H>
where
    T: MessageTransport,
    H: DeliveryHandler,
{
    /// Creates a consumer with at most `concurrency` simultaneous handler calls.
    ///
    /// A positive limit is required to guarantee progress and bound memory retained by pending
    /// deliveries. The runner never spawns unbounded tasks: it keeps at most this many delivery
    /// futures in its local bounded stream buffer.
    pub fn new(transport: Arc<T>, handler: Arc<H>, concurrency: usize) -> CatgaResult<Self> {
        let concurrency = NonZeroUsize::new(concurrency).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "consumer concurrency must be greater than zero",
            )
        })?;
        Ok(Self {
            transport,
            handler,
            concurrency,
            dead_letters: None,
        })
    }

    /// Returns the maximum number of simultaneous handler calls.
    pub const fn concurrency(&self) -> NonZeroUsize {
        self.concurrency
    }

    /// Configures terminal-failure handling with one durable dead-letter store.
    ///
    /// A failing delivery is placed in `store` once the backend reports at
    /// least `max_attempts` deliveries. The original delivery is acknowledged
    /// only after the dead letter was persisted. If persistence fails, the
    /// delivery is negatively acknowledged for retry, so this policy never
    /// silently drops work.
    pub fn with_dead_letters<S>(mut self, max_attempts: u32, store: Arc<S>) -> CatgaResult<Self>
    where
        S: DeadLetterStore + 'static,
    {
        let max_attempts = NonZeroU32::new(max_attempts).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "consumer maximum delivery attempts must be greater than zero",
            )
        })?;
        self.dead_letters = Some(Arc::new(DeadLetterPolicy {
            max_attempts,
            store,
        }));
        Ok(self)
    }

    /// Receives and handles deliveries until `shutdown` is cancelled.
    ///
    /// Cancellation stops new receives immediately, then drains already accepted deliveries so
    /// that every acknowledgement token is resolved. A receive, acknowledgement, or negative
    /// acknowledgement failure is returned to the caller because continuing could otherwise hide
    /// a lost delivery. Handler failures are instead negatively acknowledged and included in the
    /// returned [`ConsumerRun`].
    pub async fn run_until_cancelled(
        &self,
        shutdown: CancellationToken,
    ) -> CatgaResult<ConsumerRun> {
        let mut run = ConsumerRun::default();
        let transport = Arc::clone(&self.transport);
        let receiver_shutdown = shutdown.clone();
        let deliveries = stream::unfold((), move |_| {
            let transport = Arc::clone(&transport);
            let shutdown = receiver_shutdown.clone();
            async move {
                tokio::select! {
                    _ = shutdown.cancelled() => None,
                    delivery = transport.receive() => Some((delivery, ())),
                }
            }
        });
        let handler = Arc::clone(&self.handler);
        let dead_letters = self.dead_letters.clone();
        let work = deliveries
            .map(move |delivery| {
                let transport = Arc::clone(&self.transport);
                let handler = Arc::clone(&handler);
                let dead_letters = dead_letters.clone();
                async move {
                    let delivery = delivery?;
                    process_delivery(transport, handler, dead_letters, delivery).await
                }
            })
            .buffer_unordered(self.concurrency.get());
        futures::pin_mut!(work);
        while let Some(outcome) = work.next().await {
            run.received += 1;
            run.record(outcome?);
        }
        Ok(run)
    }
}

#[derive(Clone, Copy)]
enum DeliveryOutcome {
    Acknowledged,
    Rejected,
    DeadLettered,
}

async fn process_delivery<T, H>(
    transport: Arc<T>,
    handler: Arc<H>,
    dead_letters: Option<Arc<DeadLetterPolicy>>,
    delivery: Delivery,
) -> CatgaResult<DeliveryOutcome>
where
    T: ?Sized + MessageTransport,
    H: ?Sized + DeliveryHandler,
{
    match handler.handle(delivery.envelope()).await {
        Ok(()) => {
            transport.ack(delivery).await?;
            Ok(DeliveryOutcome::Acknowledged)
        }
        Err(error) => {
            if let Some(policy) = dead_letters
                && delivery.attempts() >= policy.max_attempts.get()
            {
                let letter = DeadLetter::new(
                    delivery.envelope().clone(),
                    error.message(),
                    delivery.attempts(),
                );
                match policy.store.enqueue(letter).await {
                    Ok(()) => {
                        transport.ack(delivery).await?;
                        return Ok(DeliveryOutcome::DeadLettered);
                    }
                    Err(_) => {
                        transport.nack(delivery).await?;
                        return Ok(DeliveryOutcome::Rejected);
                    }
                }
            }
            transport.nack(delivery).await?;
            Ok(DeliveryOutcome::Rejected)
        }
    }
}
