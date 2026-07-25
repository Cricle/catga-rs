//! Competing transport consumer tests.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, CompetingConsumer, DeadLetter, DeadLetterStore,
    Delivery, DeliveryHandler, Envelope, ErrorCode, MessageMetadata, MessageTransport,
};
use catga_memory::MemoryDeadLetters;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Acknowledgements {
    acknowledged: AtomicUsize,
    rejected: AtomicUsize,
}

struct TestAcknowledger {
    acknowledgements: Arc<Acknowledgements>,
}

#[async_trait]
impl Acknowledger for TestAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.acknowledgements
            .acknowledged
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.acknowledgements
            .rejected
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct QueueTransport {
    deliveries: Mutex<VecDeque<(u64, u32)>>,
    acknowledgements: Arc<Acknowledgements>,
}

impl QueueTransport {
    fn new(ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            deliveries: Mutex::new(ids.into_iter().map(|id| (id, 1)).collect()),
            acknowledgements: Arc::new(Acknowledgements::default()),
        }
    }

    fn with_attempts(deliveries: impl IntoIterator<Item = (u64, u32)>) -> Self {
        Self {
            deliveries: Mutex::new(deliveries.into_iter().collect()),
            acknowledgements: Arc::new(Acknowledgements::default()),
        }
    }
}

#[async_trait]
impl MessageTransport for QueueTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let delivery = self.deliveries.lock().await.pop_front();
        match delivery {
            Some((id, attempts)) => Ok(Delivery::with_acknowledger(
                Envelope::new(
                    id,
                    "orders.created",
                    Vec::new(),
                    MessageMetadata::new(id, None),
                ),
                Box::new(TestAcknowledger {
                    acknowledgements: Arc::clone(&self.acknowledgements),
                }),
            )
            .with_attempts(attempts)),
            None => std::future::pending().await,
        }
    }
}

struct ConcurrentHandler {
    active: AtomicUsize,
    max_active: AtomicUsize,
    completed: AtomicUsize,
    cancel: CancellationToken,
    expected: usize,
}

impl ConcurrentHandler {
    fn record_maximum(&self, active: usize) {
        let mut observed = self.max_active.load(Ordering::Acquire);
        while active > observed {
            match self.max_active.compare_exchange_weak(
                observed,
                active,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }
}

#[async_trait]
impl DeliveryHandler for ConcurrentHandler {
    async fn handle(&self, _: &Envelope) -> CatgaResult<()> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.record_maximum(active);
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.active.fetch_sub(1, Ordering::AcqRel);
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if completed == self.expected {
            self.cancel.cancel();
        }
        Ok(())
    }
}

#[tokio::test]
async fn competing_consumer_bounds_concurrency_and_acknowledges_successful_deliveries() {
    let transport = Arc::new(QueueTransport::new([1, 2, 3]));
    let cancel = CancellationToken::new();
    let handler = Arc::new(ConcurrentHandler {
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        cancel: cancel.clone(),
        expected: 3,
    });
    let consumer = CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 2)
        .expect("positive concurrency is valid");

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .expect("the consumer must observe cancellation")
        .expect("the consumer completes after cancellation");

    assert_eq!(run.received(), 3);
    assert_eq!(run.acknowledged(), 3);
    assert_eq!(run.rejected(), 0);
    assert_eq!(handler.max_active.load(Ordering::Acquire), 2);
    assert_eq!(
        transport
            .acknowledgements
            .acknowledged
            .load(Ordering::Acquire),
        3
    );
}

struct RejectingHandler {
    cancel: CancellationToken,
}

/// Dead-letter store double that refuses persistence, modelling an unavailable durable store.
struct FailingDeadLetters;

#[async_trait]
impl DeadLetterStore for FailingDeadLetters {
    async fn enqueue(&self, _: DeadLetter) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Transient,
            "dead-letter store is unavailable",
        ))
    }

    async fn list(&self, _: usize) -> CatgaResult<Vec<DeadLetter>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl DeliveryHandler for RejectingHandler {
    async fn handle(&self, _: &Envelope) -> CatgaResult<()> {
        self.cancel.cancel();
        Err(CatgaError::new(
            ErrorCode::Validation,
            "order cannot be processed",
        ))
    }
}

#[tokio::test]
async fn competing_consumer_negatively_acknowledges_failed_handler_work() {
    let transport = Arc::new(QueueTransport::new([7]));
    let cancel = CancellationToken::new();
    let consumer = CompetingConsumer::new(
        Arc::clone(&transport),
        Arc::new(RejectingHandler {
            cancel: cancel.clone(),
        }),
        1,
    )
    .expect("positive concurrency is valid");

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .expect("the consumer must observe cancellation")
        .expect("a handler failure is redelivered instead of failing the worker");

    assert_eq!(run.received(), 1);
    assert_eq!(run.acknowledged(), 0);
    assert_eq!(run.rejected(), 1);
    assert_eq!(
        transport.acknowledgements.rejected.load(Ordering::Acquire),
        1
    );
}

#[tokio::test]
async fn competing_consumer_dead_letters_terminal_failures_before_acknowledging() -> CatgaResult<()>
{
    let transport = Arc::new(QueueTransport::with_attempts([(11, 3)]));
    let cancel = CancellationToken::new();
    let dead_letters = Arc::new(MemoryDeadLetters::new(2)?);
    let consumer = CompetingConsumer::new(
        Arc::clone(&transport),
        Arc::new(RejectingHandler {
            cancel: cancel.clone(),
        }),
        1,
    )?
    .with_dead_letters(3, Arc::clone(&dead_letters))?;

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .map_err(|_| {
            CatgaError::new(ErrorCode::Timeout, "consumer did not observe cancellation")
        })??;
    let letters = dead_letters.list(10).await?;

    assert_eq!(run.received(), 1);
    assert_eq!(run.acknowledged(), 1);
    assert_eq!(run.rejected(), 0);
    assert_eq!(run.dead_lettered(), 1);
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].envelope().id(), 11);
    assert_eq!(letters[0].attempts(), 3);
    assert_eq!(
        transport
            .acknowledgements
            .acknowledged
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        transport.acknowledgements.rejected.load(Ordering::Acquire),
        0
    );
    Ok(())
}

/// A terminal handler failure remains recoverable when durable dead-letter persistence fails.
#[tokio::test]
async fn competing_consumer_nacks_terminal_failure_when_dead_letter_persistence_fails()
-> CatgaResult<()> {
    let transport = Arc::new(QueueTransport::with_attempts([(12, 2)]));
    let cancel = CancellationToken::new();
    let consumer = CompetingConsumer::new(
        Arc::clone(&transport),
        Arc::new(RejectingHandler {
            cancel: cancel.clone(),
        }),
        1,
    )?
    .with_dead_letters(2, Arc::new(FailingDeadLetters))?;

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .map_err(|_| {
            CatgaError::new(ErrorCode::Timeout, "consumer did not observe cancellation")
        })??;

    assert_eq!(run.received(), 1);
    assert_eq!(run.acknowledged(), 0);
    assert_eq!(run.rejected(), 1);
    assert_eq!(run.dead_lettered(), 0);
    assert_eq!(
        transport
            .acknowledgements
            .acknowledged
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        transport.acknowledgements.rejected.load(Ordering::Acquire),
        1
    );
    Ok(())
}
