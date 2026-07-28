//! Competing transport consumer tests.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, CompetingConsumer, DeadLetter, DeadLetterStore,
    Delivery, DeliveryHandler, Envelope, EnvelopeHeaders, ErrorCode, MessageMetadata,
    MessageTransport, current_transport_context,
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

struct ContextCapturingHandler {
    cancel: CancellationToken,
    processing_span: Arc<StdMutex<Option<String>>>,
    processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
}

struct ContextTransport {
    ack_context_scoped: AtomicBool,
    ack_processing_span: Arc<StdMutex<Option<String>>>,
    ack_processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
    nack_context_scoped: AtomicBool,
    nack_processing_span: Arc<StdMutex<Option<String>>>,
    nack_processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
    delivered: AtomicBool,
}

impl ContextTransport {
    fn new(
        ack_processing_span: Arc<StdMutex<Option<String>>>,
        ack_processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
        nack_processing_span: Arc<StdMutex<Option<String>>>,
        nack_processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
    ) -> Self {
        Self {
            ack_context_scoped: AtomicBool::new(false),
            ack_processing_span,
            ack_processing_span_id,
            nack_context_scoped: AtomicBool::new(false),
            nack_processing_span,
            nack_processing_span_id,
            delivered: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl MessageTransport for ContextTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        if self.delivered.swap(true, Ordering::AcqRel) {
            std::future::pending().await
        }
        Ok(Delivery::new(
            Envelope::new(
                19,
                "orders.created",
                Vec::new(),
                MessageMetadata::new(19, None),
            )
            .with_headers(EnvelopeHeaders::try_new([("tenant", "blue")])?),
        ))
    }

    async fn ack(&self, _: Delivery) -> CatgaResult<()> {
        let scoped = current_transport_context().is_some_and(|context| {
            context.headers().and_then(|headers| headers.get("tenant")) == Some("blue")
        });
        self.ack_context_scoped.store(scoped, Ordering::Release);
        let span = tracing::Span::current();
        *self
            .ack_processing_span
            .lock()
            .expect("test observer lock is available") =
            span.metadata().map(|metadata| metadata.name().to_owned());
        *self
            .ack_processing_span_id
            .lock()
            .expect("test observer lock is available") = span.id();
        Ok(())
    }

    async fn nack(&self, _: Delivery) -> CatgaResult<()> {
        let scoped = current_transport_context().is_some_and(|context| {
            context.headers().and_then(|headers| headers.get("tenant")) == Some("blue")
        });
        self.nack_context_scoped.store(scoped, Ordering::Release);
        let span = tracing::Span::current();
        *self
            .nack_processing_span
            .lock()
            .expect("test observer lock is available") =
            span.metadata().map(|metadata| metadata.name().to_owned());
        *self
            .nack_processing_span_id
            .lock()
            .expect("test observer lock is available") = span.id();
        Ok(())
    }
}

#[async_trait]
impl DeliveryHandler for ContextCapturingHandler {
    async fn handle(&self, _: &Envelope) -> CatgaResult<()> {
        let context = current_transport_context().ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "transport context must be scoped")
        })?;
        if context.headers().and_then(|headers| headers.get("tenant")) != Some("blue") {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "transport headers must be scoped",
            ));
        }
        let span = tracing::Span::current();
        *self
            .processing_span
            .lock()
            .expect("test observer lock is available") =
            span.metadata().map(|metadata| metadata.name().to_owned());
        *self
            .processing_span_id
            .lock()
            .expect("test observer lock is available") = span.id();
        self.cancel.cancel();
        Ok(())
    }
}

struct RejectingContextHandler {
    cancel: CancellationToken,
    processing_span_id: Arc<StdMutex<Option<tracing::Id>>>,
}

#[async_trait]
impl DeliveryHandler for RejectingContextHandler {
    async fn handle(&self, _: &Envelope) -> CatgaResult<()> {
        let scoped = current_transport_context().is_some_and(|context| {
            context.headers().and_then(|headers| headers.get("tenant")) == Some("blue")
        });
        if !scoped {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "transport context must stay scoped for rejected work",
            ));
        }
        *self
            .processing_span_id
            .lock()
            .expect("test observer lock is available") = tracing::Span::current().id();
        self.cancel.cancel();
        Err(CatgaError::new(
            ErrorCode::Validation,
            "handler rejects delivery",
        ))
    }
}

#[tokio::test]
async fn competing_consumer_scopes_inbound_transport_context_while_handling_and_acknowledging()
-> CatgaResult<()> {
    let handler_processing_span = Arc::new(StdMutex::new(None));
    let handler_processing_span_id = Arc::new(StdMutex::new(None));
    let ack_processing_span = Arc::new(StdMutex::new(None));
    let ack_processing_span_id = Arc::new(StdMutex::new(None));
    let nack_processing_span = Arc::new(StdMutex::new(None));
    let nack_processing_span_id = Arc::new(StdMutex::new(None));
    let transport = Arc::new(ContextTransport::new(
        Arc::clone(&ack_processing_span),
        Arc::clone(&ack_processing_span_id),
        nack_processing_span,
        nack_processing_span_id,
    ));
    let cancel = CancellationToken::new();
    let consumer = CompetingConsumer::new(
        Arc::clone(&transport),
        Arc::new(ContextCapturingHandler {
            cancel: cancel.clone(),
            processing_span: Arc::clone(&handler_processing_span),
            processing_span_id: Arc::clone(&handler_processing_span_id),
        }),
        1,
    )?;

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "consumer did not stop"))??;

    assert_eq!(run.acknowledged(), 1);
    assert!(transport.ack_context_scoped.load(Ordering::Acquire));
    Ok(())
}

#[tokio::test]
async fn competing_consumer_scopes_inbound_transport_context_while_nacking_rejected_work()
-> CatgaResult<()> {
    let handler_processing_span_id = Arc::new(StdMutex::new(None));
    let ack_processing_span = Arc::new(StdMutex::new(None));
    let ack_processing_span_id = Arc::new(StdMutex::new(None));
    let nack_processing_span = Arc::new(StdMutex::new(None));
    let nack_processing_span_id = Arc::new(StdMutex::new(None));
    let transport = Arc::new(ContextTransport::new(
        ack_processing_span,
        ack_processing_span_id,
        Arc::clone(&nack_processing_span),
        Arc::clone(&nack_processing_span_id),
    ));
    let cancel = CancellationToken::new();
    let consumer = CompetingConsumer::new(
        Arc::clone(&transport),
        Arc::new(RejectingContextHandler {
            cancel: cancel.clone(),
            processing_span_id: Arc::clone(&handler_processing_span_id),
        }),
        1,
    )?;

    let run = tokio::time::timeout(Duration::from_secs(1), consumer.run_until_cancelled(cancel))
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "consumer did not stop"))??;

    assert_eq!(run.rejected(), 1);
    assert!(transport.nack_context_scoped.load(Ordering::Acquire));
    Ok(())
}
