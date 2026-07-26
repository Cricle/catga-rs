//! Transport batch publication tests.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, EnvelopeHeaders, ErrorCode, MessageMetadata,
    MessageTransport, QualityOfService, TransportBatchOptions, TransportBatcher,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct ProbeTransport {
    fail_id: u64,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    completed: AtomicUsize,
}

impl ProbeTransport {
    fn new(fail_id: u64) -> Self {
        Self {
            fail_id,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl MessageTransport for ProbeTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let current = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(current, Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.completed.fetch_add(1, Ordering::AcqRel);
        if envelope.id() == self.fail_id {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "simulated transport failure",
            ));
        }
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "probe transport does not receive deliveries",
        ))
    }
}

fn envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[derive(Default)]
struct RecordingBatchTransport {
    batches: Mutex<Vec<(Vec<Envelope>, usize)>>,
}

impl RecordingBatchTransport {
    fn batches(&self) -> Vec<(Vec<Envelope>, usize)> {
        self.batches.lock().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |batches| batches.clone(),
        )
    }
}

#[async_trait]
impl MessageTransport for RecordingBatchTransport {
    async fn publish(&self, _envelope: Envelope) -> CatgaResult<()> {
        Ok(())
    }

    async fn publish_batch_with_concurrency(
        &self,
        envelopes: Vec<Envelope>,
        concurrency_limit: usize,
    ) -> CatgaResult<()> {
        let mut batches = self.batches.lock().map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "recording transport lock poisoned")
        })?;
        batches.push((envelopes, concurrency_limit));
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "recording transport does not receive deliveries",
        ))
    }
}

struct BlockingBatchTransport {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl BlockingBatchTransport {
    fn new() -> Self {
        Self {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }
    }
}

#[async_trait]
impl MessageTransport for BlockingBatchTransport {
    async fn publish(&self, _envelope: Envelope) -> CatgaResult<()> {
        Ok(())
    }

    async fn publish_batch_with_concurrency(
        &self,
        _envelopes: Vec<Envelope>,
        _concurrency_limit: usize,
    ) -> CatgaResult<()> {
        self.started.add_permits(1);
        let permit = self
            .release
            .acquire()
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Unavailable, "test transport stopped"))?;
        permit.forget();
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "blocking transport does not receive deliveries",
        ))
    }
}

#[tokio::test]
async fn transport_batcher_flushes_at_capacity_through_the_transport_batch_api() {
    let transport = Arc::new(RecordingBatchTransport::default());
    let transport_for_batcher: Arc<dyn MessageTransport> = transport.clone();
    let options = TransportBatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        max_queue_length: 4,
        publish_concurrency: 3,
    };
    let (batcher, runner) = TransportBatcher::new(transport_for_batcher, options).unwrap();
    let shutdown = CancellationToken::new();
    let runner_task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));
    let first_envelope = Envelope::new(
        1,
        "order.created",
        vec![1],
        MessageMetadata::new(1, Some(99)).with_quality_of_service(QualityOfService::ExactlyOnce),
    )
    .with_reply_to("orders.reply")
    .with_headers(EnvelopeHeaders::try_new([("tenant", "blue")]).unwrap());
    let second_envelope = envelope(2);

    let first = batcher.publish(first_envelope.clone());
    let second = batcher.publish(second_envelope.clone());
    let (first, second) = tokio::join!(first, second);

    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(
        transport.batches(),
        vec![(vec![first_envelope, second_envelope], 3)]
    );

    shutdown.cancel();
    assert!(runner_task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn transport_batcher_flushes_after_the_batch_timeout() {
    let transport = Arc::new(RecordingBatchTransport::default());
    let transport_for_batcher: Arc<dyn MessageTransport> = transport.clone();
    let options = TransportBatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        max_queue_length: 4,
        publish_concurrency: 1,
    };
    let (batcher, runner) = TransportBatcher::new(transport_for_batcher, options).unwrap();
    let shutdown = CancellationToken::new();
    let runner_task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));
    let publish_task = tokio::spawn(async move { batcher.publish(envelope(3)).await });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;

    assert!(publish_task.await.unwrap().is_ok());
    assert_eq!(transport.batches(), vec![(vec![envelope(3)], 1)]);

    shutdown.cancel();
    assert!(runner_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn transport_batcher_rejects_full_or_closed_runners_as_unavailable() {
    let transport: Arc<dyn MessageTransport> = Arc::new(RecordingBatchTransport::default());
    let options = TransportBatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        max_queue_length: 1,
        publish_concurrency: 1,
    };
    let (batcher, runner) = TransportBatcher::new(transport, options).unwrap();

    let first = tokio::spawn({
        let batcher = batcher.clone();
        async move { batcher.publish(envelope(4)).await }
    });
    tokio::task::yield_now().await;
    let full = batcher.publish(envelope(5)).await.unwrap_err();
    assert_eq!(full.code(), ErrorCode::Unavailable);

    drop(runner);
    let closed = batcher.publish(envelope(6)).await.unwrap_err();
    assert_eq!(closed.code(), ErrorCode::Unavailable);
    let first = first.await.unwrap();
    assert_eq!(first.unwrap_err().code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn transport_batcher_cancellation_rejects_unstarted_work_then_drains_started_work() {
    let transport = Arc::new(BlockingBatchTransport::new());
    let transport_for_batcher: Arc<dyn MessageTransport> = transport.clone();
    let options = TransportBatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        max_queue_length: 4,
        publish_concurrency: 1,
    };
    let (batcher, runner) = TransportBatcher::new(transport_for_batcher, options).unwrap();
    let shutdown = CancellationToken::new();
    let runner_task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));

    let first = tokio::spawn({
        let batcher = batcher.clone();
        async move { batcher.publish(envelope(7)).await }
    });
    let second = tokio::spawn({
        let batcher = batcher.clone();
        async move { batcher.publish(envelope(8)).await }
    });
    let started = transport.started.acquire().await.unwrap();
    started.forget();

    let queued = tokio::spawn({
        let batcher = batcher.clone();
        async move { batcher.publish(envelope(9)).await }
    });
    tokio::task::yield_now().await;
    shutdown.cancel();

    let queued = queued.await.unwrap();
    assert_eq!(queued.unwrap_err().code(), ErrorCode::Unavailable);

    transport.release.add_permits(1);
    assert!(first.await.unwrap().is_ok());
    assert!(second.await.unwrap().is_ok());
    assert!(runner_task.await.unwrap().is_ok());
}

#[test]
fn transport_batch_options_reject_zero_limits() {
    let transport: Arc<dyn MessageTransport> = Arc::new(RecordingBatchTransport::default());
    for options in [
        TransportBatchOptions {
            max_batch_size: 0,
            ..TransportBatchOptions::default()
        },
        TransportBatchOptions {
            batch_timeout: Duration::ZERO,
            ..TransportBatchOptions::default()
        },
        TransportBatchOptions {
            max_queue_length: 0,
            ..TransportBatchOptions::default()
        },
        TransportBatchOptions {
            publish_concurrency: 0,
            ..TransportBatchOptions::default()
        },
    ] {
        let error = match TransportBatcher::new(transport.clone(), options) {
            Ok(_) => panic!("zero transport batch option unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn batch_publish_bounds_concurrency_and_attempts_every_envelope_after_a_failure() {
    let transport = ProbeTransport::new(2);

    let error = transport
        .publish_batch_with_concurrency(vec![envelope(1), envelope(2), envelope(3), envelope(4)], 2)
        .await
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(transport.max_in_flight.load(Ordering::Acquire), 2);
    assert_eq!(transport.completed.load(Ordering::Acquire), 4);
}

#[tokio::test]
async fn batch_publish_rejects_a_zero_concurrency_limit() {
    let transport = ProbeTransport::new(0);

    let error = transport
        .publish_batch_with_concurrency(vec![envelope(1)], 0)
        .await
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "transport batch concurrency limit must be greater than zero"
    );
}
