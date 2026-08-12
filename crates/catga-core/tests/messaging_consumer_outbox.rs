//! Strict contracts for the competing consumer and the outbox processor:
//! acknowledgement ownership, dead-letter promotion, bounded flushes, and
//! failure accounting.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CompetingConsumer, ConsumerRun, DeadLetter, DeadLetterStore, Delivery,
    DeliveryHandler, Envelope, ErrorCode, Message, MessageMetadata, MessageTransport,
    OutboxLoopOptions, OutboxMessage, OutboxProcessor, OutboxStore, PayloadDecoder,
    TypedDeliveryHandler, memory::MemoryOutbox,
};
use tokio_util::sync::CancellationToken;

fn env(id: u64) -> Envelope {
    Envelope::new(
        id,
        "ConsumerMsg",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

// ---------------------------------------------------------------------------
// Transport stub
// ---------------------------------------------------------------------------

/// Transport with scripted deliveries and selectable acknowledgement faults.
struct ScriptedTransport {
    deliveries: Mutex<VecDeque<CatgaResult<Delivery>>>,
    acks: AtomicUsize,
    nacks: AtomicUsize,
    published: Mutex<Vec<u64>>,
    fail_ack: AtomicBool,
    fail_nack: AtomicBool,
    fail_publish_ids: Mutex<Vec<u64>>,
}

impl ScriptedTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            deliveries: Mutex::new(VecDeque::new()),
            acks: AtomicUsize::new(0),
            nacks: AtomicUsize::new(0),
            published: Mutex::new(Vec::new()),
            fail_ack: AtomicBool::new(false),
            fail_nack: AtomicBool::new(false),
            fail_publish_ids: Mutex::new(Vec::new()),
        })
    }

    fn push(&self, delivery: Delivery) {
        self.deliveries
            .lock()
            .expect("delivery lock")
            .push_back(Ok(delivery));
    }

    fn push_error(&self, error: CatgaError) {
        self.deliveries
            .lock()
            .expect("delivery lock")
            .push_back(Err(error));
    }
}

#[async_trait]
impl MessageTransport for ScriptedTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let id = envelope.id();
        if self
            .fail_publish_ids
            .lock()
            .expect("publish fault lock")
            .contains(&id)
        {
            return Err(CatgaError::new(ErrorCode::Unavailable, "publish refused"));
        }
        self.published.lock().expect("publish lock").push(id);
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        let next = self.deliveries.lock().expect("delivery lock").pop_front();
        match next {
            Some(result) => result,
            // Exhausted: park until the caller cancels instead of spinning.
            None => std::future::pending().await,
        }
    }

    async fn ack(&self, delivery: Delivery) -> CatgaResult<()> {
        if self.fail_ack.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "ack refused"));
        }
        self.acks.fetch_add(1, Ordering::SeqCst);
        drop(delivery);
        Ok(())
    }

    async fn nack(&self, delivery: Delivery) -> CatgaResult<()> {
        if self.fail_nack.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "nack refused"));
        }
        self.nacks.fetch_add(1, Ordering::SeqCst);
        drop(delivery);
        Ok(())
    }
}

/// Handler that fails every envelope whose id appears in its failure list.
struct SelectiveHandler {
    failures: Vec<u64>,
}

#[async_trait]
impl DeliveryHandler for SelectiveHandler {
    async fn handle(&self, envelope: &Envelope) -> CatgaResult<()> {
        if self.failures.contains(&envelope.id()) {
            return Err(CatgaError::new(ErrorCode::Transient, "handler refused"));
        }
        Ok(())
    }
}

/// Dead-letter store recording envelope ids, refusing the configured ids.
struct RecordingDeadLetters {
    letters: Mutex<Vec<u64>>,
    refuse_ids: Vec<u64>,
}

#[async_trait]
impl DeadLetterStore for RecordingDeadLetters {
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()> {
        if self.refuse_ids.contains(&letter.envelope().id()) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "dead letters full"));
        }
        self.letters
            .lock()
            .expect("dead letter lock")
            .push(letter.envelope().id());
        Ok(())
    }

    async fn list(&self, _: usize) -> CatgaResult<Vec<DeadLetter>> {
        Ok(Vec::new())
    }
}

/// Waits until `observed` reaches `target` or fails the test.
async fn wait_for_count(counter: &AtomicUsize, target: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while counter.load(Ordering::SeqCst) < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "consumer never processed {target} deliveries"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ---------------------------------------------------------------------------
// CompetingConsumer contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn consumer_construction_validates_bounds() {
    let transport = ScriptedTransport::new();
    let handler = Arc::new(SelectiveHandler {
        failures: Vec::new(),
    });
    match CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 0) {
        Ok(_) => panic!("zero concurrency must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }

    let consumer =
        CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 2).expect("builds");
    assert_eq!(consumer.concurrency().get(), 2);

    let letters = Arc::new(RecordingDeadLetters {
        letters: Mutex::new(Vec::new()),
        refuse_ids: Vec::new(),
    });
    match consumer.with_dead_letters(0, Arc::clone(&letters)) {
        Ok(_) => panic!("zero attempt ceiling must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }

    let consumer =
        CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 2).expect("builds");
    let guarded = consumer
        .with_dead_letters(3, letters)
        .expect("dead letters attach");
    assert_eq!(guarded.concurrency().get(), 2);
}

#[tokio::test]
async fn consumer_counts_acknowledged_rejected_and_dead_lettered_work() {
    let transport = ScriptedTransport::new();
    let handler = Arc::new(SelectiveHandler {
        failures: vec![2, 3, 4],
    });
    let letters = Arc::new(RecordingDeadLetters {
        letters: Mutex::new(Vec::new()),
        refuse_ids: vec![4],
    });
    let consumer = CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 2)
        .expect("builds")
        .with_dead_letters(3, Arc::clone(&letters))
        .expect("dead letters attach");

    // id 1 succeeds; id 2 fails below the attempt ceiling and is redelivered;
    // id 3 fails at the ceiling and is dead-lettered; id 4 fails at the
    // ceiling but the store refuses, forcing redelivery.
    transport.push(Delivery::new(env(1)));
    transport.push(Delivery::new(env(2)).with_attempts(1));
    transport.push(Delivery::new(env(3)).with_attempts(5));
    transport.push(Delivery::new(env(4)).with_attempts(3));

    let token = CancellationToken::new();
    let acks = Arc::clone(&transport);
    let task = tokio::spawn({
        let token = token.clone();
        async move { consumer.run_until_cancelled(token).await }
    });
    wait_for_count(&acks.acks, 2).await;
    wait_for_count(&acks.nacks, 2).await;
    token.cancel();
    let run = task
        .await
        .expect("consumer exits")
        .expect("the run completes");

    assert_eq!(run.received(), 4);
    assert_eq!(run.acknowledged(), 2, "success and dead-letter acks");
    assert_eq!(run.rejected(), 2);
    assert_eq!(run.dead_lettered(), 1);
    assert_eq!(*letters.letters.lock().expect("dead letter lock"), vec![3]);

    // A pre-cancelled token yields an empty run without receiving.
    let consumer = CompetingConsumer::new(Arc::clone(&transport), handler, 2).expect("builds");
    let stopped = CancellationToken::new();
    stopped.cancel();
    let run = consumer
        .run_until_cancelled(stopped)
        .await
        .expect("cancelled run completes");
    assert_eq!(run, ConsumerRun::default());
}

#[tokio::test]
async fn consumer_surfaces_receive_ack_and_nack_failures() {
    // Receive errors end the run with the store error.
    let transport = ScriptedTransport::new();
    transport.push(Delivery::new(env(1)));
    transport.push_error(CatgaError::new(ErrorCode::Unavailable, "receive broke"));
    let handler = Arc::new(SelectiveHandler {
        failures: Vec::new(),
    });
    let consumer =
        CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 1).expect("builds");
    let error = consumer
        .run_until_cancelled(CancellationToken::new())
        .await
        .expect_err("the receive failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(error.message().contains("receive broke"));
    assert_eq!(transport.acks.load(Ordering::SeqCst), 1);

    // Ack failures end the run after the handler succeeded.
    let transport = ScriptedTransport::new();
    transport.push(Delivery::new(env(1)));
    transport.fail_ack.store(true, Ordering::SeqCst);
    let consumer =
        CompetingConsumer::new(Arc::clone(&transport), Arc::clone(&handler), 1).expect("builds");
    let error = consumer
        .run_until_cancelled(CancellationToken::new())
        .await
        .expect_err("the ack failure surfaces");
    assert!(error.message().contains("ack refused"));

    // Nack failures end the run after the handler failed.
    let failing = Arc::new(SelectiveHandler { failures: vec![1] });
    let transport = ScriptedTransport::new();
    transport.push(Delivery::new(env(1)));
    transport.fail_nack.store(true, Ordering::SeqCst);
    let consumer = CompetingConsumer::new(Arc::clone(&transport), failing, 1).expect("builds");
    let error = consumer
        .run_until_cancelled(CancellationToken::new())
        .await
        .expect_err("the nack failure surfaces");
    assert!(error.message().contains("nack refused"));
}

// ---------------------------------------------------------------------------
// Typed consumer contracts
// ---------------------------------------------------------------------------

/// Decoded application message carried by typed deliveries.
#[derive(Clone)]
struct Ping(u64);
impl Message for Ping {}

/// Little-endian u64 payload decoder.
struct PingDecoder;

impl PayloadDecoder<Ping> for PingDecoder {
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<Ping> {
        let sized: [u8; 8] = bytes
            .try_into()
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "payload is corrupt"))?;
        Ok(Ping(u64::from_le_bytes(sized)))
    }
}

struct PingHandler {
    seen: Mutex<Vec<u64>>,
}

#[async_trait]
impl TypedDeliveryHandler<Ping> for PingHandler {
    async fn handle(&self, message: &Ping) -> CatgaResult<()> {
        if message.0 == 13 {
            return Err(CatgaError::new(ErrorCode::Validation, "unlucky ping"));
        }
        self.seen.lock().expect("ping lock").push(message.0);
        Ok(())
    }
}

#[tokio::test]
async fn typed_consumer_decodes_before_handling() {
    let transport = ScriptedTransport::new();
    // A valid payload, an application failure, and a corrupt payload.
    transport.push(Delivery::new(Envelope::new(
        1,
        "Ping",
        7_u64.to_le_bytes().to_vec(),
        MessageMetadata::new(1, None),
    )));
    transport.push(Delivery::new(Envelope::new(
        2,
        "Ping",
        13_u64.to_le_bytes().to_vec(),
        MessageMetadata::new(2, None),
    )));
    transport.push(Delivery::new(Envelope::new(
        3,
        "Ping",
        vec![1, 2, 3],
        MessageMetadata::new(3, None),
    )));

    let consumer = CompetingConsumer::typed(
        transport.clone(),
        Arc::new(PingHandler {
            seen: Mutex::new(Vec::new()),
        }),
        Arc::new(PingDecoder),
        1,
    )
    .expect("typed consumer builds");

    let token = CancellationToken::new();
    let task = tokio::spawn({
        let token = token.clone();
        async move { consumer.run_until_cancelled(token).await }
    });
    wait_for_count(&transport.nacks, 2).await;
    wait_for_count(&transport.acks, 1).await;
    token.cancel();
    let run = task
        .await
        .expect("consumer exits")
        .expect("the run completes");
    assert_eq!(run.received(), 3);
    assert_eq!(run.acknowledged(), 1);
    assert_eq!(run.rejected(), 2);
}

// ---------------------------------------------------------------------------
// OutboxProcessor contracts
// ---------------------------------------------------------------------------

fn outbox_env(id: u64) -> OutboxMessage {
    OutboxMessage::new(env(id))
}

#[tokio::test]
async fn outbox_processor_construction_validates_bounds() {
    let store = Arc::new(MemoryOutbox::new(4).expect("capacity builds"));
    let transport = ScriptedTransport::new();
    match OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker", 0) {
        Ok(_) => panic!("zero batch size must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }

    match OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker", 1025) {
        Ok(_) => panic!("an oversized batch must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }

    match OutboxProcessor::new_with_concurrency(
        Arc::clone(&store),
        Arc::clone(&transport),
        "worker",
        4,
        0,
    ) {
        Ok(_) => panic!("zero concurrency must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }

    let processor =
        OutboxProcessor::new(store, transport, "worker", 4).expect("valid bounds build");
    let options = OutboxLoopOptions::default();
    assert_eq!(options.scan_interval(), Duration::from_secs(1));
    assert_eq!(options.error_delay(), Duration::from_secs(1));
    let error = OutboxLoopOptions::new(Duration::ZERO, Duration::from_secs(1))
        .expect_err("zero scan interval fails");
    assert_eq!(error.code(), ErrorCode::Validation);
    let error = OutboxLoopOptions::new(Duration::from_secs(1), Duration::ZERO)
        .expect_err("zero error delay fails");
    assert_eq!(error.code(), ErrorCode::Validation);
    let options = OutboxLoopOptions::new(Duration::from_millis(5), Duration::from_millis(7))
        .expect("valid intervals build");
    assert_eq!(options.scan_interval(), Duration::from_millis(5));
    assert_eq!(options.error_delay(), Duration::from_millis(7));
    drop(processor);
}

#[tokio::test]
async fn outbox_flush_publishes_acks_and_counts_failures() {
    let store = Arc::new(MemoryOutbox::new(8).expect("capacity builds"));
    for id in 1..=3_u64 {
        store
            .enqueue(outbox_env(id))
            .await
            .expect("enqueue succeeds");
    }
    let transport = ScriptedTransport::new();
    transport
        .fail_publish_ids
        .lock()
        .expect("publish fault lock")
        .push(2);
    let processor = OutboxProcessor::new_with_concurrency(
        Arc::clone(&store),
        Arc::clone(&transport),
        "worker",
        8,
        2,
    )
    .expect("builds");

    let run = processor.flush_once().await.expect("flush completes");
    assert_eq!(run.published(), 2);
    assert_eq!(run.failed(), 1);
    let mut published = transport.published.lock().expect("publish lock").clone();
    published.sort_unstable();
    assert_eq!(published, vec![1, 3]);

    // The failed delivery returns to pending with a bounded reason.
    let reclaimed = store.claim("worker", 8).await.expect("claim succeeds");
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].id(), 2);
    let reason = reclaimed[0].last_error().expect("the failure is recorded");
    assert!(reason.starts_with("outbox publication failed: "));
    assert!(reason.contains("publish refused"));
}

/// Outbox store that can fail acknowledgement, failure recording, or claims.
struct FaultyOutbox {
    inner: MemoryOutbox,
    ack_fails: AtomicBool,
    record_fails: AtomicBool,
    claim_fails: AtomicBool,
}

#[async_trait]
impl OutboxStore for FaultyOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        self.inner.enqueue(message).await
    }

    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        if self.claim_fails.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "claim store down"));
        }
        self.inner.claim(owner, limit).await
    }

    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        if self.ack_fails.load(Ordering::SeqCst) {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "ack persistence down",
            ));
        }
        self.inner.ack(owner, id, claim_token).await
    }

    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        self.inner.release(owner, id, claim_token).await
    }

    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()> {
        if self.record_fails.load(Ordering::SeqCst) {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "record persistence down",
            ));
        }
        self.inner
            .record_failure(owner, id, claim_token, reason)
            .await
    }

    async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        self.inner.cancel(id).await
    }
}

#[tokio::test]
async fn outbox_flush_records_ack_failures_and_propagates_store_errors() {
    // A failed ack records the failure and keeps the loop going.
    let store = Arc::new(FaultyOutbox {
        inner: MemoryOutbox::new(4).expect("capacity builds"),
        ack_fails: AtomicBool::new(true),
        record_fails: AtomicBool::new(false),
        claim_fails: AtomicBool::new(false),
    });
    store
        .enqueue(outbox_env(1))
        .await
        .expect("enqueue succeeds");
    let transport = ScriptedTransport::new();
    let processor = OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker", 4)
        .expect("builds");
    let run = processor.flush_once().await.expect("flush completes");
    assert_eq!(run.published(), 0);
    assert_eq!(run.failed(), 1);
    assert_eq!(
        transport.published.lock().expect("publish lock").clone(),
        vec![1],
        "the envelope was published before the ack failed"
    );

    // A record_failure error propagates out of the flush.
    store.record_fails.store(true, Ordering::SeqCst);
    let error = processor
        .flush_once()
        .await
        .expect_err("the record failure surfaces");
    assert!(error.message().contains("record persistence down"));

    // A claim error propagates out of the flush.
    store.claim_fails.store(true, Ordering::SeqCst);
    let error = processor
        .flush_once()
        .await
        .expect_err("the claim failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

/// Outbox store whose claims omit claim tokens, violating the contract.
struct TokenlessOutbox;

#[async_trait]
impl OutboxStore for TokenlessOutbox {
    async fn enqueue(&self, _: OutboxMessage) -> CatgaResult<()> {
        Ok(())
    }

    async fn claim(&self, _: &str, _: usize) -> CatgaResult<Vec<OutboxMessage>> {
        Ok(vec![outbox_env(1)])
    }

    async fn ack(&self, _: &str, _: u64, _: &str) -> CatgaResult<()> {
        Ok(())
    }

    async fn release(&self, _: &str, _: u64, _: &str) -> CatgaResult<()> {
        Ok(())
    }

    async fn record_failure(&self, _: &str, _: u64, _: &str, _: &str) -> CatgaResult<()> {
        Ok(())
    }

    async fn cancel(&self, _: u64) -> CatgaResult<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn outbox_flush_rejects_tokenless_claims() {
    let transport = ScriptedTransport::new();
    let processor =
        OutboxProcessor::new(Arc::new(TokenlessOutbox), transport, "worker", 4).expect("builds");
    let error = processor
        .flush_once()
        .await
        .expect_err("a tokenless claim fails");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("claim token"));
}

#[tokio::test]
async fn outbox_run_until_cancelled_drains_and_counts_loop_failures() {
    // The success loop drains the store and stops on cancellation.
    let store = Arc::new(MemoryOutbox::new(8).expect("capacity builds"));
    for id in 1..=3_u64 {
        store
            .enqueue(outbox_env(id))
            .await
            .expect("enqueue succeeds");
    }
    let transport = ScriptedTransport::new();
    let processor = OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker", 8)
        .expect("builds");
    let options =
        OutboxLoopOptions::new(Duration::from_millis(5), Duration::from_millis(5)).expect("opts");
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let token = token.clone();
        async move { processor.run_until_cancelled(options, token).await }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while transport.published.lock().expect("publish lock").len() < 3 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the loop never drained the outbox"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    token.cancel();
    let total = task.await.expect("loop exits").expect("the loop completes");
    assert_eq!(total.published(), 3);
    assert_eq!(total.failed(), 0);

    // Store failures are counted and retried after the error delay.
    let broken = Arc::new(FaultyOutbox {
        inner: MemoryOutbox::new(1).expect("capacity builds"),
        ack_fails: AtomicBool::new(false),
        record_fails: AtomicBool::new(false),
        claim_fails: AtomicBool::new(true),
    });
    let processor =
        OutboxProcessor::new(broken, ScriptedTransport::new(), "worker", 1).expect("builds");
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let token = token.clone();
        async move { processor.run_until_cancelled(options, token).await }
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    token.cancel();
    let total = task.await.expect("loop exits").expect("the loop completes");
    assert_eq!(total.published(), 0);
    assert!(
        total.failed() >= 2,
        "each failed scan is counted, got {}",
        total.failed()
    );
}
