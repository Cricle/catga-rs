//! Reliability behavior tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CachedResultCodec, CatgaError, CatgaResult, Correlated, CorrelationBehavior,
    DeadLetterBehavior, DeadLetterEnvelope, DeadLetterStore, DistributedLockBehavior,
    DistributedLockKey, Envelope, ErrorCode, Handler, IdempotencyBehavior, IdempotencyKey,
    InboxBehavior, InboxKey, Mediator, MessageMetadata, Pipeline, Registry, Request, RetryBehavior,
    TimeoutBehavior, current_correlation_id,
};
use catga_memory::{MemoryDeadLetters, MemoryIdempotency, MemoryInbox, MemoryLeases};
use tokio::sync::Notify;

#[derive(Clone, Debug)]
struct Work;

impl catga_core::Message for Work {}

impl Request for Work {
    type Response = &'static str;
}

struct FailsThenSucceeds(Arc<AtomicUsize>);

#[async_trait]
impl Handler<Work> for FailsThenSucceeds {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(ErrorCode::Transient, "try again"));
        }
        Ok("ok")
    }
}

struct TerminalFailure(Arc<AtomicUsize>);

#[async_trait]
impl Handler<Work> for TerminalFailure {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(CatgaError::new(ErrorCode::Validation, "bad request"))
    }
}

struct SlowHandler;

#[async_trait]
impl Handler<Work> for SlowHandler {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok("late")
    }
}

#[derive(Debug)]
struct CorrelatedWork(MessageMetadata);

impl catga_core::Message for CorrelatedWork {}

impl Request for CorrelatedWork {
    type Response = u64;
}

impl Correlated for CorrelatedWork {
    fn metadata(&self) -> MessageMetadata {
        self.0
    }
}

struct CorrelationHandler;

#[async_trait]
impl Handler<CorrelatedWork> for CorrelationHandler {
    async fn handle(&self, _: CorrelatedWork) -> CatgaResult<u64> {
        Ok(current_correlation_id().expect("correlation is scoped"))
    }
}

#[derive(Debug)]
struct IdempotentWork;

impl catga_core::Message for IdempotentWork {}

impl Request for IdempotentWork {
    type Response = u64;
}

impl IdempotencyKey for IdempotentWork {
    fn idempotency_key(&self) -> &str {
        "idempotent-work"
    }
}

struct CountingHandler(Arc<AtomicUsize>);

#[async_trait]
impl Handler<IdempotentWork> for CountingHandler {
    async fn handle(&self, _: IdempotentWork) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(21)
    }
}

struct U64Codec;

impl CachedResultCodec<u64> for U64Codec {
    fn encode(&self, value: &u64) -> CatgaResult<Arc<[u8]>> {
        Ok(Arc::from(value.to_le_bytes()))
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u64> {
        bytes
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "invalid cached u64"))
    }
}

#[derive(Debug)]
struct InboxWork;

impl catga_core::Message for InboxWork {}

impl Request for InboxWork {
    type Response = u64;
}

impl InboxKey for InboxWork {
    fn inbox_message_id(&self) -> u64 {
        404
    }
}

#[async_trait]
impl Handler<InboxWork> for CountingHandler {
    async fn handle(&self, _: InboxWork) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(21)
    }
}

#[derive(Clone, Debug)]
struct DeadWork;

impl catga_core::Message for DeadWork {}

impl Request for DeadWork {
    type Response = ();
}

impl DeadLetterEnvelope for DeadWork {
    fn dead_letter_envelope(&self) -> Envelope {
        Envelope::new(88, "dead.work", vec![8], MessageMetadata::new(88, None))
    }
}

struct DeadHandler;

#[async_trait]
impl Handler<DeadWork> for DeadHandler {
    async fn handle(&self, _: DeadWork) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Validation, "fatal"))
    }
}

fn pipeline() -> Pipeline<Work> {
    Pipeline::new().with(RetryBehavior::new(2, Duration::ZERO))
}

#[tokio::test]
async fn retry_behavior_replays_transient_errors_but_not_terminal_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(FailsThenSucceeds(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send_with(Work, &pipeline()).await.unwrap(), "ok");
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(TerminalFailure(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    assert_eq!(
        mediator
            .send_with(Work, &pipeline())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn timeout_behavior_cancels_an_overdue_handler() {
    let mut registry = Registry::new();
    registry.register_request::<Work, _>(SlowHandler).unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(TimeoutBehavior::new(Duration::from_millis(1)));

    assert_eq!(
        mediator
            .send_with(Work, &pipeline)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Timeout
    );
}

#[tokio::test]
async fn correlation_behavior_scopes_message_metadata_and_restores_the_parent_context() {
    let mut registry = Registry::new();
    registry
        .register_request::<CorrelatedWork, _>(CorrelationHandler)
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(CorrelationBehavior);

    assert_eq!(
        mediator
            .send_with(CorrelatedWork(MessageMetadata::new(17, Some(9))), &pipeline,)
            .await
            .unwrap(),
        9
    );
    assert_eq!(current_correlation_id(), None);
}

#[tokio::test]
async fn idempotency_behavior_returns_a_cached_result_without_reinvoking_the_handler() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<IdempotentWork, _>(CountingHandler(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryIdempotency::default());
    let pipeline = Pipeline::new().with(IdempotencyBehavior::new(store, U64Codec));

    assert_eq!(
        mediator.send_with(IdempotentWork, &pipeline).await.unwrap(),
        21
    );
    assert_eq!(
        mediator.send_with(IdempotentWork, &pipeline).await.unwrap(),
        21
    );
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[derive(Debug)]
struct LockedWork;

impl catga_core::Message for LockedWork {}

impl Request for LockedWork {
    type Response = u8;
}

impl DistributedLockKey for LockedWork {
    fn distributed_lock_key(&self) -> Box<str> {
        "inventory:7".into()
    }
}

struct BlockingLockHandler {
    executions: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl Handler<LockedWork> for BlockingLockHandler {
    async fn handle(&self, _: LockedWork) -> CatgaResult<u8> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        Ok(7)
    }
}

#[tokio::test]
async fn distributed_lock_behavior_excludes_concurrent_requests_and_releases_after_completion() {
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut registry = Registry::new();
    registry
        .register_request::<LockedWork, _>(BlockingLockHandler {
            executions: Arc::clone(&executions),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .unwrap();
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Arc::new(Pipeline::new().with(DistributedLockBehavior::new(
        Arc::new(MemoryLeases::default()),
        "node-a",
        Duration::from_secs(1),
    )));

    let first = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move { mediator.send_with(LockedWork, pipeline.as_ref()).await }
    });
    started.notified().await;

    assert_eq!(
        mediator
            .send_with(LockedWork, pipeline.as_ref())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), 7);
    let started_again = started.notified();
    let third = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move { mediator.send_with(LockedWork, pipeline.as_ref()).await }
    });
    started_again.await;
    release.notify_one();
    assert_eq!(third.await.unwrap().unwrap(), 7);
}

#[tokio::test]
async fn distributed_lock_behavior_waits_for_a_held_resource_when_configured() {
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut registry = Registry::new();
    registry
        .register_request::<LockedWork, _>(BlockingLockHandler {
            executions: Arc::clone(&executions),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .unwrap();
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Arc::new(
        Pipeline::new().with(
            DistributedLockBehavior::new(
                Arc::new(MemoryLeases::default()),
                "node-a",
                Duration::from_secs(1),
            )
            .with_wait_timeout(Duration::from_secs(1)),
        ),
    );

    let first = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move { mediator.send_with(LockedWork, pipeline.as_ref()).await }
    });
    started.notified().await;
    let started_again = started.notified();
    let second = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move { mediator.send_with(LockedWork, pipeline.as_ref()).await }
    });

    release.notify_one();
    assert_eq!(first.await.unwrap().unwrap(), 7);
    tokio::time::timeout(Duration::from_secs(1), started_again)
        .await
        .unwrap();
    release.notify_one();
    assert_eq!(second.await.unwrap().unwrap(), 7);
    assert_eq!(executions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn inbox_behavior_returns_a_cached_result_without_reinvoking_the_handler() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<InboxWork, _>(CountingHandler(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryInbox::default());
    let pipeline = Pipeline::new().with(InboxBehavior::new(store, U64Codec));

    assert_eq!(mediator.send_with(InboxWork, &pipeline).await.unwrap(), 21);
    assert_eq!(mediator.send_with(InboxWork, &pipeline).await.unwrap(), 21);
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dead_letter_behavior_records_terminal_failures_only() {
    let mut registry = Registry::new();
    registry
        .register_request::<DeadWork, _>(DeadHandler)
        .unwrap();
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryDeadLetters::new(1).unwrap());
    let pipeline = Pipeline::new().with(DeadLetterBehavior::new(Arc::clone(&store), 1));

    assert_eq!(
        mediator
            .send_with(DeadWork, &pipeline)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let letters = store.list(1).await.unwrap();
    assert_eq!(letters[0].envelope().id(), 88);
    assert_eq!(letters[0].reason(), "fatal");
}
