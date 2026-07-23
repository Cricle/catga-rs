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
    CachedResultCodec, CatgaError, CatgaResult, Correlated, CorrelationBehavior, ErrorCode,
    Handler, IdempotencyBehavior, IdempotencyKey, InboxBehavior, InboxKey, Mediator,
    MessageMetadata, Pipeline, Registry, Request, RetryBehavior, TimeoutBehavior,
    current_correlation_id,
};
use catga_memory::{MemoryIdempotency, MemoryInbox};

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
