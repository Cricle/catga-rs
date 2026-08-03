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
    CachedResultCodec, CatgaError, CatgaResult, Command, CommandHandler, CommandPipeline,
    Correlated, CorrelationBehavior, DeadLetter, DeadLetterBehavior, DeadLetterEnvelope,
    DeadLetterStore, DistributedLockBehavior, DistributedLockKey, Envelope, ErrorCode, Handler,
    IdempotencyBehavior, IdempotencyKey, IdempotencyStore, InboxBehavior, InboxKey, InboxStore,
    LeaseStore, Mediator, MessageMetadata, Pipeline, Registry, Request, RetryBehavior,
    TimeoutBehavior, current_correlation_id,
};
use catga_memory::{MemoryDeadLetters, MemoryIdempotency, MemoryInbox, MemoryLeases};
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Debug)]
struct Work;

impl catga_core::Message for Work {}

impl Request for Work {
    type Response = &'static str;
    type TypeId = catga_core::DefaultMessageTypeId;
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

struct FailsOnceWith(Arc<AtomicUsize>, CatgaError);

#[async_trait]
impl Handler<Work> for FailsOnceWith {
    async fn handle(&self, _: Work) -> CatgaResult<&'static str> {
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(self.1.clone());
        }
        Ok("ok")
    }
}

fn error_with_retryability(code: ErrorCode, retryable: bool) -> CatgaError {
    serde_json::from_value(serde_json::json!({
        "code": code,
        "message": "configured retryability",
        "retryable": retryable,
    }))
    .expect("a CatgaError wire override is valid")
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
    type TypeId = catga_core::DefaultMessageTypeId;
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
    type TypeId = catga_core::DefaultMessageTypeId;
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

struct PanickingIdempotentHandler;

#[async_trait]
impl Handler<IdempotentWork> for PanickingIdempotentHandler {
    async fn handle(&self, _: IdempotentWork) -> CatgaResult<u64> {
        panic!("idempotent handler panic");
    }
}

struct FailingIdempotentHandler;

#[async_trait]
impl Handler<IdempotentWork> for FailingIdempotentHandler {
    async fn handle(&self, _: IdempotentWork) -> CatgaResult<u64> {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "idempotent handler failed",
        ))
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

struct EncodingFailureCodec;

impl CachedResultCodec<u64> for EncodingFailureCodec {
    fn encode(&self, _: &u64) -> CatgaResult<Arc<[u8]>> {
        Err(CatgaError::new(
            ErrorCode::Internal,
            "cache encoding failed",
        ))
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u64> {
        U64Codec.decode(bytes)
    }
}

#[derive(Debug)]
struct InboxWork;

impl catga_core::Message for InboxWork {}

impl Request for InboxWork {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
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

struct PanickingInboxHandler;

#[async_trait]
impl Handler<InboxWork> for PanickingInboxHandler {
    async fn handle(&self, _: InboxWork) -> CatgaResult<u64> {
        panic!("inbox handler panic");
    }
}

struct FailingInboxHandler;

#[async_trait]
impl Handler<InboxWork> for FailingInboxHandler {
    async fn handle(&self, _: InboxWork) -> CatgaResult<u64> {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "inbox handler failed",
        ))
    }
}

struct FailingIdempotencyCleanup {
    fail_calls: AtomicUsize,
}

#[async_trait]
impl IdempotencyStore for FailingIdempotencyCleanup {
    async fn try_claim(&self, _: &str) -> CatgaResult<bool> {
        Ok(true)
    }

    async fn complete(&self, _: &str, _: Option<Arc<[u8]>>) -> CatgaResult<()> {
        Ok(())
    }

    async fn fail(&self, _: &str) -> CatgaResult<()> {
        self.fail_calls.fetch_add(1, Ordering::Relaxed);
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "idempotency cleanup failed",
        ))
    }

    async fn state(&self, _: &str) -> CatgaResult<Option<catga_core::ProcessingState>> {
        Ok(None)
    }

    async fn result(&self, _: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        Ok(None)
    }
}

struct FailingInboxCleanup {
    fail_calls: AtomicUsize,
    fail_complete: bool,
}

#[async_trait]
impl InboxStore for FailingInboxCleanup {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<Option<catga_core::InboxClaim>> {
        Ok(catga_core::InboxClaim::new(message_id, 1))
    }

    async fn complete(&self, _: catga_core::InboxClaim, _: Option<Arc<[u8]>>) -> CatgaResult<()> {
        if self.fail_complete {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "inbox completion failed",
            ));
        }
        Ok(())
    }

    async fn fail(&self, _: catga_core::InboxClaim) -> CatgaResult<()> {
        self.fail_calls.fetch_add(1, Ordering::Relaxed);
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "inbox cleanup failed",
        ))
    }

    async fn state(&self, _: u64) -> CatgaResult<Option<catga_core::ProcessingState>> {
        Ok(None)
    }

    async fn result(&self, _: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        Ok(None)
    }
}

#[derive(Debug)]
struct UnidentifiedInboxWork;

impl catga_core::Message for UnidentifiedInboxWork {}

impl Request for UnidentifiedInboxWork {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl InboxKey for UnidentifiedInboxWork {
    fn inbox_message_id(&self) -> u64 {
        0
    }
}

#[async_trait]
impl Handler<UnidentifiedInboxWork> for CountingHandler {
    async fn handle(&self, _: UnidentifiedInboxWork) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(21)
    }
}

#[derive(Clone, Debug)]
struct DeadWork;

impl catga_core::Message for DeadWork {}

impl Request for DeadWork {
    type Response = ();
    type TypeId = catga_core::DefaultMessageTypeId;
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

struct PanickingDeadHandler;

#[async_trait]
impl Handler<DeadWork> for PanickingDeadHandler {
    async fn handle(&self, _: DeadWork) -> CatgaResult<()> {
        panic!("dead-letter handler panic");
    }
}

struct DeadCommand;

impl catga_core::Message for DeadCommand {}
impl Command for DeadCommand {
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl DeadLetterEnvelope for DeadCommand {
    fn dead_letter_envelope(&self) -> Envelope {
        Envelope::new(89, "dead.command", vec![9], MessageMetadata::new(89, None))
    }
}

struct DeadCommandHandler;

#[async_trait]
impl CommandHandler<DeadCommand> for DeadCommandHandler {
    async fn handle(&self, _: DeadCommand) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Validation, "command fatal"))
    }
}

struct FailingDeadLetters;

#[async_trait]
impl DeadLetterStore for FailingDeadLetters {
    async fn enqueue(&self, _: DeadLetter) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "dead letter store is unavailable",
        ))
    }

    async fn list(&self, _: usize) -> CatgaResult<Vec<DeadLetter>> {
        Ok(Vec::new())
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
async fn retry_behavior_retries_unavailable_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(FailsOnceWith(
            Arc::clone(&attempts),
            CatgaError::new(ErrorCode::Unavailable, "temporarily unavailable"),
        ))
        .expect("handler is accepted");
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send_with(Work, &pipeline()).await, Ok("ok"));
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn retry_behavior_honors_retryability_overrides() {
    let retryable_attempts = Arc::new(AtomicUsize::new(0));
    let mut retryable_registry = Registry::new();
    retryable_registry
        .register_request::<Work, _>(FailsOnceWith(
            Arc::clone(&retryable_attempts),
            error_with_retryability(ErrorCode::Validation, true),
        ))
        .expect("handler is accepted");
    let retryable_mediator = Mediator::new(retryable_registry);

    assert_eq!(
        retryable_mediator.send_with(Work, &pipeline()).await,
        Ok("ok")
    );
    assert_eq!(retryable_attempts.load(Ordering::Relaxed), 2);

    let non_retryable_attempts = Arc::new(AtomicUsize::new(0));
    let mut non_retryable_registry = Registry::new();
    non_retryable_registry
        .register_request::<Work, _>(FailsOnceWith(
            Arc::clone(&non_retryable_attempts),
            error_with_retryability(ErrorCode::Transient, false),
        ))
        .expect("handler is accepted");
    let non_retryable_mediator = Mediator::new(non_retryable_registry);

    assert_eq!(
        non_retryable_mediator
            .send_with(Work, &pipeline())
            .await
            .expect_err("an explicit non-retryable override is returned")
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(non_retryable_attempts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn retry_behavior_never_retries_cancelled_errors() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(FailsOnceWith(
            Arc::clone(&attempts),
            error_with_retryability(ErrorCode::Cancelled, true),
        ))
        .expect("handler is accepted");
    let mediator = Mediator::new(registry);

    assert_eq!(
        mediator
            .send_with(Work, &pipeline())
            .await
            .expect_err("cancelled work is returned")
            .code(),
        ErrorCode::Cancelled
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
async fn idempotency_behavior_releases_claims_after_panics_and_encoding_failures() {
    let panic_store = Arc::new(MemoryIdempotency::default());
    let mut panic_registry = Registry::new();
    panic_registry
        .register_request::<IdempotentWork, _>(PanickingIdempotentHandler)
        .expect("panicking handler registers");
    let panic_mediator = Mediator::new(panic_registry);
    let panic_pipeline =
        Pipeline::new().with(IdempotencyBehavior::new(panic_store.clone(), U64Codec));

    assert!(matches!(
        panic_mediator.send_with(IdempotentWork, &panic_pipeline).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    assert_eq!(
        panic_store.state("idempotent-work").await.unwrap(),
        Some(catga_core::ProcessingState::Failed)
    );

    let encoding_store = Arc::new(MemoryIdempotency::default());
    let mut encoding_registry = Registry::new();
    encoding_registry
        .register_request::<IdempotentWork, _>(CountingHandler(Arc::new(AtomicUsize::new(0))))
        .expect("counting handler registers");
    let encoding_mediator = Mediator::new(encoding_registry);
    let encoding_pipeline = Pipeline::new().with(IdempotencyBehavior::new(
        encoding_store.clone(),
        EncodingFailureCodec,
    ));

    assert!(matches!(
        encoding_mediator
            .send_with(IdempotentWork, &encoding_pipeline)
            .await,
        Err(error) if error.message() == "cache encoding failed"
    ));
    assert_eq!(
        encoding_store.state("idempotent-work").await.unwrap(),
        Some(catga_core::ProcessingState::Failed)
    );
}

#[tokio::test]
async fn idempotency_behavior_preserves_handler_errors_when_cleanup_fails() {
    let store = Arc::new(FailingIdempotencyCleanup {
        fail_calls: AtomicUsize::new(0),
    });
    let mut registry = Registry::new();
    registry
        .register_request::<IdempotentWork, _>(FailingIdempotentHandler)
        .expect("failing handler registers");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(IdempotencyBehavior::new(store.clone(), U64Codec));

    assert!(matches!(
        mediator.send_with(IdempotentWork, &pipeline).await,
        Err(error) if error.message() == "idempotent handler failed"
    ));
    assert_eq!(store.fail_calls.load(Ordering::Relaxed), 1);
}

#[derive(Debug)]
struct LockedWork;

impl catga_core::Message for LockedWork {}

impl Request for LockedWork {
    type Response = u8;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl DistributedLockKey for LockedWork {
    fn distributed_lock_key(&self) -> Box<str> {
        "inventory:7".into()
    }
}

struct VirtualLeaseState {
    owner: Option<Box<str>>,
    expires_at: tokio::time::Instant,
}

struct VirtualLeases {
    state: Mutex<VirtualLeaseState>,
    renewals: AtomicUsize,
}

impl VirtualLeases {
    fn new() -> Self {
        Self {
            state: Mutex::new(VirtualLeaseState {
                owner: None,
                expires_at: tokio::time::Instant::now(),
            }),
            renewals: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LeaseStore for VirtualLeases {
    async fn try_acquire(&self, _: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        let mut state = self.state.lock().await;
        if state.owner.is_none() || state.expires_at <= tokio::time::Instant::now() {
            state.owner = Some(owner.into());
            state.expires_at = tokio::time::Instant::now() + ttl;
            return Ok(true);
        }
        Ok(false)
    }

    async fn renew(&self, _: &str, owner: &str, ttl: Duration) -> CatgaResult<bool> {
        let mut state = self.state.lock().await;
        if state.owner.as_deref() != Some(owner) || state.expires_at <= tokio::time::Instant::now()
        {
            return Ok(false);
        }
        state.expires_at = tokio::time::Instant::now() + ttl;
        self.renewals.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }

    async fn release(&self, _: &str, owner: &str) -> CatgaResult<bool> {
        let mut state = self.state.lock().await;
        if state.owner.as_deref() != Some(owner) {
            return Ok(false);
        }
        state.owner = None;
        Ok(true)
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

#[tokio::test(start_paused = true)]
async fn distributed_lock_behavior_renews_while_a_handler_exceeds_its_initial_lease() {
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let leases = Arc::new(VirtualLeases::new());
    let mut registry = Registry::new();
    registry
        .register_request::<LockedWork, _>(BlockingLockHandler {
            executions: Arc::clone(&executions),
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        })
        .expect("blocking lock handler registers");
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Arc::new(Pipeline::new().with(DistributedLockBehavior::new(
        Arc::clone(&leases) as Arc<dyn LeaseStore>,
        "node-a",
        Duration::from_millis(10),
    )));

    let first = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move { mediator.send_with(LockedWork, pipeline.as_ref()).await }
    });
    started.notified().await;

    tokio::time::advance(Duration::from_millis(6)).await;
    tokio::task::yield_now().await;
    assert_eq!(leases.renewals.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(4)).await;

    assert_eq!(
        mediator
            .send_with(LockedWork, pipeline.as_ref())
            .await
            .expect_err("renewed lease excludes a later request")
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    release.notify_one();
    assert_eq!(
        first
            .await
            .expect("first task does not panic")
            .expect("first lock holder completes"),
        7
    );
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
async fn inbox_behavior_releases_claims_after_panics_and_encoding_failures() {
    let panic_store = Arc::new(MemoryInbox::default());
    let mut panic_registry = Registry::new();
    panic_registry
        .register_request::<InboxWork, _>(PanickingInboxHandler)
        .expect("panicking handler registers");
    let panic_mediator = Mediator::new(panic_registry);
    let panic_pipeline = Pipeline::new().with(InboxBehavior::new(panic_store.clone(), U64Codec));

    assert!(matches!(
        panic_mediator.send_with(InboxWork, &panic_pipeline).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    assert_eq!(
        panic_store.state(404).await.unwrap(),
        Some(catga_core::ProcessingState::Failed)
    );

    let encoding_store = Arc::new(MemoryInbox::default());
    let mut encoding_registry = Registry::new();
    encoding_registry
        .register_request::<InboxWork, _>(CountingHandler(Arc::new(AtomicUsize::new(0))))
        .expect("counting handler registers");
    let encoding_mediator = Mediator::new(encoding_registry);
    let encoding_pipeline = Pipeline::new().with(InboxBehavior::new(
        encoding_store.clone(),
        EncodingFailureCodec,
    ));

    assert!(matches!(
        encoding_mediator.send_with(InboxWork, &encoding_pipeline).await,
        Err(error) if error.message() == "cache encoding failed"
    ));
    assert_eq!(
        encoding_store.state(404).await.unwrap(),
        Some(catga_core::ProcessingState::Failed)
    );
}

#[tokio::test]
async fn inbox_behavior_preserves_handler_errors_and_does_not_fail_after_completion_errors() {
    let cleanup_store = Arc::new(FailingInboxCleanup {
        fail_calls: AtomicUsize::new(0),
        fail_complete: false,
    });
    let mut cleanup_registry = Registry::new();
    cleanup_registry
        .register_request::<InboxWork, _>(FailingInboxHandler)
        .expect("failing handler registers");
    let cleanup_mediator = Mediator::new(cleanup_registry);
    let cleanup_pipeline =
        Pipeline::new().with(InboxBehavior::new(cleanup_store.clone(), U64Codec));

    assert!(matches!(
        cleanup_mediator.send_with(InboxWork, &cleanup_pipeline).await,
        Err(error) if error.message() == "inbox handler failed"
    ));
    assert_eq!(cleanup_store.fail_calls.load(Ordering::Relaxed), 1);

    let completion_store = Arc::new(FailingInboxCleanup {
        fail_calls: AtomicUsize::new(0),
        fail_complete: true,
    });
    let mut completion_registry = Registry::new();
    completion_registry
        .register_request::<InboxWork, _>(CountingHandler(Arc::new(AtomicUsize::new(0))))
        .expect("counting handler registers");
    let completion_mediator = Mediator::new(completion_registry);
    let completion_pipeline =
        Pipeline::new().with(InboxBehavior::new(completion_store.clone(), U64Codec));

    assert!(matches!(
        completion_mediator
            .send_with(InboxWork, &completion_pipeline)
            .await,
        Err(error) if error.message() == "inbox completion failed"
    ));
    assert_eq!(completion_store.fail_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn inbox_behavior_skips_deduplication_for_the_reserved_zero_message_identifier() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<UnidentifiedInboxWork, _>(CountingHandler(Arc::clone(&attempts)))
        .unwrap();
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryInbox::default());
    let pipeline = Pipeline::new().with(InboxBehavior::new(store, U64Codec));

    assert_eq!(
        mediator
            .send_with(UnidentifiedInboxWork, &pipeline)
            .await
            .unwrap(),
        21
    );
    assert_eq!(
        mediator
            .send_with(UnidentifiedInboxWork, &pipeline)
            .await
            .unwrap(),
        21
    );
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
}

#[test]
fn inbox_behavior_exposes_a_validated_claim_lease() {
    let store = Arc::new(MemoryInbox::default());
    let behavior = InboxBehavior::new(store, U64Codec)
        .with_claim_lease(Duration::from_secs(7))
        .unwrap();
    assert_eq!(behavior.claim_lease(), Duration::from_secs(7));

    let store = Arc::new(MemoryInbox::default());
    assert!(matches!(
        InboxBehavior::new(store, U64Codec).with_claim_lease(Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
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

#[tokio::test]
async fn dead_letter_behavior_records_terminal_command_failures_without_cloning() {
    let mut registry = Registry::new();
    registry
        .register_command::<DeadCommand, _>(DeadCommandHandler)
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryDeadLetters::new(1).expect("dead letter capacity is valid"));
    let pipeline = CommandPipeline::new().with(DeadLetterBehavior::new(Arc::clone(&store), 2));

    let error = mediator
        .send_command_with(DeadCommand, &pipeline)
        .await
        .expect_err("terminal command failure is preserved");

    assert_eq!(error.code(), ErrorCode::Validation);
    let letters = store.list(1).await.expect("dead letter list succeeds");
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].envelope().id(), 89);
    assert_eq!(letters[0].reason(), "command fatal");
    assert_eq!(letters[0].attempts(), 2);
}

#[tokio::test]
async fn dead_letter_storage_failure_never_masks_the_original_handler_error() {
    let mut registry = Registry::new();
    registry
        .register_request::<DeadWork, _>(DeadHandler)
        .expect("request handler registers");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(DeadLetterBehavior::new(Arc::new(FailingDeadLetters), 1));

    let error = mediator
        .send_with(DeadWork, &pipeline)
        .await
        .expect_err("the handler error is preserved when dead-letter storage is unavailable");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "fatal");
}

#[cfg(panic = "unwind")]
#[tokio::test]
async fn dead_letter_behavior_records_a_structured_failure_when_the_handler_panics() {
    let mut registry = Registry::new();
    registry
        .register_request::<DeadWork, _>(PanickingDeadHandler)
        .expect("request handler registers");
    let mediator = Mediator::new(registry);
    let store = Arc::new(MemoryDeadLetters::new(1).expect("dead letter capacity is valid"));
    let pipeline = Pipeline::new().with(DeadLetterBehavior::new(Arc::clone(&store), 1));

    let error = mediator
        .send_with(DeadWork, &pipeline)
        .await
        .expect_err("panic is converted into a structured error");

    assert_eq!(error.code(), ErrorCode::Internal);
    let letters = store.list(1).await.expect("dead letter list succeeds");
    assert_eq!(letters.len(), 1);
    assert_eq!(letters[0].diagnostics().error_code(), ErrorCode::Internal);
}
