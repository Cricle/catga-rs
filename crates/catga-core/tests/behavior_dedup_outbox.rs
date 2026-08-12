//! Strict contracts for deduplication and durability behaviors: idempotency
//! claims, inbox delivery fencing, dead-letter retention, and outbox enqueue.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    CachedResultCodec, CatgaError, CatgaResult, DeadLetterBehavior, DeadLetterStore, ErrorCode,
    IdempotencyBehavior, IdempotencyStore, InboxBehavior, InboxClaim, InboxStore, Mediator,
    OutboxBehavior, OutboxStore, Pipeline, ProcessingState,
    memory::{MemoryDeadLetters, MemoryIdempotency, MemoryInbox, MemoryOutbox},
};

#[path = "support/behavior_support.rs"]
mod behavior_support;

use behavior_support::{Req, req_mediator};

struct CmdTypeId;
impl catga_core::MessageTypeId for CmdTypeId {
    const NAME: &'static str = "BehaviorCmd";
}

/// Cloneable command used by command-behavior contracts.
#[derive(Clone)]
struct Cmd(pub u64);

impl catga_core::Message for Cmd {}
impl catga_core::Command for Cmd {
    type TypeId = CmdTypeId;
}
impl catga_core::DeadLetterEnvelope for Cmd {
    fn dead_letter_envelope(&self) -> catga_core::Envelope {
        behavior_support::env(self.0)
    }
}

/// A handler body that always panics, with an inferable result type.
async fn panicked<T>(reason: &'static str) -> CatgaResult<T> {
    panic!("{reason}")
}

/// Little-endian u64 codec used to cache request responses.
struct U64Codec;

impl CachedResultCodec<u64> for U64Codec {
    fn encode(&self, value: &u64) -> CatgaResult<Arc<[u8]>> {
        Ok(Arc::from(value.to_le_bytes().as_slice()))
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u64> {
        let sized: [u8; 8] = bytes
            .try_into()
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "cached payload is corrupt"))?;
        Ok(u64::from_le_bytes(sized))
    }
}

/// Codec whose encoder always fails, exercising cleanup branches.
struct BrokenEncodeCodec;

impl CachedResultCodec<u64> for BrokenEncodeCodec {
    fn encode(&self, _: &u64) -> CatgaResult<Arc<[u8]>> {
        Err(CatgaError::new(ErrorCode::Internal, "encoding unavailable"))
    }

    fn decode(&self, _: &[u8]) -> CatgaResult<u64> {
        Ok(0)
    }
}

fn counting_mediator(executions: Arc<std::sync::atomic::AtomicUsize>) -> Arc<Mediator> {
    req_mediator(catga_core::request_handler(move |req: Req| {
        let executions = Arc::clone(&executions);
        async move {
            executions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(req.id)
        }
    }))
}

#[tokio::test]
async fn idempotency_behavior_caches_responses_and_fences_duplicates() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(MemoryIdempotency::default());
    let pipeline = Pipeline::<Req>::new().with(IdempotencyBehavior::new(
        Arc::clone(&store) as Arc<dyn IdempotencyStore>,
        U64Codec,
    ));

    let first = mediator
        .send_with(
            Req {
                id: 1,
                key: "order-1",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the first execution succeeds");
    assert_eq!(first, 1);
    let replay = mediator
        .send_with(
            Req {
                id: 1,
                key: "order-1",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the duplicate replays the cached response");
    assert_eq!(replay, 1);
    assert_eq!(
        executions.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the handler runs exactly once"
    );
}

#[tokio::test]
async fn idempotency_behavior_rejects_in_flight_duplicates_and_cleans_failures() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(MemoryIdempotency::default());
    let pipeline = Pipeline::<Req>::new().with(IdempotencyBehavior::new(
        Arc::clone(&store) as Arc<dyn IdempotencyStore>,
        U64Codec,
    ));

    // A claim held without completion surfaces Conflict to duplicates.
    store.try_claim("held").await.expect("claim succeeds");
    let error = mediator
        .send_with(
            Req {
                id: 2,
                key: "held",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("an in-flight duplicate conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // A failed handler releases the claim and preserves the original error.
    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "business rule"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 3,
                key: "fails",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the handler failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        store.state("fails").await.expect("state succeeds"),
        Some(ProcessingState::Failed)
    );
    let retried = mediator
        .send_with(
            Req {
                id: 3,
                key: "fails",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("a failed key is reclaimable");
    assert_eq!(retried, 3);

    // A panicking handler maps to Internal and still releases the claim.
    let panicking = req_mediator(catga_core::request_handler(move |_: Req| {
        panicked::<u64>("handler exploded")
    }));
    let error = panicking
        .send_with(
            Req {
                id: 4,
                key: "panics",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the panic maps to an internal error");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(
        mediator
            .send_with(
                Req {
                    id: 4,
                    key: "panics",
                    ..Default::default()
                },
                &pipeline
            )
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn idempotency_behavior_surfaces_encode_and_cleanup_failures() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(MemoryIdempotency::default());
    let pipeline = Pipeline::<Req>::new().with(IdempotencyBehavior::new(
        Arc::clone(&store) as Arc<dyn IdempotencyStore>,
        BrokenEncodeCodec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 1,
                key: "encode-breaks",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("an encode failure surfaces");
    assert_eq!(error.code(), ErrorCode::Internal);
    // The claim is released so a fixed codec could retry the key.
    assert_eq!(
        store.state("encode-breaks").await.expect("state succeeds"),
        Some(ProcessingState::Failed)
    );
}

/// Idempotency store whose cleanup operation always fails.
struct FailCleanupIdempotency {
    inner: MemoryIdempotency,
}

#[async_trait]
impl IdempotencyStore for FailCleanupIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        self.inner.try_claim(key).await
    }
    async fn complete(&self, _key: &str, _result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "completion persistence failed",
        ))
    }
    async fn fail(&self, _key: &str) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Unavailable, "cleanup failed"))
    }
    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        self.inner.state(key).await
    }
    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        self.inner.result(key).await
    }
}

#[tokio::test]
async fn idempotency_behavior_propagates_completion_store_errors() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(FailCleanupIdempotency {
        inner: MemoryIdempotency::default(),
    });
    let pipeline = Pipeline::<Req>::new().with(IdempotencyBehavior::new(
        Arc::clone(&store) as Arc<dyn IdempotencyStore>,
        U64Codec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 1,
                key: "store-breaks",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a completion failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // The failure path tolerates a broken cleanup while keeping its error.
    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "business rule"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 2,
                key: "store-breaks-2",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the original error wins over cleanup failures");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn inbox_behavior_bypasses_unidentified_messages() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(MemoryInbox::default());
    let pipeline = Pipeline::<Req>::new().with(
        InboxBehavior::new(Arc::clone(&store) as Arc<dyn InboxStore>, U64Codec)
            .with_claim_lease(std::time::Duration::from_secs(30))
            .expect("the lease validates"),
    );

    let value = mediator
        .send_with(
            Req {
                id: 0,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("a zero identifier bypasses deduplication");
    assert_eq!(value, 0);
    let value = mediator
        .send_with(
            Req {
                id: 0,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("bypassed messages execute every time");
    assert_eq!(value, 0);
    assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 2);

    // A bypassed failure also passes through untouched.
    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "bypass failure"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 0,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn inbox_behavior_deduplicates_and_fences_claims() {
    let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mediator = counting_mediator(Arc::clone(&executions));
    let store = Arc::new(MemoryInbox::default());
    let behavior = InboxBehavior::new(Arc::clone(&store) as Arc<dyn InboxStore>, U64Codec);
    assert!(!behavior.claim_lease().is_zero());
    let pipeline = Pipeline::<Req>::new().with(behavior);

    let value = mediator
        .send_with(
            Req {
                id: 7,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the first delivery processes");
    assert_eq!(value, 7);
    let replay = mediator
        .send_with(
            Req {
                id: 7,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the duplicate replays the cached result");
    assert_eq!(replay, 7);
    assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 1);

    // A held claim without cached result conflicts.
    store
        .try_claim(9)
        .await
        .expect("claim succeeds")
        .expect("claim wins");
    let error = mediator
        .send_with(
            Req {
                id: 9,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("an in-flight duplicate conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // Invalid leases are rejected at construction.
    let error = InboxBehavior::new(Arc::clone(&store) as Arc<dyn InboxStore>, U64Codec)
        .with_claim_lease(std::time::Duration::ZERO)
        .map(|_| ())
        .expect_err("a zero lease fails");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn inbox_behavior_maps_handler_and_store_failures() {
    let store = Arc::new(MemoryInbox::default());
    let pipeline = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::clone(&store) as Arc<dyn InboxStore>,
        U64Codec,
    ));

    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "inbox failure"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 21,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the handler failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        store.try_claim(21).await.expect("claim succeeds").is_some(),
        "a failed delivery is reclaimable"
    );

    let panicking = req_mediator(catga_core::request_handler(move |_: Req| {
        panicked::<u64>("inbox handler exploded")
    }));
    let error = panicking
        .send_with(
            Req {
                id: 22,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the panic maps to Internal");
    assert_eq!(error.code(), ErrorCode::Internal);

    let broken_encode = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::clone(&store) as Arc<dyn InboxStore>,
        BrokenEncodeCodec,
    ));
    let mediator = counting_mediator(Arc::new(std::sync::atomic::AtomicUsize::new(0)));
    let error = mediator
        .send_with(
            Req {
                id: 23,
                ..Default::default()
            },
            &broken_encode,
        )
        .await
        .expect_err("an encode failure surfaces");
    assert_eq!(error.code(), ErrorCode::Internal);
}

/// Inbox store stub whose mode selects which operation fails.
struct PartialInbox {
    mode: &'static str,
}

#[async_trait]
impl InboxStore for PartialInbox {
    async fn try_claim(&self, _: u64) -> CatgaResult<Option<InboxClaim>> {
        Err(CatgaError::new(ErrorCode::Unavailable, "claim store down"))
    }
    async fn try_claim_for(
        &self,
        message_id: u64,
        _: std::time::Duration,
    ) -> CatgaResult<Option<InboxClaim>> {
        match self.mode {
            "claim" => Err(CatgaError::new(ErrorCode::Unavailable, "claim store down")),
            "result" | "decode" => Ok(None),
            _ => Ok(Some(
                InboxClaim::new(message_id, 1).expect("a nonzero generation yields a claim"),
            )),
        }
    }
    async fn complete(&self, _: InboxClaim, _: Option<Arc<[u8]>>) -> CatgaResult<()> {
        if self.mode == "complete" {
            return Err(CatgaError::new(ErrorCode::Unavailable, "complete failed"));
        }
        Ok(())
    }
    async fn fail(&self, _: InboxClaim) -> CatgaResult<()> {
        Ok(())
    }
    async fn state(&self, _: u64) -> CatgaResult<Option<ProcessingState>> {
        Ok(None)
    }
    async fn result(&self, _: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        match self.mode {
            "result" => Err(CatgaError::new(ErrorCode::Unavailable, "result failed")),
            "decode" => Ok(Some(Arc::from([1_u8, 2, 3].as_slice()))),
            _ => Ok(None),
        }
    }
}

#[tokio::test]
async fn inbox_behavior_surfaces_claim_result_and_complete_errors() {
    let mediator = counting_mediator(Arc::new(std::sync::atomic::AtomicUsize::new(0)));

    let claim_errors = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::new(PartialInbox { mode: "claim" }) as Arc<dyn InboxStore>,
        U64Codec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 31,
                ..Default::default()
            },
            &claim_errors,
        )
        .await
        .expect_err("a claim failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // A duplicate whose cached-result lookup fails surfaces the store error.
    let result_errors = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::new(PartialInbox { mode: "result" }) as Arc<dyn InboxStore>,
        U64Codec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 32,
                ..Default::default()
            },
            &result_errors,
        )
        .await
        .expect_err("a cached-result lookup failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(error.message().contains("result failed"));

    let complete_errors = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::new(PartialInbox { mode: "complete" }) as Arc<dyn InboxStore>,
        U64Codec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 33,
                ..Default::default()
            },
            &complete_errors,
        )
        .await
        .expect_err("a completion failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    let decode_errors = Pipeline::<Req>::new().with(InboxBehavior::new(
        Arc::new(PartialInbox { mode: "decode" }) as Arc<dyn InboxStore>,
        U64Codec,
    ));
    let error = mediator
        .send_with(
            Req {
                id: 34,
                ..Default::default()
            },
            &decode_errors,
        )
        .await
        .expect_err("a corrupt cached payload fails decoding");
    assert_eq!(error.code(), ErrorCode::Internal);
}

#[tokio::test]
async fn dead_letter_behavior_retains_only_terminal_failures() {
    let letters = Arc::new(MemoryDeadLetters::new(4).expect("queue builds"));
    let pipeline = Pipeline::<Req>::new().with(DeadLetterBehavior::new(Arc::clone(&letters), 3));

    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "terminal failure"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the terminal failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    let stored = letters.list(10).await.expect("list succeeds");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].envelope().id(), 1);
    assert_eq!(stored[0].attempts(), 3);

    // Transient failures stay retryable and are not retained.
    let transient = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Transient, "retryable"))
    }));
    transient
        .send_with(
            Req {
                id: 2,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the transient failure surfaces");
    assert_eq!(letters.list(10).await.expect("list succeeds").len(), 1);

    // Panics map to Internal, which is terminal and therefore retained.
    let panicking = req_mediator(catga_core::request_handler(move |_: Req| {
        panicked::<u64>("dead letter handler exploded")
    }));
    panicking
        .send_with(
            Req {
                id: 3,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the panic maps to Internal");
    assert_eq!(letters.list(10).await.expect("list succeeds").len(), 2);

    // Successes never touch the dead-letter queue.
    let mediator = counting_mediator(Arc::new(std::sync::atomic::AtomicUsize::new(0)));
    mediator
        .send_with(
            Req {
                id: 4,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the success passes");
    assert_eq!(letters.list(10).await.expect("list succeeds").len(), 2);
}

/// Dead-letter store that refuses writes, exercising the warn-and-continue branch.
struct RefusingDeadLetters;

#[async_trait]
impl DeadLetterStore for RefusingDeadLetters {
    async fn enqueue(&self, _: catga_core::DeadLetter) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Unavailable, "dead letters full"))
    }
    async fn list(&self, _: usize) -> CatgaResult<Vec<catga_core::DeadLetter>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn dead_letter_behavior_preserves_errors_when_retention_fails() {
    let pipeline =
        Pipeline::<Req>::new().with(DeadLetterBehavior::new(Arc::new(RefusingDeadLetters), 1));
    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "original"))
    }));
    let error = failing
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the original error is preserved");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(error.message().contains("original"));
}

#[tokio::test]
async fn dead_letter_behavior_covers_the_command_pipeline() {
    let letters = Arc::new(MemoryDeadLetters::new(4).expect("queue builds"));
    let pipeline = catga_core::CommandPipeline::<Cmd>::new()
        .with(DeadLetterBehavior::new(Arc::clone(&letters), 2));
    let mut registry = catga_core::Registry::new();
    registry
        .register_command::<Cmd, _>(catga_core::command_handler(move |_: Cmd| async move {
            Err(CatgaError::new(ErrorCode::Validation, "command failed"))
        }))
        .expect("command registration succeeds");
    let mediator = Mediator::new(registry);
    let error = mediator
        .send_command_with(Cmd(5), &pipeline)
        .await
        .expect_err("the command failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    let stored = letters.list(10).await.expect("list succeeds");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].envelope().id(), 5);
}

#[tokio::test]
async fn outbox_behavior_enqueues_only_successful_envelopes() {
    let outbox = Arc::new(MemoryOutbox::default());
    let pipeline = Pipeline::<Req>::new().with(OutboxBehavior::new(Arc::clone(&outbox)));

    let mediator = counting_mediator(Arc::new(std::sync::atomic::AtomicUsize::new(0)));
    mediator
        .send_with(
            Req {
                id: 11,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the request succeeds");
    let claimed = outbox.claim("publisher", 10).await.expect("claim succeeds");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id(), 11);

    // Failed handlers leave nothing to deliver.
    let failing = req_mediator(catga_core::request_handler(move |_: Req| async move {
        Err(CatgaError::new(ErrorCode::Validation, "no outbox record"))
    }));
    failing
        .send_with(
            Req {
                id: 12,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the failure surfaces");
    let claimed = outbox.claim("publisher", 10).await.expect("claim succeeds");
    assert!(claimed.is_empty());

    // Store enqueue errors propagate to the caller.
    let tiny = Arc::new(MemoryOutbox::new(1).expect("capacity builds"));
    let bounded = Pipeline::<Req>::new().with(OutboxBehavior::new(Arc::clone(&tiny)));
    mediator
        .send_with(
            Req {
                id: 13,
                ..Default::default()
            },
            &bounded,
        )
        .await
        .expect("the first enqueue fits");
    let error = mediator
        .send_with(
            Req {
                id: 14,
                ..Default::default()
            },
            &bounded,
        )
        .await
        .expect_err("a full outbox surfaces backpressure");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}
