#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Core contracts for Catga's explicit, typed CQRS runtime.
//!
//! Applications construct a [`Registry`] once at startup, then dispatch values through a
//! [`Mediator`]. [`catga_handlers!`] keeps common handler registration concise while preserving
//! explicit startup composition. The core owns no adapter configuration or background worker;
//! callers choose bounded stores, transports, and policies from the companion crates.
//!
//! # Lifecycle, errors, and bounds
//!
//! Constructing a [`Registry`], [`Mediator`], pipeline, or lifecycle coordinator never starts a
//! background task. Initialize adapters and supervise polling, recovery, and shutdown from the
//! application runtime. [`TransportLifecycle`] provides the same explicit pattern for one
//! transport: initialize it, stop accepting work, then perform a bounded drain.
//!
//! Public operations return [`CatgaResult`]. Handle [`ErrorCode::Validation`] and
//! [`ErrorCode::Conflict`] as caller-visible input or coordination failures, and apply the
//! application's retry or dead-letter policy to transient delivery and store failures. Bulk and
//! persistence-facing APIs expose their limits as `MAX_*` constants (for example,
//! [`MAX_MEDIATOR_BATCH_SIZE`] and [`MAX_EVENT_STORE_PAGE_SIZE`]); select a bounded page or batch
//! size before accepting untrusted input.
//!
//! # Deterministic policy checks
//!
//! Retry policies can be tested without waiting for a timer. Production constructors use full
//! jitter; a fixed policy is useful when a deterministic integration test needs an exact delay.
//!
//! ```
//! use std::time::Duration;
//! use catga_core::RetryJitter;
//!
//! let policy = RetryJitter::fixed(Duration::from_millis(5));
//! assert_eq!(policy.delay_for_sample(Duration::from_secs(1), 0), Duration::from_millis(5));
//! ```
//!
//! # Mediator composition
//!
//! A mediator remains an explicit startup object. This longer example is `no_run` because an
//! application supplies its own Tokio runtime and `async-trait` dependency.
//!
//! ```no_run
//! use catga_core::{CatgaResult, Mediator, Request, catga_handlers, request_handler};
//!
//!
//! struct Double(u64);
//! impl catga_core::Message for Double {}
//! impl Request for Double { type Response = u64; }
//!
//! # async fn run() -> CatgaResult<()> {
//! let mediator = Mediator::new(catga_handlers! {
//!     request Double => request_handler(|message: Double| async move { Ok(message.0 * 2) })
//! }?);
//! assert_eq!(mediator.send(Double(21)).await?, 42);
//! # Ok(())
//! # }
//! ```
//!
//! # CQRS message roles
//!
//! A request has one handler and returns a typed response; a command has one handler and no
//! response; an event can have many handlers. Registration rejects duplicate request and command
//! handlers with [`ErrorCode::Conflict`], so misconfigured startup fails before dispatch.
//!
//! ```no_run
//! use async_trait::async_trait;
//! use catga_core::{
//!     CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Mediator, Message,
//!     Registry, Request,
//! };
//!
//!
//! struct GetBalance;
//! impl Message for GetBalance {}
//! impl Request for GetBalance { type Response = u64; }
//!
//! struct Credit;
//! impl Message for Credit {}
//! impl Command for Credit {}
//!
//! #[derive(Clone)]
//! struct BalanceChanged;
//! impl Message for BalanceChanged {}
//! impl Event for BalanceChanged {}
//!
//! struct BalanceReader;
//! #[async_trait]
//! impl Handler<GetBalance> for BalanceReader {
//!     async fn handle(&self, _: GetBalance) -> CatgaResult<u64> {
//!         Ok(42)
//!     }
//! }
//!
//! struct CreditWriter;
//! #[async_trait]
//! impl CommandHandler<Credit> for CreditWriter {
//!     async fn handle(&self, _: Credit) -> CatgaResult<()> {
//!         Ok(())
//!     }
//! }
//!
//! struct BalanceProjection;
//! #[async_trait]
//! impl EventHandler<BalanceChanged> for BalanceProjection {
//!     async fn handle(&self, _: BalanceChanged) -> CatgaResult<()> {
//!         Ok(())
//!     }
//! }
//!
//! # async fn run() -> CatgaResult<()> {
//! let mut registry = Registry::new();
//! registry.register_request::<GetBalance, _>(BalanceReader)?;
//! registry.register_command::<Credit, _>(CreditWriter)?;
//! registry.register_event::<BalanceChanged, _>(BalanceProjection);
//! let mediator = Mediator::new(registry);
//!
//! assert_eq!(mediator.send(GetBalance).await?, 42);
//! mediator.send_command(Credit).await?;
//! mediator.publish(BalanceChanged).await?;
//! # Ok(())
//! # }
//! ```

mod aggregate;
mod auto_snapshot;
mod behaviors;
mod cache;
mod cancellation;
mod cluster;
pub mod codec;
mod codec_traits;
mod compression;
mod consensus;
mod consumer;
mod correlation;
pub mod distributed_id;
pub mod error;
mod event_store;
mod event_version;
mod fault;
pub mod flow;
mod handler;
pub mod hash;
mod lease;
mod lifecycle;
pub mod macros;
mod mediator;
/// Bounded in-memory implementations of Catga contracts.
///
/// This module is useful for deterministic local composition and integration tests.
pub mod memory;
mod message;
mod message_signing;
mod message_type;
mod observability;
mod outbox_processor;
mod pipeline;
pub mod projection;
mod read_model;
mod registry;
mod reliability;
mod request_client;
mod resilience;
mod resilient_transport;
mod retry_jitter;
mod routing;
pub mod sealed_dispatch;
mod security;
mod snapshot;
mod snapshot_codec;
mod store;
mod subscription;
pub mod telemetry;
pub mod testing;
pub mod time;
mod time_travel;
mod trace_context;
mod transport;
mod transport_batching;
mod upgrading_event_store;
pub mod validation;

pub use aggregate::{
    Aggregate, AggregateRepository, CompositeSnapshotStrategy, EventCountSnapshotStrategy,
    SnapshotStrategy, TimeBasedSnapshotStrategy,
};
pub use auto_snapshot::AutoSnapshotManager;
pub use behaviors::{
    AuthorizationBehavior, AuthorizationPolicies, AuthorizationPolicy, AutoBatchingBehavior,
    AutoBatchingRunner, BatchOptions, CircuitBreakerBehavior, CircuitBreakerOptions,
    CircuitBreakerOptionsBuilder, CompensationBehavior, CompensationPublisher, CorrelationBehavior,
    DeadLetterBehavior, DeadLetterEnvelope, DistributedLockBehavior, DistributedLockKey,
    EventCompensationPublisher, FaultPublisher, FaultPublishingBehavior, IdempotencyBehavior,
    IdempotencyKey, InboxBehavior, InboxKey, OutboxBehavior, OutboxEnvelope, RetryBehavior,
    TimeoutBehavior, ValidationBehavior, Validator,
};
pub use cache::CachedResultCodec;
pub use cancellation::{current_cancellation, scope_cancellation};
pub use cluster::ClusterForwarder;
pub use codec::{
    bincode::{BincodeCodec, MAX_BINCODE_FRAME_BYTES},
    memorypack::{
        MemoryPackCodec, MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackError,
        MemoryPackReader, MemoryPackRpcResponse, MemoryPackSerialize, MemoryPackSerializer,
        MemoryPackWriter, MemoryPackable,
    },
};
pub use codec_traits::{EnvelopeCodec, PayloadDecoder, PayloadEncoder};
pub use compression::{
    CompressionAlgorithm, CompressionStats, DEFAULT_MAX_DECOMPRESSED_BYTES, compress,
    compress_into, compress_to_slice, decompress, decompress_limited, is_compressed,
};
pub use consensus::{ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine};
pub use consumer::{
    CompetingConsumer, ConsumerRun, DeliveryHandler, TypedDeliveryHandler,
    TypedDeliveryHandlerAdapter,
};
pub use correlation::{
    CORRELATION_ID_HEADER, Correlated, TransportContext, current_correlation_id,
    current_correlation_value, current_transport_context, scope_correlation_id,
    scope_correlation_value, scope_transport_context, scope_transport_context_value,
};
pub use distributed_id::{
    DistributedIdGenerator, IdMetadata, SnowflakeIdGenerator, SnowflakeLayout,
};
pub use error::{CatgaError, CatgaResult, ErrorCode, MAX_ERROR_DETAILS_BYTES, bounded_details};
pub use event_store::{
    EventPage, EventStore, EventStream, MAX_EVENT_STORE_PAGE_SIZE, StoredEvent, StreamIdsPage,
    VersionHistoryPage, VersionInfo, validate_event_store_page_size,
};
pub use event_version::{EventUpgrader, EventVersionRegistry};
pub use fault::Fault;
pub use flow::{
    DslFlow, DslFlowLifecycleHooks, DslQueryStep, FlowRuntime, FlowRuntimeResult, FlowScheduler,
    FlowTagPolicy, MemoryFlowScheduler, ScheduledResume,
    definition::{FlowDefinition, FlowStepOutcome},
    suspension,
};
pub use handler::{
    CommandHandler, CommandHandlerFn, EventHandler, EventHandlerFn, Handler, RequestHandlerFn,
    command_handler, command_handler_with, event_handler, event_handler_with, request_handler,
    request_handler_with,
};
pub use lease::LeaseStore;
pub use lifecycle::{
    AcceptanceGate, AsyncInitializable, AutoRecoveryOptions, HealthCheckable, OperationGuard,
    OperationTracker, RecoverableComponent, RecoveryManager, RecoveryResult, ShutdownCoordinator,
    Stoppable, TransportLifecycle, TransportLifecycleOptions, TransportShutdown, Waitable,
};
pub use macros::{
    Message, catga_command, catga_event, catga_handler, catga_handlers, catga_request,
    catga_service, catga_typed_mediator,
};
pub use mediator::{MAX_MEDIATOR_BATCH_SIZE, Mediator, MediatorHandle};
pub use message::{
    BatchKeyProvider, BatchOptionsProvider, Command, DelayedEvent, DelayedMessage, DelayedRequest,
    DeliveryMode, Event, Message, MessageMetadata, MessagePriority, QualityOfService, Request,
};
pub use message_signing::{HmacMessageSigner, MessageSigner};
pub use message_type::MessageTypeRegistry;
pub use observability::TRACING_TARGET;
pub use outbox_processor::{OutboxLoopOptions, OutboxProcessor, OutboxRun};
pub use pipeline::{
    Behavior, CommandBehavior, CommandNext, CommandPipeline, MAX_PIPELINE_DEPTH, Next, Pipeline,
};
pub use projection::{
    CatchUpProjectionRunner, LiveProjection, Projection, ProjectionCheckpoint,
    ProjectionCheckpointStore, ProjectionRun,
};
pub use read_model::{
    BatchSyncStrategy, ChangeKind, ChangeRecord, ChangeTracker, MAX_READ_MODEL_PAGE_SIZE,
    ReadModelStore, ReadModelSynchronizer, RealtimeSyncStrategy, ScheduledSyncStrategy,
    SyncStrategy, validate_read_model_page_size,
};
pub use registry::Registry;
pub use reliability::{
    DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_INBOX_CLAIM_LEASE, DeadLetter, DeadLetterDiagnostics,
    DeadLetterStore, IdempotencyStore, InboxClaim, InboxStore, MAX_DEAD_LETTER_DESCRIPTION_BYTES,
    MAX_DEAD_LETTER_STAGE_BYTES, MAX_RETENTION_CLEANUP_LIMIT, ProcessingState,
    inbox_claim_expires_at, validate_completed_retention, validate_inbox_claim_lease,
    validate_retention_cleanup_limit,
};
pub use request_client::{EnvelopeRequestClient, RemoteRequest, RequestClient, RequestTransport};
pub use resilience::{ResilienceExecutor, ResilienceOptions, retry_delay};
pub use resilient_transport::ResilientTransport;
pub use retry_jitter::RetryJitter;
pub use routing::{MessageDestinationRouter, MessageRouter};
pub use security::{
    AuthorizationRequirements, AuthorizedRequest, MAX_SECURITY_CLAIM_KEY_BYTES,
    MAX_SECURITY_CLAIM_VALUE_BYTES, MAX_SECURITY_CLAIMS, SecurityClaim, SecurityClaims,
    SecurityIdentity, current_security_identity, scope_security_identity,
};
pub use snapshot::{EnhancedSnapshotStore, Snapshot, SnapshotInfo, SnapshotStore};
pub use snapshot_codec::SnapshotCodec;
pub use store::{
    DEFAULT_OUTBOX_CLAIM_LEASE, DEFAULT_OUTBOX_MAX_RETRIES, Envelope, EnvelopeHeader,
    EnvelopeHeaders, MAX_ENVELOPE_HEADER_BYTES, MAX_ENVELOPE_HEADERS, MAX_OUTBOX_CLAIM_LEASE,
    MAX_OUTBOX_CLAIM_LIMIT, MAX_OUTBOX_FAILURE_ERROR_BYTES, OutboxMessage, OutboxState,
    OutboxStore, outbox_claim_expires_at, validate_outbox_claim_lease, validate_outbox_claim_limit,
    validate_outbox_message_id,
};
pub use subscription::{
    CompetingSubscriptionRunner, PersistentSubscription, SubscriptionCheckpoint,
    SubscriptionHandler, SubscriptionLoopOptions, SubscriptionRun, SubscriptionRunner,
    SubscriptionStore,
};
pub use testing::{
    EventHandlerSpy, HandlerSpy, MessageCapture, assert_contains, assert_error_code,
    assert_failure, assert_success, assert_value,
};
pub use time_travel::{
    MAX_STATE_COMPARISON_EVENTS, SnapshotTimeTravelService, StateComparison, TimeTravelService,
};
pub use trace_context::{
    MAX_TRACEPARENT_BYTES, MAX_TRACESTATE_BYTES, TRACEPARENT_HEADER, TRACESTATE_HEADER,
    TraceContext,
};
pub use transport::{
    Acknowledger, DEFAULT_TRANSPORT_BATCH_CONCURRENCY, Delivery, Destination, DestinationTransport,
    MessageTransport,
};
pub use transport_batching::{TransportBatchOptions, TransportBatchRunner, TransportBatcher};
pub use upgrading_event_store::UpgradingEventStore;
pub use validation::{
    EndpointValidation, format_validation_errors, validate_max_length, validate_min_count,
    validate_min_length, validate_not_empty, validate_positive, validate_range, validate_required,
};

/// Creates a compensating flow with named steps and their compensation actions.
///
/// The macro generates a [`DslFlow`] where each step has a corresponding compensation action
/// that runs in reverse order if a subsequent step fails. The context must implement
/// the step methods as async closures or functions.
///
/// ```
/// use catga_core::flow::DslFlow;
///
/// #[derive(Clone)]
/// struct Checkout;
/// impl Checkout {
///     async fn reserve_inventory(self) -> catga_core::CatgaResult<()> { Ok(()) }
///     async fn release_inventory(self) -> catga_core::CatgaResult<()> { Ok(()) }
///     async fn capture_payment(self) -> catga_core::CatgaResult<()> { Ok(()) }
///     async fn refund_payment(self) -> catga_core::CatgaResult<()> { Ok(()) }
/// }
///
/// let _flow: DslFlow<()> = catga_core::compensating_flow! {
///     "checkout";
///     context = Checkout;
///     steps {
///         reserve_inventory => release_inventory;
///         capture_payment => refund_payment;
///     }
/// };
/// ```
#[macro_export]
macro_rules! compensating_flow {
    (
        $name:expr;
        context = $context:expr;
        steps {
            $($run:ident => $compensate:ident;)+
        }
    ) => {{
        let mut __catga_flow = $crate::flow::DslFlow::new();
        $(__catga_flow = __catga_flow.compensate(
            {
                let __ctx = $context.clone();
                move |_| {
                    let ctx = __ctx.clone();
                    async move { ctx.$run().await }
                }
            },
            {
                let __ctx = $context.clone();
                move |_| {
                    let ctx = __ctx.clone();
                    async move { ctx.$compensate().await }
                }
            },
        );)+
        __catga_flow
    }};
    (
        $name:expr;
        context = $context:expr;
        $($run:expr => $compensate:expr);+ $(;)?
    ) => {{
        let __catga_flow_context = $context;
        let mut __catga_flow = $crate::flow::DslFlow::new();
        $(__catga_flow = __catga_flow.compensate(
            move |_ctx| { $run },
            move |_ctx| { $compensate },
        );)+
        __catga_flow
    }};
}
