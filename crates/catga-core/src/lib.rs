#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Core contracts for Catga's explicit, typed CQRS runtime.
//!
//! Applications construct a [`Registry`] once at startup, then dispatch values through a
//! [`Mediator`]. [`catga_handlers!`] keeps common handler registration concise while preserving
//! explicit startup composition. The core owns no adapter configuration or background worker;
//! callers choose bounded stores, transports, and policies from the companion crates.
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
//! use async_trait::async_trait;
//! use catga_core::{CatgaResult, Handler, Mediator, Request, catga_handlers};
//!
//! struct Double(u64);
//! impl catga_core::Message for Double {}
//! impl Request for Double { type Response = u64; }
//!
//! struct DoubleHandler;
//! #[async_trait]
//! impl Handler<Double> for DoubleHandler {
//!     async fn handle(&self, message: Double) -> CatgaResult<u64> {
//!         Ok(message.0 * 2)
//!     }
//! }
//!
//! # async fn run() -> CatgaResult<()> {
//! let mediator = Mediator::new(catga_handlers! { request Double => DoubleHandler }?);
//! assert_eq!(mediator.send(Double(21)).await?, 42);
//! # Ok(())
//! # }
//! ```

mod aggregate;
mod auto_snapshot;
mod behaviors;
mod cache;
mod cancellation;
mod codec;
mod compression;
mod consumer;
mod correlation;
mod distributed_id;
mod error;
mod event_store;
mod event_version;
mod fault;
mod handler;
mod lease;
mod lifecycle;
mod mediator;
mod message;
mod message_signing;
mod message_type;
mod observability;
mod outbox_processor;
mod pipeline;
mod projection;
mod read_model;
mod registry;
mod reliability;
mod request_client;
mod resilience;
mod resilient_transport;
mod retry_jitter;
mod routing;
mod scheduler;
mod security;
mod snapshot;
mod snapshot_codec;
mod store;
mod subscription;
pub mod telemetry;
mod time_travel;
mod trace_context;
mod transport;
mod transport_batching;
mod typed_transport;
mod upgrading_event_store;
mod versioned_transport;

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
    IdempotencyKey, InboxBehavior, InboxKey, LoggingBehavior, OutboxBehavior, OutboxEnvelope,
    RetryBehavior, TimeoutBehavior, TracingBehavior, ValidationBehavior, Validator,
};
pub use cache::CachedResultCodec;
pub use cancellation::{current_cancellation, scope_cancellation};
pub use catga_macros::{Message, catga_handlers};
pub use codec::{EnvelopeCodec, PayloadDecoder, PayloadEncoder};
pub use compression::{
    CompressionAlgorithm, CompressionStats, DEFAULT_MAX_DECOMPRESSED_BYTES, compress,
    compress_into, compress_to_slice, decompress, decompress_limited, is_compressed,
};
pub use consumer::{CompetingConsumer, ConsumerRun, DeliveryHandler};
pub use correlation::{
    Correlated, TransportContext, current_correlation_id, current_correlation_value,
    current_transport_context, scope_correlation_id, scope_correlation_value,
    scope_transport_context,
};
pub use distributed_id::{
    DistributedIdGenerator, IdMetadata, SnowflakeIdGenerator, SnowflakeLayout,
};
pub use error::{CatgaError, CatgaResult, ErrorCode, MAX_ERROR_DETAILS_BYTES};
pub use event_store::{
    EventPage, EventStore, EventStream, MAX_EVENT_STORE_PAGE_SIZE, StoredEvent, StreamIdsPage,
    VersionHistoryPage, VersionInfo, validate_event_store_page_size,
};
pub use event_version::{EventUpgrader, EventVersionRegistry};
pub use fault::Fault;
pub use handler::{CommandHandler, EventHandler, Handler};
pub use lease::LeaseStore;
pub use lifecycle::{
    AcceptanceGate, AsyncInitializable, AutoRecoveryOptions, HealthCheckable, OperationGuard,
    OperationTracker, RecoverableComponent, RecoveryManager, RecoveryResult, ShutdownCoordinator,
    Stoppable, TransportLifecycle, TransportLifecycleOptions, TransportShutdown, Waitable,
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
pub use resilience::{ResilienceExecutor, ResilienceOptions};
pub use resilient_transport::ResilientTransport;
pub use retry_jitter::RetryJitter;
pub use routing::{MessageDestinationRouter, MessageRouter};
pub use scheduler::{
    MAX_CRON_SCHEDULE_BYTES, MAX_SCHEDULED_TASK_ID_BYTES, ScheduledTask, ScheduledTaskId,
    TaskSchedule, TaskScheduler,
};
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
pub use typed_transport::{TypedDelivery, TypedProcessOutcome, TypedTransport};
pub use upgrading_event_store::UpgradingEventStore;
pub use versioned_transport::VersionedMessageTransport;

/// Builds a typed, bounded [`Pipeline`] during application startup.
///
/// The macro accepts already-constructed behavior expressions, preserving their explicit
/// dependencies and shared state. It does not install global policy state or create a behavior
/// per request, so stateful stages such as circuit breakers retain one caller-owned lifecycle.
/// Every stage uses [`Pipeline::try_with`], returning a validation error instead of allowing a
/// generated configuration to exceed [`MAX_PIPELINE_DEPTH`].
///
/// ```
/// # use std::time::Duration;
/// # use catga_core::{Pipeline, RetryBehavior, TimeoutBehavior};
/// # #[derive(Clone)]
/// # struct Request;
/// # impl catga_core::Message for Request {}
/// # impl catga_core::Request for Request { type Response = (); }
/// let pipeline: Pipeline<Request> = catga_core::catga_pipeline!(
///     Request;
///     RetryBehavior::new(2, Duration::from_millis(10)),
///     TimeoutBehavior::new(Duration::from_secs(1)),
/// )?;
/// # Ok::<(), catga_core::CatgaError>(())
/// ```
#[macro_export]
macro_rules! catga_pipeline {
    ($message:ty; $($behavior:expr),* $(,)?) => {{
        (|| -> $crate::CatgaResult<$crate::Pipeline<$message>> {
            let pipeline = $crate::Pipeline::<$message>::new();
            $(
                let pipeline = pipeline.try_with($behavior)?;
            )*
            Ok(pipeline)
        })()
    }};
}

/// Builds a typed, bounded [`CommandPipeline`] during application startup.
///
/// This is the no-response command counterpart to [`catga_pipeline!`]. It accepts existing
/// [`CommandBehavior`] values, returns validation errors for excessive depth, and creates no
/// global or background state.
///
/// ```
/// use catga_core::{Command, CommandPipeline, Message, catga_command_pipeline};
///
/// struct Archive;
/// impl Message for Archive {}
/// impl Command for Archive {}
///
/// let pipeline: CommandPipeline<Archive> = catga_command_pipeline!(Archive;)?;
/// assert!(pipeline.is_empty());
/// # Ok::<(), catga_core::CatgaError>(())
/// ```
#[macro_export]
macro_rules! catga_command_pipeline {
    ($command:ty; $($behavior:expr),* $(,)?) => {{
        (|| -> $crate::CatgaResult<$crate::CommandPipeline<$command>> {
            let pipeline = $crate::CommandPipeline::<$command>::new();
            $(
                let pipeline = pipeline.try_with($behavior)?;
            )*
            Ok(pipeline)
        })()
    }};
}
