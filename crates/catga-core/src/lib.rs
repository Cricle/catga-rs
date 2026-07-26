#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

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
mod retry_jitter;
mod routing;
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
    BatchKeyProvider, BatchOptionsProvider, Command, DeliveryMode, Event, Message, MessageMetadata,
    MessagePriority, QualityOfService, Request,
};
pub use message_signing::{HmacMessageSigner, MessageSigner};
pub use message_type::MessageTypeRegistry;
pub use observability::TRACING_TARGET;
pub use outbox_processor::{OutboxLoopOptions, OutboxProcessor, OutboxRun};
pub use pipeline::{Behavior, MAX_PIPELINE_DEPTH, Next, Pipeline};
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
    DeadLetterStore, IdempotencyStore, InboxStore, MAX_DEAD_LETTER_DESCRIPTION_BYTES,
    MAX_DEAD_LETTER_STAGE_BYTES, MAX_RETENTION_CLEANUP_LIMIT, ProcessingState,
    inbox_claim_expires_at, validate_completed_retention, validate_inbox_claim_lease,
    validate_retention_cleanup_limit,
};
pub use request_client::{EnvelopeRequestClient, RemoteRequest, RequestClient, RequestTransport};
pub use resilience::{ResilienceExecutor, ResilienceOptions};
pub use retry_jitter::RetryJitter;
pub use routing::MessageRouter;
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
pub use upgrading_event_store::UpgradingEventStore;
pub use versioned_transport::VersionedMessageTransport;
