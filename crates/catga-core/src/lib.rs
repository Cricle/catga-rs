#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

mod aggregate;
mod behaviors;
mod cache;
mod codec;
mod correlation;
mod distributed_id;
mod error;
mod event_store;
mod event_version;
mod handler;
mod mediator;
mod message;
mod outbox_processor;
mod pipeline;
mod projection;
mod registry;
mod reliability;
mod snapshot;
mod store;
mod subscription;
mod time_travel;
mod transport;
mod upgrading_event_store;

pub use aggregate::{Aggregate, AggregateRepository, EventCountSnapshotStrategy, SnapshotStrategy};
pub use behaviors::{
    CorrelationBehavior, DeadLetterBehavior, DeadLetterEnvelope, IdempotencyBehavior,
    IdempotencyKey, InboxBehavior, InboxKey, RetryBehavior, TimeoutBehavior,
};
pub use cache::CachedResultCodec;
pub use catga_macros::{Message, catga_handlers};
pub use codec::EnvelopeCodec;
pub use correlation::{Correlated, current_correlation_id};
pub use distributed_id::{
    DistributedIdGenerator, IdMetadata, SnowflakeIdGenerator, SnowflakeLayout,
};
pub use error::{CatgaError, CatgaResult, ErrorCode};
pub use event_store::{EventStore, EventStream, StoredEvent, VersionInfo};
pub use event_version::{EventUpgrader, EventVersionRegistry};
pub use handler::{EventHandler, Handler};
pub use mediator::Mediator;
pub use message::{Command, Event, Message, MessageMetadata, Request};
pub use outbox_processor::{OutboxProcessor, OutboxRun};
pub use pipeline::{Behavior, Next, Pipeline};
pub use projection::{
    CatchUpProjectionRunner, Projection, ProjectionCheckpoint, ProjectionCheckpointStore,
    ProjectionRun,
};
pub use registry::Registry;
pub use reliability::{DeadLetter, DeadLetterStore, IdempotencyStore, InboxStore, ProcessingState};
pub use snapshot::{Snapshot, SnapshotStore};
pub use store::{Envelope, OutboxMessage, OutboxState, OutboxStore};
pub use subscription::{
    CompetingSubscriptionRunner, PersistentSubscription, SubscriptionCheckpoint,
    SubscriptionHandler, SubscriptionRun, SubscriptionRunner, SubscriptionStore,
};
pub use time_travel::{StateComparison, TimeTravelService};
pub use transport::{Acknowledger, Delivery, MessageTransport};
pub use upgrading_event_store::UpgradingEventStore;
