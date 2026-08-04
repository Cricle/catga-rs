#![forbid(unsafe_code)]
//! In-memory store implementations.

pub mod capacity;
/// In-memory claim store.
pub mod claim;
/// In-memory dead letter store.
pub mod dead_letter;
pub mod dsl_progress;
pub mod enhanced_snapshot;
/// In-memory event store.
pub mod event_store;
pub mod flow;
/// In-memory idempotency store.
pub mod idempotency;
/// In-memory inbox store.
pub mod inbox;
pub mod lease;
/// In-memory outbox store.
pub mod outbox;
/// In-memory projection store.
pub mod projection;
pub mod pubsub;
pub mod read_model;
/// In-memory snapshot store.
pub mod snapshot;
pub mod state_machine;
/// In-memory subscription store.
pub mod subscription;
pub mod suspended_flow;
/// In-memory suspended flow timeout store.
pub mod suspended_flow_timeout;
/// In-memory transport.
pub mod transport;

pub use capacity::DEFAULT_MEMORY_RECORD_CAPACITY;
pub use dead_letter::MemoryDeadLetters;
pub use dsl_progress::MemoryDslStepProgress;
pub use enhanced_snapshot::MemoryEnhancedSnapshots;
pub use event_store::MemoryEventStore;
pub use flow::MemoryFlows;
pub use idempotency::MemoryIdempotency;
pub use inbox::MemoryInbox;
pub use lease::MemoryLeases;
pub use outbox::MemoryOutbox;
pub use projection::MemoryProjectionCheckpoints;
pub use pubsub::MemoryPubSubTransport;
pub use read_model::{MemoryChangeTracker, MemoryReadModels};
pub use snapshot::MemorySnapshots;
pub use state_machine::MemoryStateMachines;
pub use subscription::MemorySubscriptions;
pub use suspended_flow::MemorySuspendedFlows;
pub use transport::MemoryTransport;
