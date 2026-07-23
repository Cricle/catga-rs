#![forbid(unsafe_code)]
//! In-memory implementations of Catga contracts.

mod claim;
mod dead_letter;
mod enhanced_snapshot;
mod event_store;
mod flow;
mod idempotency;
mod inbox;
mod lease;
mod outbox;
mod projection;
mod read_model;
mod snapshot;
mod subscription;
mod suspended_flow;
mod transport;

pub use dead_letter::MemoryDeadLetters;
pub use enhanced_snapshot::MemoryEnhancedSnapshots;
pub use event_store::MemoryEventStore;
pub use flow::MemoryFlows;
pub use idempotency::MemoryIdempotency;
pub use inbox::MemoryInbox;
pub use lease::MemoryLeases;
pub use outbox::MemoryOutbox;
pub use projection::MemoryProjectionCheckpoints;
pub use read_model::{MemoryChangeTracker, MemoryReadModels};
pub use snapshot::MemorySnapshots;
pub use subscription::MemorySubscriptions;
pub use suspended_flow::MemorySuspendedFlows;
pub use transport::MemoryTransport;
