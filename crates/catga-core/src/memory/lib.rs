#![forbid(unsafe_code)]
//! Bounded in-memory implementations of Catga contracts.
//!
//! This crate is useful for deterministic local composition and integration tests. Each adapter
//! keeps its own explicit capacity and uses the same public traits as a production backend, so
//! replacing it does not require a service locator or a global runtime.
//!
//! ```
//! use crate::ErrorCode;
//! use crate::memory::MemoryTransport;
//!
//! assert!(matches!(
//!     MemoryTransport::new(0),
//!     Err(error) if error.code() == ErrorCode::Validation
//! ));
//! ```

mod capacity;
mod claim;
mod dead_letter;
mod dsl_progress;
mod enhanced_snapshot;
mod event_store;
mod flow;
mod idempotency;
mod inbox;
mod lease;
mod outbox;
mod projection;
mod pubsub;
mod read_model;
mod snapshot;
mod state_machine;
mod subscription;
mod suspended_flow;
mod suspended_flow_timeout;
mod transport;

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
