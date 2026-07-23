#![forbid(unsafe_code)]
//! In-memory implementations of Catga contracts.

mod claim;
mod dead_letter;
mod event_store;
mod idempotency;
mod inbox;
mod outbox;
mod projection;
mod snapshot;
mod transport;

pub use dead_letter::MemoryDeadLetters;
pub use event_store::MemoryEventStore;
pub use idempotency::MemoryIdempotency;
pub use inbox::MemoryInbox;
pub use outbox::MemoryOutbox;
pub use projection::MemoryProjectionCheckpoints;
pub use snapshot::MemorySnapshots;
pub use transport::MemoryTransport;
