#![forbid(unsafe_code)]
//! In-memory implementations of Catga contracts.

mod claim;
mod dead_letter;
mod idempotency;
mod inbox;
mod outbox;
mod transport;

pub use dead_letter::MemoryDeadLetters;
pub use idempotency::MemoryIdempotency;
pub use inbox::MemoryInbox;
pub use outbox::MemoryOutbox;
pub use transport::MemoryTransport;
