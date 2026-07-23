#![forbid(unsafe_code)]
//! In-memory implementations of Catga contracts.

mod outbox;
mod transport;

pub use outbox::MemoryOutbox;
pub use transport::MemoryTransport;
