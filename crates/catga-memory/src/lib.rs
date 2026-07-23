#![forbid(unsafe_code)]
//! In-memory implementations of Catga contracts.

mod outbox;

pub use outbox::MemoryOutbox;
