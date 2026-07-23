#![forbid(unsafe_code)]
//! NATS JetStream transport for Catga.

mod acknowledgement;
mod config;
mod dead_letter;
mod event_store;
mod idempotency;
mod inbox;
mod lease;
mod outbox;
mod snapshot;
mod transport;

pub use config::NatsConfig;
pub use dead_letter::NatsDeadLetters;
pub use event_store::NatsEventStore;
pub use idempotency::NatsIdempotency;
pub use inbox::NatsInbox;
pub use lease::NatsLeases;
pub use outbox::NatsOutbox;
pub use snapshot::NatsSnapshotStore;
pub use transport::NatsTransport;
