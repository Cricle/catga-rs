#![forbid(unsafe_code)]
//! Redis Streams transport for Catga.

mod acknowledgement;
mod config;
mod event_store;
mod inbox;
mod lease;
mod outbox;
mod projection;
mod snapshot;
mod transport;

pub use config::RedisConfig;
pub use event_store::RedisEventStore;
pub use inbox::RedisInbox;
pub use lease::RedisLeases;
pub use outbox::RedisOutbox;
pub use projection::RedisProjectionCheckpoints;
pub use snapshot::RedisSnapshotStore;
pub use transport::RedisTransport;
