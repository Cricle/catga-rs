#![forbid(unsafe_code)]
//! Redis Streams transport for Catga.

mod acknowledgement;
mod config;
mod event_store;
mod lease;
mod projection;
mod snapshot;
mod transport;

pub use config::RedisConfig;
pub use event_store::RedisEventStore;
pub use lease::RedisLeases;
pub use projection::RedisProjectionCheckpoints;
pub use snapshot::RedisSnapshotStore;
pub use transport::RedisTransport;
