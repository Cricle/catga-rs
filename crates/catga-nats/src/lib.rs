#![forbid(unsafe_code)]
//! NATS JetStream transport for Catga.

mod acknowledgement;
mod config;
mod event_store;
mod lease;
mod snapshot;
mod transport;

pub use config::NatsConfig;
pub use event_store::NatsEventStore;
pub use lease::NatsLeases;
pub use snapshot::NatsSnapshotStore;
pub use transport::NatsTransport;
