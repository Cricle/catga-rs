#![forbid(unsafe_code)]
//! NATS JetStream transport for Catga.

mod acknowledgement;
mod config;
mod transport;

pub use config::NatsConfig;
pub use transport::NatsTransport;
