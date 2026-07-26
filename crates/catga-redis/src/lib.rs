#![forbid(unsafe_code)]
//! Redis Streams transport for Catga.
//!
//! Enable the `streams-rpc` Cargo feature to use the durable
//! `RedisStreamsRequestClient` and `RedisStreamsRequestServer` APIs. They write request
//! ingress through a [`catga_core::DestinationTransport`] such as [`RedisTransport`] while using
//! a private Redis Pub/Sub inbox only for the one correlated reply.

mod acknowledgement;
mod config;
mod dead_letter;
mod dsl_progress;
mod enhanced_snapshot;
mod event_store;
mod idempotency;
mod inbox;
mod lease;
mod outbox;
mod projection;
mod pubsub;
mod rpc;
mod scheduler;
mod snapshot;
mod state_machine;
#[cfg(feature = "streams-rpc")]
mod streams_rpc;
mod subscription;
mod suspended_flow;
mod suspended_flow_timeout;
mod transport;

pub use catga_codec_postcard::{PostcardRequestClient, PostcardRpcResponse};
pub use config::{
    MAX_REDIS_PENDING_RECLAIM_SCANS, RedisConfig, RedisPendingReclaimOptions, RedisPubSubConfig,
};
pub use dead_letter::RedisDeadLetters;
pub use dsl_progress::RedisDslStepProgress;
pub use enhanced_snapshot::RedisEnhancedSnapshots;
pub use event_store::RedisEventStore;
pub use idempotency::RedisIdempotency;
pub use inbox::RedisInbox;
pub use lease::RedisLeases;
pub use outbox::RedisOutbox;
pub use projection::RedisProjectionCheckpoints;
pub use pubsub::RedisPubSubTransport;
pub use rpc::{RedisRequest, RedisRequestClient, RedisRequestServer};
pub use scheduler::RedisFlowScheduler;
pub use snapshot::RedisSnapshotStore;
pub use state_machine::RedisStateMachines;
#[cfg(feature = "streams-rpc")]
pub use streams_rpc::{RedisStreamsRequest, RedisStreamsRequestClient, RedisStreamsRequestServer};
pub use subscription::RedisSubscriptions;
pub use suspended_flow::RedisSuspendedFlows;
pub use transport::RedisTransport;
