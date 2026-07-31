#![forbid(unsafe_code)]
//! NATS JetStream transport for Catga.
//!
//! # Durable transport composition
//!
//! `NatsTransport` provisions exactly the stream and pull consumer named by
//! [`NatsConfig`]. Keep those names stable across rolling deployments; make a
//! new configuration for a destination with a different retention policy.
//! Construction is explicit and no worker is started by this crate.
//!
//! ```no_run
//! use catga_nats::{NatsConfig, NatsTransport};
//!
//! # async fn connect() -> Result<(), catga_core::CatgaError> {
//! let config = NatsConfig {
//!     server: "nats://127.0.0.1:4222".into(),
//!     stream: "orders".into(),
//!     subject: "orders.created".into(),
//!     consumer: "orders-worker".into(),
//! };
//! let transport = NatsTransport::connect(config).await?;
//! # drop(transport);
//! # Ok(())
//! # }
//! ```

mod acknowledgement;
mod config;
mod dead_letter;
mod dsl_progress;
mod enhanced_snapshot;
mod event_store;
mod flow;
mod idempotency;
mod inbox;
mod kv;
mod lease;
mod outbox;
mod projection;
mod projection_runner;
mod publisher;
mod pubsub;
mod record;
mod rpc;
mod scheduler;
mod snapshot;
mod state_machine;
mod subscription;
mod suspended_flow;
mod suspended_flow_timeout;

pub use catga_codec_memorypack::{MemoryPackRequestClient, MemoryPackRpcResponse};
mod transport;

pub use config::{
    DEFAULT_NATS_PULL_BATCH_SIZE, NatsConfig, NatsConsumerMode, NatsConsumerOptions,
    NatsDestinationConfig, NatsPubSubConfig, NatsPublisherConfig, NatsReceiveOptions,
    NatsTransportOptions,
};
pub use dead_letter::NatsDeadLetters;
pub use dsl_progress::NatsDslStepProgress;
pub use enhanced_snapshot::NatsEnhancedSnapshots;
pub use event_store::NatsEventStore;
pub use flow::NatsFlows;
pub use idempotency::NatsIdempotency;
pub use inbox::NatsInbox;
pub use lease::NatsLeases;
pub use outbox::NatsOutbox;
pub use projection::NatsProjectionCheckpoints;
pub use projection_runner::{NatsProjectionConfig, NatsProjectionRunner};
pub use publisher::NatsPublisher;
pub use pubsub::NatsPubSubTransport;
pub use rpc::{NatsRequest, NatsRequestClient, NatsRequestServer, NatsTypedRequestClient};
pub use scheduler::NatsFlowScheduler;
pub use snapshot::NatsSnapshotStore;
pub use state_machine::NatsStateMachines;
pub use subscription::NatsSubscriptions;
pub use suspended_flow::NatsSuspendedFlows;
pub use transport::NatsTransport;
