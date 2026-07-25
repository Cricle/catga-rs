#![forbid(unsafe_code)]
//! NATS JetStream transport for Catga.

mod acknowledgement;
mod config;
mod dead_letter;
mod dsl_progress;
mod enhanced_snapshot;
mod event_store;
mod flow;
mod idempotency;
mod inbox;
mod lease;
mod outbox;
mod projection;
mod pubsub;
mod record;
mod rpc;
mod snapshot;
mod state_machine;
mod subscription;
mod suspended_flow;
mod suspended_flow_timeout;

pub use catga_codec_postcard::{PostcardRequestClient, PostcardRpcResponse};
mod transport;

pub use config::{NatsConfig, NatsDestinationConfig, NatsPubSubConfig};
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
pub use pubsub::NatsPubSubTransport;
pub use rpc::{NatsRequest, NatsRequestClient, NatsRequestServer, NatsTypedRequestClient};
pub use snapshot::NatsSnapshotStore;
pub use state_machine::NatsStateMachines;
pub use subscription::NatsSubscriptions;
pub use suspended_flow::NatsSuspendedFlows;
pub use transport::NatsTransport;
