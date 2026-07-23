#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

mod behaviors;
mod cache;
mod codec;
mod correlation;
mod error;
mod handler;
mod mediator;
mod message;
mod outbox_processor;
mod pipeline;
mod registry;
mod reliability;
mod store;
mod transport;

pub use behaviors::{
    CorrelationBehavior, IdempotencyBehavior, IdempotencyKey, RetryBehavior, TimeoutBehavior,
};
pub use cache::CachedResultCodec;
pub use catga_macros::{Message, catga_handlers};
pub use codec::EnvelopeCodec;
pub use correlation::{Correlated, current_correlation_id};
pub use error::{CatgaError, CatgaResult, ErrorCode};
pub use handler::{EventHandler, Handler};
pub use mediator::Mediator;
pub use message::{Command, Event, Message, MessageMetadata, Request};
pub use outbox_processor::{OutboxProcessor, OutboxRun};
pub use pipeline::{Behavior, Next, Pipeline};
pub use registry::Registry;
pub use reliability::{DeadLetter, DeadLetterStore, IdempotencyStore, InboxStore, ProcessingState};
pub use store::{Envelope, OutboxMessage, OutboxState, OutboxStore};
pub use transport::{Acknowledger, Delivery, MessageTransport};
