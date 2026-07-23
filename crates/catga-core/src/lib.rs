#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

mod error;
mod handler;
mod mediator;
mod message;
mod registry;

pub use error::{CatgaError, CatgaResult, ErrorCode};
pub use handler::{EventHandler, Handler};
pub use mediator::Mediator;
pub use message::{Command, Event, Message, MessageMetadata, Request};
pub use registry::Registry;
