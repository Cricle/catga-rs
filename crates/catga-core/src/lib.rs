#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

mod error;
mod message;

pub use error::{CatgaError, CatgaResult, ErrorCode};
pub use message::{Command, Event, Message, MessageMetadata, Request};
