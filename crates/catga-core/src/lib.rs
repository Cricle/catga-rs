#![forbid(unsafe_code)]
//! Core contracts for the Catga CQRS runtime.

mod error;

pub use error::{CatgaError, CatgaResult, ErrorCode};
