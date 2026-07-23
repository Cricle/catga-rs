#![forbid(unsafe_code)]
//! Durable and compensating flow primitives for Catga.

mod executor;
mod local;
mod state;
mod store;
mod suspension;

pub use executor::FlowExecutor;
pub use local::{Flow, FlowResult};
pub use state::{FlowState, FlowStatus};
pub use store::FlowStore;
pub use suspension::{FlowContinuation, WaitCondition, WaitPolicy, WaitResult};
