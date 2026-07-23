//! Typed state-machine configuration and optimistic execution contracts.

mod actions;
mod definition;
mod executor;
mod model;
mod router;
mod store;

pub use definition::{StateMachine, StateMachineBuilder};
pub use executor::StateMachineExecutor;
pub use model::{StateMachineResult, StateMachineSnapshot, StateMachineState};
pub use router::StateMachineEventRouter;
pub use store::StateMachineStore;
