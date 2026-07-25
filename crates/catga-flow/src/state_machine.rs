//! Typed state-machine configuration and optimistic execution contracts.

mod actions;
mod definition;
mod executor;
mod model;
mod persistence;
mod router;
mod store;

pub use definition::{StateMachine, StateMachineBuilder};
pub use executor::StateMachineExecutor;
pub use model::{StateMachineResult, StateMachineSnapshot, StateMachineState};
pub use persistence::{decode_state_machine_snapshot, encode_state_machine_snapshot};
pub use router::StateMachineEventRouter;
pub use store::StateMachineStore;
