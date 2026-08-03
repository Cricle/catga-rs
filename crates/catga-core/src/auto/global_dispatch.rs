//! Thread-local global dispatch functions for catga-core auto module.
//!
//! These functions allow sending messages from anywhere in the application
//! without explicitly passing the mediator handle.

use crate::{CatgaResult, Command, Event, MediatorHandle, Mediator, Request};
use std::future::Future;

thread_local! {
    static MEDIATOR: MediatorHandle = MediatorHandle::new();
}

/// Binds the mediator at startup (called by #[catga_main]).
pub fn bind_mediator(mediator: std::sync::Arc<Mediator>) -> CatgaResult<()> {
    MEDIATOR.with(|m| m.bind(mediator))
}

/// Returns a clone of the mediator handle.
pub fn mediator_handle() -> MediatorHandle {
    MEDIATOR.with(|m| m.clone())
}

/// Sends a request and returns the response.
pub fn send<M: Request + 'static>(msg: M) -> impl Future<Output = CatgaResult<M::Response>> {
    let handle = mediator_handle();
    async move { handle.send(msg).await }
}

/// Sends a command (fire-and-forget style).
pub fn send_command<C: Command + 'static>(cmd: C) -> impl Future<Output = CatgaResult<()>> {
    let handle = mediator_handle();
    async move { handle.send_command(cmd).await }
}

/// Publishes an event to all subscribers.
pub fn publish<E: Event + 'static>(evt: E) -> impl Future<Output = CatgaResult<()>> {
    let handle = mediator_handle();
    async move { handle.publish(evt).await }
}

/// Checks if the mediator is bound.
pub fn is_bound() -> bool {
    MEDIATOR.with(|m| m.is_bound())
}
