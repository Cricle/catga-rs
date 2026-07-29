//! Sealed dispatch traits for the typed mediator.
//!
//! These traits are implementation details of `catga_typed_mediator!`. They are public only
//! because the generated code references them through `::catga_core::sealed_dispatch`. Do not
//! implement them manually.

use crate::{CatgaResult, Command, Event, Request};

/// Sealed trait enabling zero-allocation request dispatch on a typed mediator.
///
/// Implemented by `catga_typed_mediator!` for each registered request type. The compiler
/// monomorphizes the call site per message type, eliminating `Box<dyn Any>`, downcast, and
/// vtable indirection.
pub trait SealedRequestDispatch<M: Request>: Send + Sync {
    /// Dispatches `message` to the concrete handler stored in the typed mediator.
    fn __dispatch_request(
        &self,
        message: M,
    ) -> impl std::future::Future<Output = CatgaResult<M::Response>> + Send;
}

/// Sealed trait enabling zero-allocation command dispatch on a typed mediator.
pub trait SealedCommandDispatch<C: Command>: Send + Sync {
    /// Dispatches `command` to the concrete handler stored in the typed mediator.
    fn __dispatch_command(
        &self,
        command: C,
    ) -> impl std::future::Future<Output = CatgaResult<()>> + Send;
}

/// Sealed trait enabling zero-allocation event dispatch on a typed mediator.
pub trait SealedEventDispatch<E: Event>: Send + Sync {
    /// Dispatches `event` to all concrete handlers stored in the typed mediator.
    fn __dispatch_event(
        &self,
        event: E,
    ) -> impl std::future::Future<Output = CatgaResult<()>> + Send;
}
