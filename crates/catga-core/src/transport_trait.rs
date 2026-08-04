//! Simplified typed transport using TypeId for compile-time routing.
//!
//! This module provides a clean Transport trait that works with the simplified
//! message traits (Request, Command, Event) without requiring Envelope wrapping.

#![allow(async_fn_in_trait)]

use std::time::Duration;

use crate::{CatgaResult, Command, Event, Request};

/// Simplified typed transport — unified interface for Request/Command/Event.
///
/// Implementors provide one concrete type (e.g., NatsTransport, LocalTransport)
/// that satisfies all methods. Users pass `impl Transport` to handlers.
///
/// # Example
///
/// ```ignore
/// use async_trait::async_trait;
/// use catga_core::{CatgaResult, Command, Event, Request, Transport};
/// use std::time::Duration;
///
/// struct GetUser { id: u64 }
/// impl catga_core::Message for GetUser {}
/// impl Request for GetUser { type Response = String; type TypeId = (); }
///
/// struct UpdateCache;
/// impl catga_core::Message for UpdateCache {}
/// impl Command for UpdateCache { type TypeId = (); }
///
/// #[derive(Clone)]
/// struct UserLoggedIn { user_id: u64 }
/// impl catga_core::Message for UserLoggedIn {}
/// impl Event for UserLoggedIn { type TypeId = (); }
///
/// struct MyTransport;
/// #[async_trait]
/// impl Transport for MyTransport {
///     async fn send<R: Request>(&self, request: R) -> CatgaResult<R::Response> {
///         // Route by R::TypeId
///         todo!("implement send")
///     }
///     async fn send_command<C: Command>(&self, command: C) -> CatgaResult<()> {
///         todo!("implement send_command")
///     }
///     async fn publish<E: Event>(&self, event: E) -> CatgaResult<()> {
///         todo!("implement publish")
///     }
///     async fn send_delayed<R: Request>(&self, request: R, delay: Duration) -> CatgaResult<R::Response> {
///         todo!("implement send_delayed")
///     }
///     async fn send_command_delayed<C: Command>(&self, command: C, delay: Duration) -> CatgaResult<()> {
///         todo!("implement send_command_delayed")
///     }
///     async fn publish_delayed<E: Event>(&self, event: E, delay: Duration) -> CatgaResult<()> {
///         todo!("implement publish_delayed")
///     }
/// }
/// ```
pub trait Transport: Send + Sync {
    /// Sends a request and waits for its typed response.
    ///
    /// The `TypeId` of `R` is used by implementations to route to the correct
    /// destination (topic, queue, or handler).
    async fn send<R: Request>(&self, request: R) -> CatgaResult<R::Response>;

    /// Sends a command (fire-and-forget) and waits for acknowledgement.
    async fn send_command<C: Command>(&self, command: C) -> CatgaResult<()>;

    /// Publishes an event to all subscribers.
    async fn publish<E: Event>(&self, event: E) -> CatgaResult<()>;

    /// Sends a request after a delay.
    ///
    /// Implementations may use timers, delayed queues, or scheduled message features.
    async fn send_delayed<R: Request>(
        &self,
        request: R,
        delay: Duration,
    ) -> CatgaResult<R::Response>;

    /// Sends a command after a delay.
    async fn send_command_delayed<C: Command>(
        &self,
        command: C,
        delay: Duration,
    ) -> CatgaResult<()>;

    /// Publishes an event after a delay.
    async fn publish_delayed<E: Event>(&self, event: E, delay: Duration) -> CatgaResult<()>;
}
