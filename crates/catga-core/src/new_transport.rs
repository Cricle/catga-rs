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
    async fn send_delayed<R: Request>(&self, request: R, delay: Duration) -> CatgaResult<R::Response>;

    /// Sends a command after a delay.
    async fn send_command_delayed<C: Command>(&self, command: C, delay: Duration) -> CatgaResult<()>;

    /// Publishes an event after a delay.
    async fn publish_delayed<E: Event>(&self, event: E, delay: Duration) -> CatgaResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, MessagePriority, MessageTypeId};

    // TypeId markers for tests
    mod type_ids {
        use crate::MessageTypeId;
        pub struct EchoTypeId;
        impl MessageTypeId for EchoTypeId {
            const NAME: &'static str = "Echo";
        }
    }

    #[derive(Clone, Debug)]
    struct Echo(String);

    impl Message for Echo {
        fn schema_version(&self) -> u32 { 1 }
        fn priority(&self) -> MessagePriority { MessagePriority::Normal }
    }

    impl Request for Echo {
        type Response = String;
        type TypeId = type_ids::EchoTypeId;
    }

    #[derive(Clone, Debug)]
    struct Reset;

    impl Message for Reset {
        fn schema_version(&self) -> u32 { 1 }
        fn priority(&self) -> MessagePriority { MessagePriority::High }
    }

    impl Command for Reset {
        type TypeId = type_ids::EchoTypeId;
    }

    #[derive(Clone, Debug)]
    struct UserLoggedIn { user_id: u64 }

    impl Message for UserLoggedIn {
        fn schema_version(&self) -> u32 { 1 }
        fn priority(&self) -> MessagePriority { MessagePriority::Normal }
    }

    impl Event for UserLoggedIn {
        type TypeId = type_ids::EchoTypeId;
    }

    // Mock transport that verifies trait bounds compile
    struct MockTransport;

    impl Transport for MockTransport {
        async fn send<R: Request>(&self, _request: R) -> CatgaResult<R::Response> {
            unimplemented!("mock")
        }

        async fn send_command<C: Command>(&self, _command: C) -> CatgaResult<()> {
            unimplemented!("mock")
        }

        async fn publish<E: Event>(&self, _event: E) -> CatgaResult<()> {
            unimplemented!("mock")
        }

        async fn send_delayed<R: Request>(&self, _request: R, _delay: Duration) -> CatgaResult<R::Response> {
            unimplemented!("mock")
        }

        async fn send_command_delayed<C: Command>(&self, _command: C, _delay: Duration) -> CatgaResult<()> {
            unimplemented!("mock")
        }

        async fn publish_delayed<E: Event>(&self, _event: E, _delay: Duration) -> CatgaResult<()> {
            unimplemented!("mock")
        }
    }

    #[test]
    fn typed_transport_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<MockTransport>();
        assert_sync::<MockTransport>();
    }

    #[test]
    fn request_type_id() {
        assert_eq!(<Echo as Request>::TypeId::NAME, "Echo");
    }

    #[test]
    fn command_type_id() {
        assert_eq!(<Reset as Command>::TypeId::NAME, "Echo"); // shared for demo
    }

    #[test]
    fn event_type_id() {
        assert_eq!(<UserLoggedIn as Event>::TypeId::NAME, "Echo"); // shared for demo
    }

    #[test]
    fn mock_transport_implements_trait() {
        fn assert_transport<T: Transport>() {}
        assert_transport::<MockTransport>();
    }
}
