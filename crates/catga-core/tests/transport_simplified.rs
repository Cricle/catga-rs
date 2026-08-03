//! Tests for simplified Transport trait with TypeId pattern.

#![allow(dead_code)]

use std::time::Duration;

use catga_core::{
    Command, Event, Message, MessagePriority, MessageTypeId, Request, Transport,
};

mod __catga_types {
    pub struct EchoTypeId;
    impl catga_core::MessageTypeId for EchoTypeId {
        const NAME: &'static str = "Echo";
    }

    pub struct ResetTypeId;
    impl catga_core::MessageTypeId for ResetTypeId {
        const NAME: &'static str = "Reset";
    }

    pub struct UserLoggedInTypeId;
    impl catga_core::MessageTypeId for UserLoggedInTypeId {
        const NAME: &'static str = "UserLoggedIn";
    }
}

#[derive(Clone, Debug)]
struct Echo(String);

impl Message for Echo {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }
}

impl Request for Echo {
    type Response = String;
    type TypeId = __catga_types::EchoTypeId;
}

#[derive(Clone, Debug)]
struct Reset;

impl Message for Reset {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::High
    }
}

impl Command for Reset {
    type TypeId = __catga_types::ResetTypeId;
}

#[derive(Clone, Debug)]
struct UserLoggedIn {
    user_id: u64,
}

impl Message for UserLoggedIn {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }
}

impl Event for UserLoggedIn {
    type TypeId = __catga_types::UserLoggedInTypeId;
}

/// Mock transport that always returns Ok for testing trait bounds.
struct MockTransport;

impl Transport for MockTransport {
    async fn send<R: Request>(&self, _request: R) -> catga_core::CatgaResult<R::Response> {
        // Cannot construct R::Response generically, so this is a compile-time proof of trait bounds
        // Actual implementation would be in concrete transports
        unimplemented!("mock transport")
    }

    async fn send_command<C: Command>(&self, _command: C) -> catga_core::CatgaResult<()> {
        unimplemented!("mock transport")
    }

    async fn publish<E: Event>(&self, _event: E) -> catga_core::CatgaResult<()> {
        unimplemented!("mock transport")
    }

    async fn send_delayed<R: Request>(
        &self,
        _request: R,
        _delay: Duration,
    ) -> catga_core::CatgaResult<R::Response> {
        unimplemented!("mock transport")
    }

    async fn send_command_delayed<C: Command>(
        &self,
        _command: C,
        _delay: Duration,
    ) -> catga_core::CatgaResult<()> {
        unimplemented!("mock transport")
    }

    async fn publish_delayed<E: Event>(
        &self,
        _event: E,
        _delay: Duration,
    ) -> catga_core::CatgaResult<()> {
        unimplemented!("mock transport")
    }
}

// --- Trait bounds tests ---

#[test]
fn transport_trait_has_send_sync_bounds() {
    fn assert_sync<T: Sync>() {}
    fn assert_send<T: Send>() {}
    assert_sync::<MockTransport>();
    assert_send::<MockTransport>();
}

#[test]
fn request_implements_message() {
    fn assert_message<M: Message>() {}
    assert_message::<Echo>();
}

#[test]
fn command_implements_message() {
    fn assert_message<M: Message>() {}
    assert_message::<Reset>();
}

#[test]
fn event_implements_message_and_clone() {
    fn assert_message<M: Message>() {}
    fn assert_clone<M: Clone>() {}
    assert_message::<UserLoggedIn>();
    assert_clone::<UserLoggedIn>();
}

// --- TypeId tests ---

#[test]
fn request_type_id_name() {
    assert_eq!(<Echo as Request>::TypeId::NAME, "Echo");
}

#[test]
fn command_type_id_name() {
    assert_eq!(<Reset as Command>::TypeId::NAME, "Reset");
}

#[test]
fn event_type_id_name() {
    assert_eq!(<UserLoggedIn as Event>::TypeId::NAME, "UserLoggedIn");
}

// --- Transport trait method signatures ---

#[test]
fn transport_send_accepts_request() {
    fn accepts_transport<T: Transport>() {}
    accepts_transport::<MockTransport>();
}

#[test]
fn transport_send_command_accepts_command() {
    // This is verified at the trait level - all Command types work with send_command
    fn accepts_command<C: Command>() {}
    accepts_command::<Reset>();
}

#[test]
fn transport_publish_accepts_event() {
    // This is verified at the trait level - all Event types work with publish
    fn accepts_event<E: Event + Clone>() {}
    accepts_event::<UserLoggedIn>();
}
