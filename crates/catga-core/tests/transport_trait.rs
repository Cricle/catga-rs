//! Tests for transport trait

use std::time::Duration;

use catga_core::{
    CatgaResult, Command, Event, Message, MessagePriority, MessageTypeId, Request, Transport,
};

// TypeId markers for tests
mod type_ids {
    use catga_core::MessageTypeId;
    pub struct EchoTypeId;
    impl MessageTypeId for EchoTypeId {
        const NAME: &'static str = "Echo";
    }
}

#[derive(Clone, Debug)]
struct Echo;

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
    type TypeId = type_ids::EchoTypeId;
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
    type TypeId = type_ids::EchoTypeId;
}

#[derive(Clone, Debug)]
struct UserLoggedIn;

impl Message for UserLoggedIn {
    fn schema_version(&self) -> u32 {
        1
    }
    fn priority(&self) -> MessagePriority {
        MessagePriority::Normal
    }
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

    async fn send_delayed<R: Request>(
        &self,
        _request: R,
        _delay: Duration,
    ) -> CatgaResult<R::Response> {
        unimplemented!("mock")
    }

    async fn send_command_delayed<C: Command>(
        &self,
        _command: C,
        _delay: Duration,
    ) -> CatgaResult<()> {
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
