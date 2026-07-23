//! Macro ergonomics tests.

use async_trait::async_trait;
use catga_core::{CatgaResult, Event, EventHandler, Handler, Mediator, Message, Request};

#[derive(catga_core::Message)]
struct Ping;

impl Request for Ping {
    type Response = &'static str;
}

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<&'static str> {
        Ok("pong")
    }
}

#[derive(Clone, catga_core::Message)]
struct Notified;

impl Event for Notified {}

struct Noop;

#[async_trait]
impl EventHandler<Notified> for Noop {
    async fn handle(&self, _: Notified) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn derive_and_registration_macros_keep_setup_explicit_and_short() {
    let registry = catga_core::catga_handlers! {
        request Ping => PingHandler;
        event Notified => [Noop];
    };
    let mediator = Mediator::new(registry);

    assert_eq!(Ping.message_type(), "Ping");
    assert_eq!(mediator.send(Ping).await.unwrap(), "pong");
    mediator.publish(Notified).await.unwrap();
}
