//! A valid `catga_handlers!` invocation builds a working registry.

use catga_core::{Mediator, command_handler, event_handler, request_handler};

#[catga_core::catga_request(response = u64)]
pub struct Ask {
    pub value: u64,
}

#[derive(catga_core::catga_command)]
pub struct Log;

#[derive(Clone, catga_core::catga_event)]
pub struct Bell;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let registry = catga_core::catga_handlers! {
        request Ask => request_handler(|ask: Ask| async move { Ok(ask.value + 1) });
        command Log => command_handler(|_log: Log| async move { Ok(()) });
        event Bell => [event_handler(|_bell: Bell| async move { Ok(()) })];
    }
    .expect("valid registrations build a registry");

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Ask { value: 41 }).await.unwrap(), 42);
    mediator.send_command(Log).await.unwrap();
    mediator.publish(Bell).await.unwrap();
}
