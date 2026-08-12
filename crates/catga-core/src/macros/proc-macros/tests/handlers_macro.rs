//! Behavioral contracts for the `catga_handlers!` registry builder macro.

#[path = "support/messages.rs"]
mod messages;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaResult, Mediator, command_handler, event_handler, request_handler};
use messages::{Ask, Bell, Log};

#[tokio::test]
async fn handlers_macro_registers_requests_commands_and_events() -> CatgaResult<()> {
    let handled_bells = Arc::new(AtomicUsize::new(0));
    let registry = catga_core::catga_handlers! {
        request Ask => request_handler(|ask: Ask| async move { Ok(ask.value + 1) });
        command Log => command_handler(|_log: Log| async move { Ok(()) });
        event Bell => [
            event_handler({
                let handled_bells = Arc::clone(&handled_bells);
                move |_: Bell| {
                    let handled_bells = Arc::clone(&handled_bells);
                    async move {
                        handled_bells.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            }),
            event_handler({
                let handled_bells = Arc::clone(&handled_bells);
                move |_: Bell| {
                    let handled_bells = Arc::clone(&handled_bells);
                    async move {
                        handled_bells.fetch_add(10, Ordering::SeqCst);
                        Ok(())
                    }
                }
            }),
        ];
    }?;

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Ask { value: 41 }).await?, 42);
    mediator.send_command(Log).await?;
    mediator.publish(Bell).await?;
    assert_eq!(handled_bells.load(Ordering::SeqCst), 11);
    Ok(())
}

#[tokio::test]
async fn handlers_macro_supports_empty_registration_lists() -> CatgaResult<()> {
    let registry = catga_core::catga_handlers! {}?;
    let _mediator = Mediator::new(registry);
    Ok(())
}
