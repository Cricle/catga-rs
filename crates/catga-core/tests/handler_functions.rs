//! Contracts for closure-backed typed handlers.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{
    CatgaResult, Command, Event, Mediator, Message, Registry, Request, command_handler,
    command_handler_with, event_handler, event_handler_with, request_handler, request_handler_with,
};

#[derive(Clone)]
struct Double(u64);

impl Message for Double {}

impl Request for Double {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Clone)]
struct Increment;

impl Message for Increment {}
impl Command for Increment {
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Clone)]
struct Counted;

impl Message for Counted {}
impl Event for Counted {
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[tokio::test]
async fn closure_handlers_register_through_the_typed_registry() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(request_handler(|request: Double| async move {
        Ok(request.0.saturating_mul(2))
    }))?;
    registry.register_command::<Increment, _>(command_handler({
        let commands = Arc::clone(&commands);
        move |_: Increment| {
            let commands = Arc::clone(&commands);
            async move {
                commands.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }))?;
    registry.register_event::<Counted, _>(event_handler({
        let events = Arc::clone(&events);
        move |_: Counted| {
            let events = Arc::clone(&events);
            async move {
                events.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }));

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Double(21)).await?, 42);
    mediator.send_command(Increment).await?;
    mediator.publish(Counted).await?;
    assert_eq!(commands.load(Ordering::Acquire), 1);
    assert_eq!(events.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn contextual_closure_handlers_keep_dependencies_explicit() -> CatgaResult<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(request_handler_with(
        Arc::clone(&calls),
        |calls, request: Double| async move {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(request.0.saturating_mul(2))
        },
    ))?;
    registry.register_command::<Increment, _>(command_handler_with(
        Arc::clone(&calls),
        |calls, _: Increment| async move {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        },
    ))?;
    registry.register_event::<Counted, _>(event_handler_with(
        Arc::clone(&calls),
        |calls, _: Counted| async move {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        },
    ));

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Double(21)).await?, 42);
    mediator.send_command(Increment).await?;
    mediator.publish(Counted).await?;
    assert_eq!(calls.load(Ordering::Acquire), 3);
    Ok(())
}
