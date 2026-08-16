//! Strict contracts for the public message macros: `catga_request`, `catga_command`,
//! `catga_event`, and the `catga_handlers!` registry builder.

use std::any::TypeId;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{
    CatgaResult, ErrorCode, Mediator, Message, Registry, catga_command, catga_event,
    catga_handlers, catga_request, command_handler, event_handler, request_handler,
};

#[catga_request(response = u64)]
struct Double(u64);

#[catga_request(response = String)]
struct EchoText;

#[derive(catga_command)]
struct Reset(usize);

#[derive(Clone, catga_event)]
struct ResetApplied;

mod alpha {
    #[catga_core::catga_request(response = u8)]
    pub struct Ping(pub u8);
}

mod beta {
    #[catga_core::catga_request(response = u8)]
    pub struct Ping(pub u8);
}

#[test]
fn same_named_types_in_different_modules_get_independent_type_ids() {
    assert_ne!(
        TypeId::of::<alpha::Ping>(),
        TypeId::of::<beta::Ping>(),
        "dispatch keys on Rust type identity, so equal names cannot collide"
    );
}

#[test]
fn derived_messages_keep_the_default_message_metadata() {
    assert_eq!(Double(1).message_type(), std::any::type_name::<Double>());
    assert_eq!(
        EchoText.message_type(),
        std::any::type_name::<EchoText>(),
        "message_type reflects the concrete Rust type"
    );
    assert_ne!(Double(1).message_type(), EchoText.message_type());
    assert_eq!(Double(1).schema_version(), 1);
    assert_eq!(ResetApplied.schema_version(), 1);
}

#[tokio::test]
async fn derived_messages_dispatch_end_to_end() -> CatgaResult<()> {
    let resets = Arc::new(AtomicUsize::new(0));
    let applications = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(request_handler(|double: Double| async move {
        Ok(double.0 * 2)
    }))?;
    registry.register_request::<EchoText, _>(request_handler(|_: EchoText| async {
        Ok("echo".to_string())
    }))?;
    registry.register_command::<Reset, _>(command_handler({
        let resets = Arc::clone(&resets);
        move |reset: Reset| {
            let resets = Arc::clone(&resets);
            async move {
                resets.fetch_add(reset.0, Ordering::SeqCst);
                Ok(())
            }
        }
    }))?;
    registry.register_event::<ResetApplied, _>(event_handler({
        let applications = Arc::clone(&applications);
        move |_: ResetApplied| {
            let applications = Arc::clone(&applications);
            async move {
                applications.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }));
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Double(21)).await?, 42);
    assert_eq!(mediator.send(EchoText).await?, "echo");
    mediator.send_command(Reset(3)).await?;
    mediator.publish(ResetApplied).await?;
    assert_eq!(resets.load(Ordering::SeqCst), 3);
    assert_eq!(applications.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn same_named_types_route_to_their_own_handlers() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<alpha::Ping, _>(request_handler(
        |ping: alpha::Ping| async move { Ok(ping.0 + 1) },
    ))?;
    registry.register_request::<beta::Ping, _>(request_handler(|ping: beta::Ping| async move {
        Ok(ping.0 + 100)
    }))?;
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(alpha::Ping(1)).await?, 2);
    assert_eq!(mediator.send(beta::Ping(1)).await?, 101);
    Ok(())
}

#[tokio::test]
async fn derived_requests_and_commands_follow_the_single_handler_rule() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(request_handler(|double: Double| async move {
        Ok(double.0)
    }))?;
    let conflict = registry
        .register_request::<Double, _>(request_handler(
            |double: Double| async move { Ok(double.0) },
        ))
        .expect_err("a second derived request handler must conflict");
    assert_eq!(conflict.code(), ErrorCode::Conflict);

    registry.register_command::<Reset, _>(command_handler(|_: Reset| async { Ok(()) }))?;
    let conflict = registry
        .register_command::<Reset, _>(command_handler(|_: Reset| async { Ok(()) }))
        .expect_err("a second derived command handler must conflict");
    assert_eq!(conflict.code(), ErrorCode::Conflict);

    // Events intentionally accept any number of handlers.
    registry.register_event::<ResetApplied, _>(event_handler(|_: ResetApplied| async { Ok(()) }));
    registry.register_event::<ResetApplied, _>(event_handler(|_: ResetApplied| async { Ok(()) }));
    Ok(())
}

#[tokio::test]
async fn catga_handlers_builds_a_complete_routing_registry() -> CatgaResult<()> {
    let resets = Arc::new(AtomicUsize::new(0));
    let applications = Arc::new(AtomicUsize::new(0));
    let registry = catga_handlers! {
        request Double => request_handler(|double: Double| async move { Ok(double.0 * 2) });
        command Reset => command_handler({
            let resets = Arc::clone(&resets);
            move |reset: Reset| {
                let resets = Arc::clone(&resets);
                async move {
                    resets.fetch_add(reset.0, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        event ResetApplied => [
            event_handler({
                let applications = Arc::clone(&applications);
                move |_: ResetApplied| {
                    let applications = Arc::clone(&applications);
                    async move {
                        applications.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            }),
            event_handler({
                let applications = Arc::clone(&applications);
                move |_: ResetApplied| {
                    let applications = Arc::clone(&applications);
                    async move {
                        applications.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                }
            })
        ]
    }?;
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Double(5)).await?, 10);
    mediator.send_command(Reset(2)).await?;
    mediator.publish(ResetApplied).await?;
    assert_eq!(resets.load(Ordering::SeqCst), 2);
    assert_eq!(
        applications.load(Ordering::SeqCst),
        2,
        "every bracketed event handler is registered"
    );
    Ok(())
}
