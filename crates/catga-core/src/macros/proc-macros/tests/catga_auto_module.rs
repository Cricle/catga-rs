//! Behavioral contracts for `#[catga_auto]` module scanning: handler impl discovery,
//! registration propagation, and pass-through of non-handler items.

#[path = "support/messages.rs"]
mod messages;

pub use messages::*;

use catga_core::{CatgaResult, Mediator, Registry, request_handler};

#[catga_core::catga_auto]
pub mod discovered {
    pub struct AskHandler;

    #[async_trait::async_trait]
    impl catga_core::Handler<Ask> for AskHandler {
        async fn handle(&self, ask: Ask) -> catga_core::CatgaResult<u64> {
            Ok(ask.value + 1)
        }
    }

    pub struct LogHandler;

    #[async_trait::async_trait]
    impl catga_core::CommandHandler<Log> for LogHandler {
        async fn handle(&self, _log: Log) -> catga_core::CatgaResult<()> {
            Ok(())
        }
    }

    pub struct BellHandler;

    #[async_trait::async_trait]
    impl catga_core::EventHandler<Bell> for BellHandler {
        async fn handle(&self, _event: Bell) -> catga_core::CatgaResult<()> {
            Ok(())
        }
    }

    pub struct Helper;

    impl Helper {
        pub fn answer(&self) -> u32 {
            7
        }
    }

    impl std::fmt::Debug for Helper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Helper")
        }
    }

    pub fn plain_sync_fn(x: u32) -> u32 {
        x + 1
    }

    pub async fn argless_async() -> u32 {
        3
    }

    #[catga_core::catga_request(response = u64)]
    pub struct Probe {
        pub seed: u64,
    }

    pub async fn free_probe(probe: Probe) -> catga_core::CatgaResult<u64> {
        Ok(probe.seed + 100)
    }
}

#[tokio::test]
async fn auto_module_registers_discovered_handler_impls() -> CatgaResult<()> {
    let registry = discovered::__catga_auto_register(Registry::new())?;
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Ask { value: 41 }).await?, 42);
    assert_eq!(
        mediator.send(discovered::Probe { seed: 1 }).await?,
        101,
        "free async fns register through the real request_handler mechanism"
    );
    mediator.send_command(Log).await?;
    mediator.publish(Bell).await?;
    Ok(())
}

#[tokio::test]
async fn auto_module_reemits_non_handler_items_unchanged() {
    assert_eq!(discovered::plain_sync_fn(1), 2);
    assert_eq!(discovered::Helper.answer(), 7);
    assert_eq!(format!("{:?}", discovered::Helper), "Helper");
    assert_eq!(discovered::argless_async().await, 3);
}

#[tokio::test]
async fn auto_module_propagates_registration_conflicts() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry
        .register_request::<Ask, _>(request_handler(|ask: Ask| async move { Ok(ask.value) }))?;

    let result = discovered::__catga_auto_register(registry);
    assert!(result.is_err(), "duplicate Ask handlers must conflict");
    Ok(())
}
