//! Compile-pass and dispatch tests for `#[catga_handler]` on command and event impls,
//! including qualified trait paths.

#[path = "support/messages.rs"]
mod messages;

use catga_core::{CatgaResult, CommandHandler, EventHandler, Mediator, Registry, catga_handler};
use messages::{Ask, Bell, Log};

struct LogHandler;

#[catga_handler]
#[async_trait::async_trait]
impl CommandHandler<Log> for LogHandler {
    async fn handle(&self, _log: Log) -> CatgaResult<()> {
        Ok(())
    }
}

struct BellHandler;

#[catga_handler]
#[async_trait::async_trait]
impl EventHandler<Bell> for BellHandler {
    async fn handle(&self, _event: Bell) -> CatgaResult<()> {
        Ok(())
    }
}

struct QualifiedAskHandler;

#[catga_handler]
#[async_trait::async_trait]
impl catga_core::Handler<Ask> for QualifiedAskHandler {
    async fn handle(&self, ask: Ask) -> CatgaResult<u64> {
        Ok(ask.value + 1)
    }
}

#[tokio::test]
async fn catga_handler_accepts_all_three_handler_traits() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ask, _>(QualifiedAskHandler)?;
    registry.register_command::<Log, _>(LogHandler)?;
    registry.register_event::<Bell, _>(BellHandler);

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Ask { value: 41 }).await?, 42);
    mediator.send_command(Log).await?;
    mediator.publish(Bell).await?;
    Ok(())
}
