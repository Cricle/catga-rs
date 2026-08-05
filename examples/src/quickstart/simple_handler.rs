//! Minimal handler example using explicit trait implementation.
//!
//! ```bash
//! cargo run --bin simple_handler
//! ```

use async_trait::async_trait;
use catga_core::{CatgaResult, CommandHandler, Handler, Mediator};

// Request with response type
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Command with no response
#[derive(catga_core::catga_command)]
struct Log(String);

// Handlers — explicit trait implementation
struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }
}

struct LogHandler;

#[async_trait]
impl CommandHandler<Log> for LogHandler {
    async fn handle(&self, msg: Log) -> CatgaResult<()> {
        println!("[log] {}", msg.0);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let registry = catga_core::catga_handlers! {
        request Double => DoubleHandler;
        command Log => LogHandler;
    }?;

    let mediator = Mediator::new(registry);

    let result = mediator.send(Double(21)).await?;
    println!("Double(21) = {}", result);
    assert_eq!(result, 42);

    mediator
        .send_command(Log("hello from simple handler!".to_string()))
        .await?;

    Ok(())
}
