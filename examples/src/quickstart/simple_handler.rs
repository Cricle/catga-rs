//! Minimal handler example using plain async functions.
//!
//! The Fn-blanket impl in `catga-core` allows plain async functions to satisfy
//! `Handler`, `CommandHandler`, and `EventHandler` without `#[async_trait]`.
//!
//! ```bash
//! cargo run --bin simple_handler
//! ```

use catga_core::{CatgaResult, Command, Mediator, Message, Registry, Request};

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Double(u64);
impl Message for Double {}
impl Request for Double {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Clone)]
struct Log(String);
impl Message for Log {}
impl Command for Log { type TypeId = catga_core::DefaultMessageTypeId; }

// ---------------------------------------------------------------------------
// Handlers — plain async fns, no macros, no #[async_trait]
// ---------------------------------------------------------------------------

// Plain async fn - no #[async_trait] needed!
async fn double_handler(msg: Double) -> CatgaResult<u64> {
    Ok(msg.0 * 2)
}

// Command handler — also just a plain async fn
async fn log_handler(msg: Log) -> CatgaResult<()> {
    println!("[log] {}", msg.0);
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mut registry = Registry::new();

    // Register plain async fns as handlers
    registry.register_request(double_handler)?;
    registry.register_command(log_handler)?;

    let mediator = Mediator::new(registry);

    // Use the mediator - requests use send(), commands use send_command()
    let result = mediator.send(Double(21)).await?;
    println!("Double(21) = {}", result);
    assert_eq!(result, 42);

    mediator
        .send_command(Log("hello from plain handler!".to_string()))
        .await?;

    Ok(())
}
