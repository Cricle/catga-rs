//! Minimal handler example using `#[catga_core::catga_request]` and `catga_handlers!` macro.
//!
//! ```bash
//! cargo run --bin simple_handler
//! ```

use catga_core::{CatgaResult, Mediator};

// Request with response type
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Command with no response
#[derive(catga_core::catga_command)]
struct Log(String);

// Handlers — plain async fns, no #[async_trait] needed!
async fn double_handler(msg: Double) -> CatgaResult<u64> {
    Ok(msg.0 * 2)
}

async fn log_handler(msg: Log) -> CatgaResult<()> {
    println!("[log] {}", msg.0);
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    // One-liner registration: message => handler;
    let registry = catga_core::catga_handlers! {
        request Double => double_handler;
        command Log => log_handler;
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
