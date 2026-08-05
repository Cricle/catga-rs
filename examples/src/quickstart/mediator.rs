//! Registers request handlers using `catga_handlers!` macro with AutoApp.
//!
//! ```bash
//! cargo run --example mediator
//! ```

use async_trait::async_trait;
use catga_core::CatgaResult;
use catga_core::{Handler, auto::AutoApp};

// One-liner: #[catga_core::catga_request] auto-implements Message + Request
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Handler struct
struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, value: Double) -> CatgaResult<u64> {
        Ok(value.0 * 2)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let registry = catga_core::catga_handlers! {
        request Double => DoubleHandler;
    }?;

    let app = AutoApp::from_registry(registry)?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
