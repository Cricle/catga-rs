//! Registers request handlers using `catga_handlers!` macro with AutoApp.
//!
//! ```bash
//! cargo run --example mediator
//! ```

use catga_core::CatgaResult;
use catga_core::auto::AutoApp;

// One-liner: #[catga_core::catga_request] auto-implements Message + Request
#[catga_core::catga_request(response = u64)]
struct Double(u64);

// Plain async fn
async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    // One-liner registration!
    let registry = catga_core::catga_handlers! {
        request Double => double_handler;
    }?;

    let app = AutoApp::from_registry(registry)?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
