//! Registers a request handler and sends a typed request.
//!
//! Plain async functions automatically satisfy [`catga_core::Handler`] — no `#[async_trait]` or helper
//! wrappers needed. For handlers that need shared state, see `request_handler_with`.

use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Request};

#[derive(catga_core::Message)]
struct Double(u64);

impl Request for Double {
    type Response = u64;
}

// Plain async fn — Fn-blanket impl makes this a valid Handler.
async fn double_handler(value: Double) -> CatgaResult<u64> {
    Ok(value.0 * 2)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .request::<Double, _>(double_handler)?
        .build()?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
