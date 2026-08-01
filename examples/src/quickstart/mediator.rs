//! Registers a closure-backed request handler and sends a typed request.

use catga_auto::AutoApp;
use catga_core::{CatgaResult, Request, request_handler};

#[derive(catga_core::Message)]
struct Double(u64);

impl Request for Double {
    type Response = u64;
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .request(request_handler(
            |value: Double| async move { Ok(value.0 * 2) },
        ))?
        .build()?;
    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
