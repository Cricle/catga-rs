//! Registers a closure-backed request handler and sends a typed request.

use catga_core::{CatgaResult, Mediator, Request, catga_handlers, request_handler};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mediator = Mediator::new(catga_handlers! {
        request Double => request_handler(|value: Double| async move { Ok(value.0 * 2) })
    }?);
    let result = mediator.send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("21 doubled is {result}");
    Ok(())
}
