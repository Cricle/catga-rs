//! Registers a request handler with the mediator macro and sends a typed request.

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Mediator, Request, catga_handlers};

struct Double(u64);
impl catga_core::Message for Double {}
impl Request for Double {
    type Response = u64;
}
struct DoubleHandler;
#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, value: Double) -> CatgaResult<u64> {
        Ok(value.0 * 2)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mediator = Mediator::new(catga_handlers! { request Double => DoubleHandler }?);
    assert_eq!(mediator.send(Double(21)).await?, 42);
    Ok(())
}
