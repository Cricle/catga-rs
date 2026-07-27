//! Registers one typed request handler during startup and dispatches it explicitly.

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Mediator, Registry, Request};

struct Double(u64);

impl catga_core::Message for Double {}

impl Request for Double {
    type Response = u64;
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, request: Double) -> CatgaResult<u64> {
        Ok(request.0 * 2)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(DoubleHandler)?;

    let answer = Mediator::new(registry).send(Double(21)).await?;
    assert_eq!(answer, 42);
    Ok(())
}
