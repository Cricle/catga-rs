//! Compile-pass and dispatch test for `#[catga_handler]` on a trait impl block.
//!
//! The success expansion must re-emit the impl block as valid items; a regression emitted
//! `impl impl Handler<M> for H { .. }` (duplicated `impl`) inside a block expression with a
//! trailing type ascription, which can never compile.

use async_trait::async_trait;
use catga_core::{
    CatgaResult, DefaultMessageTypeId, Handler, Mediator, Message, Registry, Request, catga_handler,
};

struct Ping;

impl Message for Ping {}

impl Request for Ping {
    type Response = u64;
    type TypeId = DefaultMessageTypeId;
}

struct PingHandler;

#[catga_handler]
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<u64> {
        Ok(42)
    }
}

#[tokio::test]
async fn catga_handler_impl_dispatches_through_registry() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PingHandler)?;
    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Ping).await?, 42);
    Ok(())
}
