//! AutoApp builder and facade tests

use async_trait::async_trait;
use catga_core::auto::AutoAppBuilder;
use catga_core::{CatgaResult, Handler, Message, Registry, Request};

#[derive(Clone)]
struct Ping;

impl Message for Ping {}

impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
}

#[test]
fn with_registry_accepts_prebuilt_registry() {
    let registry = Registry::new();
    let builder = AutoAppBuilder::new().with_registry(registry);
    let _app = builder.build().expect("build with prebuilt registry");
}

#[tokio::test]
async fn auto_app_sends_request() -> CatgaResult<()> {
    let app = AutoAppBuilder::new()
        .request::<Ping, _>(PingHandler)?
        .build()?;
    let response = app.mediator().send(Ping).await?;
    assert_eq!(response, "pong");
    Ok(())
}
