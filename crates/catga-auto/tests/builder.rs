//! Typed application facade contracts.

use async_trait::async_trait;
use catga_auto::AutoApp;
use catga_core::{CatgaResult, Handler, Message, Request};

#[derive(Clone)]
struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = &'static str;
}

struct PingHandler;
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<&'static str> {
        Ok("pong")
    }
}

#[tokio::test]
async fn auto_app_builds_a_typed_mediator_and_handle() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .register_request::<Ping, _>(PingHandler)?
        .build()?;

    assert_eq!(app.mediator().send(Ping).await?, "pong");
    assert_eq!(app.handle().send(Ping).await?, "pong");
    Ok(())
}

#[test]
fn duplicate_typed_registration_is_rejected_before_build() -> CatgaResult<()> {
    let mut builder = AutoApp::builder();
    builder.register_request::<Ping, _>(PingHandler)?;
    let error = match builder.register_request::<Ping, _>(PingHandler) {
        Ok(_) => panic!("duplicate request registration must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
    Ok(())
}

#[tokio::test]
async fn app_shutdown_is_explicit_and_does_not_spawn_a_hidden_task() {
    let app = AutoApp::builder().build().expect("empty app is valid");
    let shutdown = app.shutdown_token();
    app.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(1), shutdown.cancelled())
        .await
        .expect("explicit shutdown must cancel the app token");
}
