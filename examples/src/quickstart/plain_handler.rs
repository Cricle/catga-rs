//! Demonstrates the simplest handler registration pattern.
//!
//! Handlers implement `Handler`, `CommandHandler`, or `EventHandler` traits explicitly.

use async_trait::async_trait;
use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Handler, Request};

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

struct Ping;
impl catga_core::Message for Ping {}
impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

// ---------------------------------------------------------------------------
// Handlers — explicit trait implementation
// ---------------------------------------------------------------------------

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let app = AutoApp::builder()
        .request::<Ping, _>(PingHandler)?
        .build()?;

    let response = app.mediator().send(Ping).await?;
    assert_eq!(response, "pong");
    println!("Ping handler responded: {response}");
    Ok(())
}
