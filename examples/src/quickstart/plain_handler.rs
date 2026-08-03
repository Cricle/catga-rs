//! Demonstrates the simplest handler registration pattern enabled by Fn-blanket impls.
//!
//! Plain async functions automatically satisfy [`catga_core::Handler`] without `#[async_trait]` or helper
//! wrappers. The handler type is inferred by the registry:

use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Request};

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
// Handlers — plain async fns, no macros, no #[async_trait]
// ---------------------------------------------------------------------------

async fn ping_handler(_: Ping) -> CatgaResult<String> {
    Ok("pong".to_string())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    // Plain async fn as handler — Fn-blanket impl makes this work.
    let app = AutoApp::builder()
        .request::<Ping, _>(ping_handler)?
        .build()?;

    let response = app.mediator().send(Ping).await?;
    assert_eq!(response, "pong");
    println!("Ping handler responded: {response}");
    Ok(())
}
