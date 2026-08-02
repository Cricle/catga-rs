//! Demonstrates zero-boilerplate handler registration with catga-auto.
//!
//! This example shows how to use the new derive macros and global dispatch functions
//! for minimal boilerplate Catga applications.
//!
//! Run with: `cargo run -p catga-auto --example zero_boilerplate`

use catga_auto::{AutoApp, send};
use catga_core::{catga_request, CatgaResult};

/// Define a request message with the response type specified via attribute.
#[catga_request(response = String)]
struct GetUser(String);

/// Plain async fn handler - no trait impl needed!
async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("User {} found", msg.0))
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // Build the application with auto-discovered handlers
    // AutoApp::build() automatically binds the global dispatch mediator
    let app = AutoApp::builder()
        .handler(get_user_handler)?
        .build()?;

    // Use the global send function from anywhere
    let result = send(GetUser("123".into())).await?;
    println!("{}", result);

    app.shutdown();
    Ok(())
}
