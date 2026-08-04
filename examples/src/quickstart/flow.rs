//! Builds and runs a two-step in-memory flow with explicit compensation handlers.

use catga_core::{CatgaResult, flow::Flow};

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let result = Flow::new("checkout")
        // The first closure performs the step; the second compensates it if a later step fails.
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .run()
        .await;
    assert!(result.is_success());
    println!("checkout completed {} steps", result.completed_steps());
    Ok(())
}
