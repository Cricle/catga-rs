//! Builds and runs a two-step in-memory flow.

use catga_core::CatgaResult;
use catga_flow::Flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let result = Flow::new("checkout")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .run()
        .await;
    assert!(result.is_success());
    Ok(())
}
