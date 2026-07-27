//! Runs a local flow with explicit forward and compensating operations.

use catga_core::CatgaResult;
use catga_flow::Flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let result = Flow::new("checkout")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .run()
        .await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 2);
    Ok(())
}
