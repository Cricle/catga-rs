//! Contracts for local Flow steps with a shared cloneable context.

use std::sync::{Arc, Mutex};

use catga_core::CatgaResult;
use catga_flow::Flow;

#[tokio::test]
async fn local_flow_clones_context_for_forward_and_compensation_actions() -> CatgaResult<()> {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let flow = Flow::new("context")
        .step_with(
            Arc::clone(&trace),
            |trace| async move {
                trace.lock().expect("trace lock").push("reserve");
                Ok(())
            },
            |trace| async move {
                trace.lock().expect("trace lock").push("release");
                Ok(())
            },
        )
        .step_with(
            Arc::clone(&trace),
            |trace| async move {
                trace.lock().expect("trace lock").push("charge");
                Err(catga_core::CatgaError::new(
                    catga_core::ErrorCode::Unavailable,
                    "payment declined",
                ))
            },
            |trace| async move {
                trace.lock().expect("trace lock").push("refund");
                Ok(())
            },
        );

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(
        trace.lock().expect("trace lock").as_slice(),
        ["reserve", "charge", "release"]
    );
    Ok(())
}
