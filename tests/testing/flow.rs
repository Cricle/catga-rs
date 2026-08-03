//! Contract tests for typed Flow test contexts.

use catga_core::flow::{FlowDefinition, FlowRuntime, FlowStatus, FlowStepOutcome, SuspendedFlowStore};
use catga_testing::FlowTestContext;

#[tokio::test]
async fn flow_test_context_exposes_bounded_memory_flow_dependencies() {
    let context = FlowTestContext::new();
    let runtime = FlowRuntime::new(
        context.suspended_flows(),
        context.scheduler(),
        FlowDefinition::new("test-flow")
            .step("complete", |_| async { Ok(FlowStepOutcome::complete()) }),
        "test-node",
    );

    assert!(
        runtime
            .start("flow-1", b"input".to_vec())
            .await
            .expect("in-memory Flow runtime succeeds")
            .is_success()
    );
    let continuation = context
        .suspended_flows()
        .get("flow-1")
        .await
        .expect("memory continuation store is available")
        .expect("completed Flow remains available for inspection");
    assert_eq!(continuation.state().status(), FlowStatus::Done);
}
