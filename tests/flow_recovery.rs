use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_flow::{
    FlowContinuation, FlowDefinition, FlowRuntime, FlowScheduler, FlowState, FlowStepOutcome,
    MemoryFlowScheduler, SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;

#[tokio::test]
async fn runtime_rejects_a_continuation_owned_by_a_different_definition() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("wrong", "payments", b"input".to_vec(), "node-a").suspended(),
            "finish",
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("refunds")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    );

    assert!(runtime.resume("wrong").await.is_err());
}

#[tokio::test]
async fn memory_scheduler_rejects_duplicate_flow_resumes() {
    let scheduler = MemoryFlowScheduler::default();

    assert!(
        scheduler
            .schedule_resume("flow-15", SystemTime::now())
            .await
            .is_ok()
    );
    assert!(
        scheduler
            .schedule_resume("flow-15", SystemTime::now())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_resumes_return_current_state_instead_of_transient_errors() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::waiting(
            FlowState::new("race", "payment", b"input".to_vec(), "node-a").suspended(),
            "finish",
            WaitCondition::new(
                "race-wait",
                WaitPolicy::All,
                0,
                SystemTime::now(),
                Duration::from_secs(30),
            ),
        ))
        .await
        .unwrap();
    let runtime = Arc::new(FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment").step("finish", |_| async {
            tokio::task::yield_now().await;
            Ok(FlowStepOutcome::complete())
        }),
        "node-b",
    ));

    let first_runtime = Arc::clone(&runtime);
    let second_runtime = Arc::clone(&runtime);
    let first = tokio::spawn(async move { first_runtime.resume("race").await });
    let second = tokio::spawn(async move { second_runtime.resume("race").await });
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap().is_ok());
    assert!(second.unwrap().is_ok());
}

#[tokio::test]
async fn runtime_claims_an_abandoned_running_continuation_after_its_stale_timeout() {
    let store = Arc::new(MemorySuspendedFlows::default());
    store
        .create(FlowContinuation::new(
            FlowState::new("abandoned", "payment", b"input".to_vec(), "dead-node")
                .running()
                .heartbeated_at(SystemTime::UNIX_EPOCH),
            "finish",
        ))
        .await
        .unwrap();
    let runtime = FlowRuntime::new(
        store,
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("payment")
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-b",
    )
    .with_stale_after(Duration::ZERO);

    assert!(runtime.resume("abandoned").await.unwrap().is_success());
}
