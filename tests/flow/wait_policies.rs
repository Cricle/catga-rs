//! Flow wait-policy integration helpers.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::flow::{
    FlowRuntime, MemoryFlowScheduler, WaitCondition, WaitPolicy,
    definition::{FlowDefinition, FlowStepOutcome},
};
use catga_core::memory::MemorySuspendedFlows;
use catga_core::{CatgaError, ErrorCode};

fn waiting_runtime(
    store: Arc<MemorySuspendedFlows>,
    scheduler: Arc<MemoryFlowScheduler>,
    name: &'static str,
    policy: WaitPolicy,
    expected_count: u32,
) -> FlowRuntime<MemorySuspendedFlows, MemoryFlowScheduler> {
    let condition = WaitCondition::new(
        format!("{name}-wait"),
        policy,
        expected_count,
        SystemTime::now(),
        Duration::from_secs(30),
    );
    let definition = FlowDefinition::new(name)
        .step("wait", move |_| {
            let condition = condition.clone();
            async move { Ok(FlowStepOutcome::wait(condition)) }
        })
        .step("finish", |_| async {
            tokio::task::yield_now().await;
            Ok(FlowStepOutcome::complete())
        });
    FlowRuntime::new(store, scheduler, definition, "node-a")
}

#[tokio::test]
async fn failed_children_follow_all_and_any_policy_semantics() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let all = waiting_runtime(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        "all-failure",
        WaitPolicy::All,
        2,
    );
    assert!(
        all.start("all-failure", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        all.record_wait_failure(
            "all-failure",
            "one",
            CatgaError::new(ErrorCode::Transient, "one failed"),
        )
        .await
        .unwrap()
        .is_failure()
    );

    let any = waiting_runtime(store, scheduler, "any-failure", WaitPolicy::Any, 2);
    assert!(
        any.start("any-failure", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        any.record_wait_failure(
            "any-failure",
            "one",
            CatgaError::new(ErrorCode::Transient, "one failed"),
        )
        .await
        .unwrap()
        .is_suspended()
    );
    assert!(
        any.record_wait_failure(
            "any-failure",
            "two",
            CatgaError::new(ErrorCode::Transient, "two failed"),
        )
        .await
        .unwrap()
        .is_failure()
    );
}

#[tokio::test]
async fn duplicate_child_notifications_return_an_existing_terminal_result() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = waiting_runtime(store, scheduler, "duplicate", WaitPolicy::Any, 2);
    assert!(
        runtime
            .start("duplicate", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        runtime
            .record_wait_success("duplicate", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_success()
    );

    assert!(
        runtime
            .record_wait_success("duplicate", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
async fn concurrent_child_notifications_do_not_leak_resume_contention() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let runtime = Arc::new(waiting_runtime(
        store,
        scheduler,
        "concurrent",
        WaitPolicy::All,
        2,
    ));
    assert!(
        runtime
            .start("concurrent", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );

    let first_runtime = Arc::clone(&runtime);
    let second_runtime = Arc::clone(&runtime);
    let first = tokio::spawn(async move {
        first_runtime
            .record_wait_success("concurrent", "one", b"one".to_vec())
            .await
    });
    let second = tokio::spawn(async move {
        second_runtime
            .record_wait_success("concurrent", "two", b"two".to_vec())
            .await
    });
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap().is_ok());
    assert!(second.unwrap().is_ok());
    assert!(runtime.resume("concurrent").await.unwrap().is_success());
}
