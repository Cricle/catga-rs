use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_flow::{
    FlowContinuation, FlowDefinition, FlowRuntime, FlowState, FlowStepOutcome, MemoryFlowScheduler,
    SuspendedFlowStore, WaitCondition, WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;

#[test]
fn continuation_keeps_shared_input_and_wait_completion_is_immutable() {
    let state = FlowState::new("flow-12", "payment", b"input".to_vec(), "node-a");
    let continuation = FlowContinuation::waiting(
        state,
        "charge",
        WaitCondition::new(
            "wait-12",
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    let next = continuation
        .wait()
        .unwrap()
        .record_success("child-a", b"ok".to_vec());

    assert_eq!(continuation.step_name(), "charge");
    assert_eq!(next.completed_count(), 1);
    assert_eq!(next.expected_count(), 2);
    assert_eq!(continuation.state().data(), b"input");
}

#[tokio::test]
async fn concurrent_wait_results_are_not_lost() {
    let store = MemorySuspendedFlows::default();
    let continuation = FlowContinuation::waiting(
        FlowState::new("flow-12", "payment", b"input".to_vec(), "node-a"),
        "charge",
        WaitCondition::new(
            "wait-12",
            WaitPolicy::All,
            2,
            SystemTime::now(),
            Duration::from_secs(30),
        ),
    );
    assert!(store.create(continuation).await.unwrap());

    let first = store.record_wait_success("flow-12", 0, "child-a", b"a".to_vec());
    let second = store.record_wait_success("flow-12", 0, "child-b", b"b".to_vec());
    let (first, second) = tokio::join!(first, second);

    assert!(first.unwrap());
    assert!(second.unwrap());
    assert_eq!(
        store
            .get("flow-12")
            .await
            .unwrap()
            .unwrap()
            .wait()
            .unwrap()
            .completed_count(),
        2
    );
}

#[tokio::test]
async fn delayed_flow_persists_and_resumes_registered_steps() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let definition = FlowDefinition::new("payment")
        .step("reserve", |_| async {
            Ok(FlowStepOutcome::suspend_until(SystemTime::now()))
        })
        .step("charge", |_| async { Ok(FlowStepOutcome::complete()) });
    let runtime = FlowRuntime::new(store, Arc::clone(&scheduler), definition, "node-a");

    let suspended = runtime.start("flow-13", b"input".to_vec()).await.unwrap();
    assert!(suspended.is_suspended());
    assert_eq!(scheduler.take_due(SystemTime::now()).len(), 1);

    assert!(runtime.resume("flow-13").await.unwrap().is_success());
}

#[tokio::test]
async fn wait_policies_resume_once_and_expire_deterministically() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let all_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("all")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "all-wait",
                    WaitPolicy::All,
                    2,
                    SystemTime::now(),
                    Duration::from_secs(30),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        all_runtime
            .start("all", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        all_runtime
            .record_wait_success("all", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        all_runtime
            .record_wait_success("all", "two", b"two".to_vec())
            .await
            .unwrap()
            .is_success()
    );

    let any_runtime = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("any")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "any-wait",
                    WaitPolicy::Any,
                    2,
                    SystemTime::now(),
                    Duration::from_secs(30),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        any_runtime
            .start("any", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        any_runtime
            .record_wait_success("any", "one", b"one".to_vec())
            .await
            .unwrap()
            .is_success()
    );

    let expired_runtime = FlowRuntime::new(
        Arc::clone(&store),
        scheduler,
        FlowDefinition::new("expired")
            .step("wait", |_| async {
                Ok(FlowStepOutcome::wait(WaitCondition::new(
                    "expired-wait",
                    WaitPolicy::All,
                    1,
                    SystemTime::UNIX_EPOCH,
                    Duration::from_secs(1),
                )))
            })
            .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        "node-a",
    );
    assert!(
        expired_runtime
            .start("expired", b"input".to_vec())
            .await
            .unwrap()
            .is_suspended()
    );
    assert!(
        expired_runtime
            .resume_at("expired", SystemTime::now())
            .await
            .unwrap()
            .is_failure()
    );
}
