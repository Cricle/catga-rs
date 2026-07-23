use std::time::{Duration, SystemTime};

use catga_flow::{
    FlowContinuation, FlowState, SuspendedFlowStore, WaitCondition, WaitPolicy,
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
