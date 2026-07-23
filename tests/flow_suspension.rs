use std::time::{Duration, SystemTime};

use catga_flow::{FlowContinuation, FlowState, WaitCondition, WaitPolicy};

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
