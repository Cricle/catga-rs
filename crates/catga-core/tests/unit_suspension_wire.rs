//! Unit tests for durable flow suspension wire types.

use std::time::{Duration, UNIX_EPOCH};

use catga_core::codec::memorypack::MemoryPackSerializer;
use catga_core::flow::suspension::{FlowContinuation, WaitCondition, WaitPolicy};
use catga_core::flow::FlowState;

#[test]
fn wait_condition_wire_round_trip_basic() {
    let wait = WaitCondition::for_children(
        "parent-42",
        WaitPolicy::All,
        ["child-a", "child-b"],
        UNIX_EPOCH,
        Duration::from_secs(30),
    )
    .expect("child identities are valid");

    let bytes = MemoryPackSerializer::serialize(&wait).expect("wait serializes");
    let deserialized: WaitCondition =
        MemoryPackSerializer::deserialize(&bytes).expect("wait deserializes");

    assert_eq!(deserialized.correlation_id(), "parent-42");
    assert_eq!(deserialized.policy(), WaitPolicy::All);
    assert_eq!(deserialized.expected_count(), 2);
}

#[test]
fn wait_condition_wire_round_trip_with_results() {
    let wait = WaitCondition::for_children(
        "parent-42",
        WaitPolicy::Any,
        ["child-1"],
        UNIX_EPOCH,
        Duration::from_secs(60),
    )
    .expect("child identities are valid")
    .record_success("child-1", [1_u8, 2, 3]);

    let bytes = MemoryPackSerializer::serialize(&wait).expect("wait with result serializes");
    let deserialized: WaitCondition =
        MemoryPackSerializer::deserialize(&bytes).expect("wait with result deserializes");

    assert_eq!(deserialized.correlation_id(), "parent-42");
    assert!(deserialized.results().len() <= 1);
}

#[test]
fn flow_continuation_wire_round_trip_suspended() {
    let state = FlowState::new("flow", "checkout", [9_u8], "worker").suspended();

    let continuation = FlowContinuation::waiting(
        state.clone(),
        "wait-payment",
        WaitCondition::for_children(
            "parent-42",
            WaitPolicy::All,
            ["child-a"],
            UNIX_EPOCH,
            Duration::from_secs(30),
        )
        .expect("valid"),
    );

    let bytes = MemoryPackSerializer::serialize(&continuation).expect("continuation serializes");
    let deserialized: FlowContinuation =
        MemoryPackSerializer::deserialize(&bytes).expect("continuation deserializes");

    assert_eq!(deserialized.step_name(), "wait-payment");
    assert!(deserialized.wait().is_some());
}

#[test]
fn flow_continuation_wire_round_trip_delayed() {
    let state = FlowState::new("flow", "retry-step", [9_u8], "worker");
    let resume_at = UNIX_EPOCH + Duration::from_secs(120);
    let continuation = FlowContinuation::new(state.clone(), "retry-step").delayed_until(resume_at);

    let bytes = MemoryPackSerializer::serialize(&continuation).expect("delayed continuation serializes");
    let deserialized: FlowContinuation =
        MemoryPackSerializer::deserialize(&bytes).expect("delayed continuation deserializes");

    assert_eq!(deserialized.step_name(), "retry-step");
    assert!(deserialized.resume_at().is_some());
}

#[test]
fn flow_state_wire_round_trip() {
    let state = FlowState::new("order-123", "processing", [1_u8, 2, 3], "worker");
    let bytes = MemoryPackSerializer::serialize(&state).expect("state serializes");
    let deserialized: FlowState = MemoryPackSerializer::deserialize(&bytes).expect("state deserializes");
    assert_eq!(deserialized.id(), "order-123");
}
