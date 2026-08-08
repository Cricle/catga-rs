use catga_core::flow::{FlowState, FlowStatus};
use catga_core::ErrorCode;

const FLOW_STATE_FORMAT_VERSION: u8 = 2;

#[test]
fn flow_state_frames_round_trip_versioned_payloads() {
    let state = FlowState::new("flow-1", "checkout", vec![1, 2, 3], "worker-a");
    let frame = super::encode_state(&state).expect("encode flow state");
    assert_eq!(frame[0], 2);
    let decoded = super::decode_state(&frame).expect("decode flow state");
    assert_eq!(decoded.id(), "flow-1");
    assert_eq!(decoded.flow_type(), "checkout");
    assert_eq!(decoded.status(), FlowStatus::Running);
    assert_eq!(decoded.data(), &[1, 2, 3]);
}

#[test]
fn flow_state_frames_reject_missing_unknown_and_corrupt_versions() {
    for frame in [Vec::new(), vec![1], vec![2, 0, 1, 2]] {
        assert_eq!(
            super::decode_state(&frame)
                .expect_err("invalid flow state frame")
                .code(),
            ErrorCode::Internal
        );
    }
}

#[test]
fn flow_state_frames_reject_future_version() {
    let frame = vec![3, 0x80];
    let err = super::decode_state(&frame).expect_err("future version rejected");
    assert_eq!(err.code(), ErrorCode::Internal);
    let message = err.message();
    assert!(message.contains("unsupported"));
    assert!(message.contains("3"));
}

#[test]
fn encode_decode_preserves_all_flow_statuses() {
    use catga_core::flow::FlowStatus;

    let statuses = [
        (
            FlowStatus::Running,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker"),
        ),
        (
            FlowStatus::Compensating,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker").compensating(),
        ),
        (
            FlowStatus::Suspended,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker").suspended(),
        ),
        (
            FlowStatus::Done,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker").done(5),
        ),
        (
            FlowStatus::Failed,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker").failed(
                catga_core::CatgaError::new(catga_core::ErrorCode::Internal, "test error"),
            ),
        ),
        (
            FlowStatus::Cancelled,
            FlowState::new("test-flow", "test-type", vec![1, 2, 3], "worker").cancelled(),
        ),
    ];

    for (expected_status, state) in statuses {
        let frame = super::encode_state(&state).expect("encode");
        let decoded = super::decode_state(&frame).expect("decode");
        assert_eq!(
            decoded.status(),
            expected_status,
            "status mismatch for {:?}",
            expected_status
        );
    }
}

#[test]
fn encode_decode_preserves_step_and_version() {
    let state = FlowState::new("flow", "type", Vec::<u8>::new(), "owner")
        .at_step(42)
        .next_version()
        .expect("next version");

    let frame = super::encode_state(&state).expect("encode");
    let decoded = super::decode_state(&frame).expect("decode");

    assert_eq!(decoded.step(), 42);
    assert_eq!(decoded.version(), 1);
}

#[test]
fn encode_decode_preserves_owner() {
    let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker-1");

    let frame = super::encode_state(&state).expect("encode");
    let decoded = super::decode_state(&frame).expect("decode");

    assert_eq!(decoded.owner(), Some("worker-1"));
}

#[test]
fn encode_decode_preserves_null_owner() {
    let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker").suspended();

    let frame = super::encode_state(&state).expect("encode");
    let decoded = super::decode_state(&frame).expect("decode");

    assert_eq!(decoded.owner(), None);
}

#[test]
fn encode_decode_preserves_data_with_max_size() {
    let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let state = FlowState::new("flow", "type", data.clone(), "worker");

    let frame = super::encode_state(&state).expect("encode");
    let decoded = super::decode_state(&frame).expect("decode");

    assert_eq!(decoded.data(), data.as_slice());
}

#[test]
fn encode_decode_preserves_empty_data() {
    let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker");

    let frame = super::encode_state(&state).expect("encode");
    let decoded = super::decode_state(&frame).expect("decode");

    assert_eq!(decoded.data(), &[]);
}

#[test]
fn encode_produces_version_byte_first() {
    let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker");
    let frame = super::encode_state(&state).expect("encode");

    assert_eq!(frame[0], FLOW_STATE_FORMAT_VERSION);
}

#[test]
fn encode_produces_contiguous_frame() {
    let state = FlowState::new("flow", "type", vec![1, 2, 3], "worker");
    let frame = super::encode_state(&state).expect("encode");

    assert!(frame.len() >= 2, "frame should have version + payload");
}

#[test]
fn decode_state_error_messages_are_descriptive() {
    let empty_err = super::decode_state(&[]).expect_err("empty");
    assert!(empty_err.message().contains("format version"));

    let wrong_err = super::decode_state(&[99]).expect_err("wrong version");
    assert!(wrong_err.message().contains("unsupported"));
}
