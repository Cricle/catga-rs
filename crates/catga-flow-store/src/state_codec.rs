//! Versioned compact encoding for plain [`catga_core::flow::FlowState`] values.

use catga_core::codec::memorypack::MemoryPackSerializer;
use catga_core::flow::FlowState;
use catga_core::{CatgaError, CatgaResult, ErrorCode};

const FLOW_STATE_FORMAT_VERSION: u8 = 2;

/// Encodes a flow state with a version byte independent of the MemoryPack wire format.
pub(crate) fn encode_state(state: &FlowState) -> CatgaResult<Vec<u8>> {
    let payload = MemoryPackSerializer::serialize(state).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot encode SQL flow state: {error}"),
        )
    })?;
    let mut frame = Vec::with_capacity(payload.len().saturating_add(1));
    frame.push(FLOW_STATE_FORMAT_VERSION);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decodes a state emitted by [`encode_state`].
pub(crate) fn decode_state(frame: &[u8]) -> CatgaResult<FlowState> {
    let Some((&version, payload)) = frame.split_first() else {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SQL flow state is missing its format version",
        ));
    };
    if version != FLOW_STATE_FORMAT_VERSION {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            format!("unsupported SQL flow state format version {version}"),
        ));
    }
    MemoryPackSerializer::deserialize_bounded(payload, FlowState::memorypack_decode_limits()?)
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("cannot decode MemoryPack SQL flow state: {error}"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use catga_core::flow::{FlowState, FlowStatus};

    #[test]
    fn flow_state_frames_round_trip_versioned_payloads() {
        let state = FlowState::new("flow-1", "checkout", vec![1, 2, 3], "worker-a");
        let frame = encode_state(&state).expect("encode flow state");
        assert_eq!(frame[0], 2);
        let decoded = decode_state(&frame).expect("decode flow state");
        assert_eq!(decoded.id(), "flow-1");
        assert_eq!(decoded.flow_type(), "checkout");
        assert_eq!(decoded.status(), FlowStatus::Running);
        assert_eq!(decoded.data(), &[1, 2, 3]);
    }

    #[test]
    fn flow_state_frames_reject_missing_unknown_and_corrupt_versions() {
        for frame in [Vec::new(), vec![1], vec![2, 0, 1, 2]] {
            assert_eq!(
                decode_state(&frame)
                    .expect_err("invalid flow state frame")
                    .code(),
                ErrorCode::Internal
            );
        }
    }

    #[test]
    fn flow_state_frames_reject_future_version() {
        // Version 3 does not exist yet
        let frame = vec![3, 0x80]; // minimal MemoryPack payload
        let err = decode_state(&frame).expect_err("future version rejected");
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
            let frame = encode_state(&state).expect("encode");
            let decoded = decode_state(&frame).expect("decode");
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

        let frame = encode_state(&state).expect("encode");
        let decoded = decode_state(&frame).expect("decode");

        assert_eq!(decoded.step(), 42);
        assert_eq!(decoded.version(), 1);
    }

    #[test]
    fn encode_decode_preserves_owner() {
        let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker-1");

        let frame = encode_state(&state).expect("encode");
        let decoded = decode_state(&frame).expect("decode");

        assert_eq!(decoded.owner(), Some("worker-1"));
    }

    #[test]
    fn encode_decode_preserves_null_owner() {
        let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker").suspended(); // Suspended clears owner

        let frame = encode_state(&state).expect("encode");
        let decoded = decode_state(&frame).expect("decode");

        assert_eq!(decoded.owner(), None);
    }

    #[test]
    fn encode_decode_preserves_data_with_max_size() {
        let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
        let state = FlowState::new("flow", "type", data.clone(), "worker");

        let frame = encode_state(&state).expect("encode");
        let decoded = decode_state(&frame).expect("decode");

        assert_eq!(decoded.data(), data.as_slice());
    }

    #[test]
    fn encode_decode_preserves_empty_data() {
        let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker");

        let frame = encode_state(&state).expect("encode");
        let decoded = decode_state(&frame).expect("decode");

        assert_eq!(decoded.data(), &[]);
    }

    #[test]
    fn encode_produces_version_byte_first() {
        let state = FlowState::new("flow", "type", Vec::<u8>::new(), "worker");
        let frame = encode_state(&state).expect("encode");

        assert_eq!(frame[0], FLOW_STATE_FORMAT_VERSION);
    }

    #[test]
    fn encode_produces_contiguous_frame() {
        let state = FlowState::new("flow", "type", vec![1, 2, 3], "worker");
        let frame = encode_state(&state).expect("encode");

        // Frame should be: version byte + MemoryPack payload
        // MemoryPack encodes the payload starting with type tag
        assert!(frame.len() >= 2, "frame should have version + payload");
    }

    #[test]
    fn decode_state_error_messages_are_descriptive() {
        // Empty frame
        let empty_err = decode_state(&[]).expect_err("empty");
        assert!(empty_err.message().contains("format version"));

        // Wrong version
        let wrong_err = decode_state(&[99]).expect_err("wrong version");
        assert!(wrong_err.message().contains("unsupported"));
    }
}
