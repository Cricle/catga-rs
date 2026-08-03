//! Versioned compact encoding for plain [`catga_core::flow::FlowState`] values.

use catga_core::codec::memorypack::MemoryPackSerializer;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::FlowState;

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
}
