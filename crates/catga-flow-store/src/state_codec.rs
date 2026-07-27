//! Versioned compact encoding for plain [`catga_flow::FlowState`] values.

use catga_codec_memorypack::MemoryPackSerializer;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::FlowState;

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
    MemoryPackSerializer::deserialize(payload).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot decode MemoryPack SQL flow state: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{decode_state, encode_state};
    use catga_flow::FlowState;

    #[test]
    fn state_frames_use_the_memorypack_format_version() {
        let state = FlowState::new("memorypack-version", "payment", [], "node-a");

        let encoded = encode_state(&state).expect("encode state");

        assert_eq!(encoded.first(), Some(&2));
    }

    #[test]
    fn state_decoder_rejects_trailing_bytes() {
        let state = FlowState::new("exact-frame", "payment", [], "node-a");
        let mut encoded = encode_state(&state).expect("encode state");
        encoded.push(0);

        assert!(decode_state(&encoded).is_err());
    }
}
