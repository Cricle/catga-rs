//! Versioned compact encoding for plain [`catga_flow::FlowState`] values.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::FlowState;

const FLOW_STATE_FORMAT_VERSION: u8 = 1;

/// Encodes a flow state with a version byte independent of Postcard's wire format.
pub(crate) fn encode_state(state: &FlowState) -> CatgaResult<Vec<u8>> {
    let payload = postcard::to_allocvec(state).map_err(|error| {
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
    postcard::from_bytes(payload).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot decode SQL flow state: {error}"),
        )
    })
}
