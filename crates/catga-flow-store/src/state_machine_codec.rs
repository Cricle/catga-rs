//! Size validation around the shared state-machine snapshot frame.

use catga_core::flow::{
    StateMachineSnapshot, decode_state_machine_snapshot, encode_state_machine_snapshot,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};

/// Maximum opaque state payload retained for one durable state-machine instance.
///
/// The cap prevents a single corrupt or untrusted state from consuming an unbounded allocation
/// while it crosses a database-driver boundary. The state-machine frame itself adds nine bytes of
/// fixed metadata.
pub(crate) const MAX_STATE_MACHINE_PAYLOAD_BYTES: usize = 1024 * 1024;

const STATE_MACHINE_FRAME_METADATA_BYTES: usize = 9;

/// Encodes a snapshot after enforcing the durable payload bound.
pub(crate) fn encode<S, C>(snapshot: &StateMachineSnapshot<S>, codec: &C) -> CatgaResult<Vec<u8>>
where
    C: SnapshotCodec<S>,
{
    let frame = encode_state_machine_snapshot(snapshot, codec)?;
    validate_frame(&frame, ErrorCode::Validation)?;
    Ok(frame)
}

/// Decodes a database frame only after enforcing the durable payload bound.
pub(crate) fn decode<S, C>(
    instance_id: impl Into<Box<str>>,
    frame: &[u8],
    codec: &C,
) -> CatgaResult<StateMachineSnapshot<S>>
where
    C: SnapshotCodec<S>,
{
    validate_frame(frame, ErrorCode::Internal)?;
    decode_state_machine_snapshot(instance_id, frame, codec)
}

fn validate_frame(frame: &[u8], code: ErrorCode) -> CatgaResult<()> {
    let max_frame_bytes = MAX_STATE_MACHINE_PAYLOAD_BYTES
        .checked_add(STATE_MACHINE_FRAME_METADATA_BYTES)
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "state-machine payload limit cannot be represented",
            )
        })?;
    if frame.len() > max_frame_bytes {
        return Err(CatgaError::new(
            code,
            "state-machine snapshot payload exceeds the one-megabyte durable limit",
        ));
    }
    Ok(())
}
