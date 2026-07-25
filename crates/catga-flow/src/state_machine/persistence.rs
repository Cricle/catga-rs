//! Compact versioned encoding shared by durable state-machine stores.

use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};

use super::StateMachineSnapshot;

const FORMAT_VERSION: u8 = 1;
const METADATA_BYTES: usize = 9;

/// Encodes one state-machine snapshot using the supplied state codec.
pub fn encode_state_machine_snapshot<S, C>(
    snapshot: &StateMachineSnapshot<S>,
    codec: &C,
) -> CatgaResult<Vec<u8>>
where
    C: SnapshotCodec<S>,
{
    if snapshot.version() < 0 {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "state-machine snapshot version cannot be negative",
        ));
    }
    let state = codec.encode_state(snapshot.state())?;
    let mut value = Vec::with_capacity(METADATA_BYTES.saturating_add(state.len()));
    value.push(FORMAT_VERSION);
    value.extend_from_slice(&snapshot.version().to_be_bytes());
    value.extend_from_slice(&state);
    Ok(value)
}

/// Decodes one state-machine snapshot using the supplied state codec.
pub fn decode_state_machine_snapshot<S, C>(
    instance_id: impl Into<Box<str>>,
    value: &[u8],
    codec: &C,
) -> CatgaResult<StateMachineSnapshot<S>>
where
    C: SnapshotCodec<S>,
{
    if value.len() < METADATA_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "state-machine snapshot value is missing metadata",
        ));
    }
    if value[0] != FORMAT_VERSION {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            format!(
                "unsupported state-machine snapshot format version {}",
                value[0]
            ),
        ));
    }
    let version = i64::from_be_bytes(value[1..METADATA_BYTES].try_into().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "state-machine snapshot version is malformed",
        )
    })?);
    StateMachineSnapshot::restore(
        instance_id,
        codec.decode_state(&value[METADATA_BYTES..])?,
        version,
    )
}
