//! Compact versioned encoding shared by durable state-machine stores.

use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};

use super::StateMachineSnapshot;
use crate::memorypack::{TIME_WIRE_BYTES, decode_time_wire, encode_time_wire};

const FORMAT_VERSION: u8 = 1;
const VERSION_BYTES: usize = 1 + size_of::<i64>();
const METADATA_BYTES: usize = VERSION_BYTES + (TIME_WIRE_BYTES * 2);

/// Encodes one state-machine snapshot using the supplied state codec.
///
/// The initial Rust durable frame is `format version`, CAS version, creation time, update time,
/// and the exact caller-owned state payload. All durable state-machine stores persist this same
/// frame, so audit metadata adds no backend-specific SQL column or query.
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
    encode_time_wire(snapshot.created_at(), &mut value);
    encode_time_wire(snapshot.updated_at(), &mut value);
    value.extend_from_slice(&state);
    Ok(value)
}

/// Decodes one state-machine snapshot using the supplied state codec.
///
/// This first-release format has no legacy decoder. A frame must contain the complete current
/// metadata layout before its caller-owned state payload is decoded.
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
    let version = i64::from_be_bytes(value[1..VERSION_BYTES].try_into().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "state-machine snapshot version is malformed",
        )
    })?);
    let created_at = decode_time_wire(&value[VERSION_BYTES..VERSION_BYTES + TIME_WIRE_BYTES])
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("state-machine snapshot creation time is malformed: {error}"),
            )
        })?;
    let updated_at = decode_time_wire(&value[VERSION_BYTES + TIME_WIRE_BYTES..METADATA_BYTES])
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("state-machine snapshot update time is malformed: {error}"),
            )
        })?;
    StateMachineSnapshot::restore(
        instance_id,
        codec.decode_state(&value[METADATA_BYTES..])?,
        version,
        created_at,
        updated_at,
    )
}
