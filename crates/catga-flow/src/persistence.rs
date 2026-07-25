//! Compact durable encoding for suspended flow continuations.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use serde::Deserialize;

use crate::{FlowContinuation, FlowState, WaitCondition};

const FORMAT_VERSION: u8 = 3;

#[derive(Deserialize)]
struct VersionOneContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<std::time::SystemTime>,
}

#[derive(Deserialize)]
struct VersionTwoContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<WaitCondition>,
    resume_at: Option<std::time::SystemTime>,
    schedule_id: Option<Box<str>>,
}

/// Encodes a suspended flow continuation for a durable provider.
///
/// The emitted frame starts with the current format version (v3). Providers must store the
/// complete frame unchanged.
pub fn encode_continuation(value: &FlowContinuation) -> CatgaResult<Vec<u8>> {
    let payload = postcard::to_allocvec(value).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot encode flow continuation: {error}"),
        )
    })?;
    let mut encoded = Vec::with_capacity(payload.len().saturating_add(1));
    encoded.push(FORMAT_VERSION);
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

/// Decodes a continuation previously produced by [`encode_continuation`].
///
/// Versions 1 and 2 are migrated in memory. Their creation time is reconstructed from the initial
/// state heartbeat because their layouts did not persist it. Unknown versions are rejected before
/// Postcard decoding instead of being mistaken for corrupt current data.
pub fn decode_continuation(bytes: &[u8]) -> CatgaResult<FlowContinuation> {
    let Some((&version, payload)) = bytes.split_first() else {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "flow continuation value is missing its format version",
        ));
    };
    match version {
        FORMAT_VERSION => postcard::from_bytes(payload).map_err(|error| {
            CatgaError::new(
                ErrorCode::Internal,
                format!("cannot decode flow continuation: {error}"),
            )
        }),
        1 => postcard::from_bytes::<VersionOneContinuation>(payload)
            .map(|value| {
                FlowContinuation::from_legacy(
                    value.state,
                    value.step_name,
                    value.wait,
                    value.resume_at,
                    None,
                )
            })
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::Internal,
                    format!("cannot decode v1 flow continuation: {error}"),
                )
            }),
        2 => postcard::from_bytes::<VersionTwoContinuation>(payload)
            .map(|value| {
                FlowContinuation::from_legacy(
                    value.state,
                    value.step_name,
                    value.wait,
                    value.resume_at,
                    value.schedule_id,
                )
            })
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::Internal,
                    format!("cannot decode v2 flow continuation: {error}"),
                )
            }),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            format!("unsupported flow continuation format version {version}"),
        )),
    }
}
