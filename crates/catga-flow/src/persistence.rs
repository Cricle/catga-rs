//! Compact durable encoding for suspended flow continuations.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use serde::Deserialize;

use crate::{FlowContinuation, FlowState, WaitCondition, WaitPolicy, WaitResult};

const FORMAT_VERSION: u8 = 4;

#[derive(Deserialize)]
struct VersionThreeWaitResult {
    child_id: Box<str>,
    payload: Option<Arc<[u8]>>,
    error: Option<CatgaError>,
}

#[derive(Deserialize)]
struct VersionThreeWaitCondition {
    correlation_id: Box<str>,
    policy: WaitPolicy,
    expected_count: u32,
    results: Vec<VersionThreeWaitResult>,
    created_at: SystemTime,
    timeout: Duration,
}

impl VersionThreeWaitCondition {
    fn into_current(self) -> WaitCondition {
        WaitCondition::from_legacy(
            self.correlation_id,
            self.policy,
            self.expected_count,
            self.results
                .into_iter()
                .map(|result| {
                    WaitResult::from_legacy(result.child_id, result.payload, result.error)
                })
                .collect(),
            self.created_at,
            self.timeout,
        )
    }
}

#[derive(Deserialize)]
struct VersionOneContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<VersionThreeWaitCondition>,
    resume_at: Option<std::time::SystemTime>,
}

#[derive(Deserialize)]
struct VersionTwoContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<VersionThreeWaitCondition>,
    resume_at: Option<std::time::SystemTime>,
    schedule_id: Option<Box<str>>,
}

#[derive(Deserialize)]
struct VersionThreeContinuation {
    state: FlowState,
    step_name: Box<str>,
    wait: Option<VersionThreeWaitCondition>,
    resume_at: Option<SystemTime>,
    schedule_id: Option<Box<str>>,
    created_at: SystemTime,
}

/// Encodes a suspended flow continuation for a durable provider.
///
/// The emitted frame starts with the current format version (v4). Providers must store the
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
/// Versions 1 through 3 are migrated in memory. Versions 1 and 2 reconstruct their creation
/// time from the initial state heartbeat; version 3 has no durable child-launch intents and
/// therefore migrates them to an empty list. Unknown versions are rejected before Postcard
/// decoding instead of being mistaken for corrupt current data.
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
                    value.wait.map(VersionThreeWaitCondition::into_current),
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
                    value.wait.map(VersionThreeWaitCondition::into_current),
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
        3 => postcard::from_bytes::<VersionThreeContinuation>(payload)
            .map(|value| {
                let mut continuation = FlowContinuation::from_legacy(
                    value.state,
                    value.step_name,
                    value.wait.map(VersionThreeWaitCondition::into_current),
                    value.resume_at,
                    value.schedule_id,
                );
                continuation = continuation.with_created_at(value.created_at);
                continuation
            })
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::Internal,
                    format!("cannot decode v3 flow continuation: {error}"),
                )
            }),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            format!("unsupported flow continuation format version {version}"),
        )),
    }
}
