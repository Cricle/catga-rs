//! Validation and versioned framing for durable DSL step progress.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::DslStepProgress;

/// Mirrors the maximum durable payload accepted by DSL parallel recovery frames.
pub(crate) const MAX_DSL_STEP_PROGRESS_PAYLOAD_BYTES: usize = 1024 * 1024;

const DSL_STEP_PROGRESS_FORMAT_VERSION: u8 = 1;

/// Validates the opaque payload before it reaches a database driver.
pub(crate) fn validate_progress(progress: &DslStepProgress) -> CatgaResult<()> {
    if progress.payload().len() > MAX_DSL_STEP_PROGRESS_PAYLOAD_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "DSL step progress payload exceeds the one-megabyte recovery limit",
        ));
    }
    Ok(())
}

/// Returns whether `next` is the sole valid successor to `expected` without wrapping at `i64::MAX`.
pub(crate) fn advances_version(expected: i64, next: i64) -> bool {
    expected.checked_add(1) == Some(next)
}

/// Encodes every public progress field so private checkpoint metadata round-trips exactly.
pub(crate) fn encode_progress(progress: &DslStepProgress) -> CatgaResult<Vec<u8>> {
    validate_progress(progress)?;
    let payload = postcard::to_allocvec(progress).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot encode SQL DSL step progress: {error}"),
        )
    })?;
    let mut frame = Vec::with_capacity(payload.len().saturating_add(1));
    frame.push(DSL_STEP_PROGRESS_FORMAT_VERSION);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decodes a progress frame and rejects oversized payloads from existing databases.
pub(crate) fn decode_progress(frame: &[u8]) -> CatgaResult<DslStepProgress> {
    let Some((&version, payload)) = frame.split_first() else {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "SQL DSL step progress is missing its format version",
        ));
    };
    if version != DSL_STEP_PROGRESS_FORMAT_VERSION {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            format!("unsupported SQL DSL step progress format version {version}"),
        ));
    }
    let progress = postcard::from_bytes(payload).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot decode SQL DSL step progress: {error}"),
        )
    })?;
    validate_progress(&progress)?;
    Ok(progress)
}

#[cfg(test)]
mod tests {
    use super::advances_version;

    #[test]
    fn maximum_version_cannot_be_its_own_successor() {
        assert!(!advances_version(i64::MAX, i64::MAX));
    }
}
