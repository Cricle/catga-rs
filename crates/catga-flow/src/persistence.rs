//! Compact durable encoding for suspended flow continuations.

use catga_codec_memorypack::MemoryPackSerializer;
use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::FlowContinuation;

const FORMAT_VERSION: u8 = 7;

/// Encodes a suspended flow continuation for a durable provider.
///
/// The emitted frame starts with the current MemoryPack format version (v7). Providers must store the
/// complete frame unchanged.
pub fn encode_continuation(value: &FlowContinuation) -> CatgaResult<Vec<u8>> {
    let payload = MemoryPackSerializer::serialize(value).map_err(|error| {
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
/// The version identifies the MemoryPack wire contract. Earlier durable-frame versions are
/// deliberately rejected: callers must migrate durable records out of band before enabling this release.
/// Received payloads are decoded as one exact frame under the codec's default resource limits.
pub fn decode_continuation(bytes: &[u8]) -> CatgaResult<FlowContinuation> {
    let Some((&version, payload)) = bytes.split_first() else {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "flow continuation value is missing its format version",
        ));
    };
    if version != FORMAT_VERSION {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            format!("unsupported flow continuation format version {version}"),
        ));
    }
    MemoryPackSerializer::deserialize(payload).map_err(|error| {
        CatgaError::new(
            ErrorCode::Internal,
            format!("cannot decode MemoryPack flow continuation: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{decode_continuation, encode_continuation};
    use crate::{FlowContinuation, FlowState};

    #[test]
    fn continuation_codec_preserves_the_durable_compensation_stack() {
        let continuation = FlowContinuation::new(
            FlowState::new("payment-rollback", "payment", [], "node-a"),
            "charge",
        )
        .record_compensation("reserve")
        .expect("record reserve rollback")
        .record_compensation("charge")
        .expect("record charge rollback");

        let encoded = encode_continuation(&continuation).expect("encode continuation");
        let restored = decode_continuation(&encoded).expect("decode continuation");

        let steps: Vec<&str> = restored
            .compensation_steps()
            .iter()
            .map(AsRef::as_ref)
            .collect();
        assert_eq!(steps, ["reserve", "charge"]);
    }

    #[test]
    fn continuation_frames_use_the_memorypack_format_version() {
        let continuation = FlowContinuation::new(
            FlowState::new("memorypack-version", "payment", [], "node-a"),
            "charge",
        );

        let encoded = encode_continuation(&continuation).expect("encode continuation");

        assert_eq!(encoded.first(), Some(&7));
    }

    #[test]
    fn continuation_decoder_rejects_trailing_bytes() {
        let continuation = FlowContinuation::new(
            FlowState::new("exact-frame", "payment", [], "node-a"),
            "charge",
        );
        let mut encoded = encode_continuation(&continuation).expect("encode continuation");
        encoded.push(0);

        assert!(decode_continuation(&encoded).is_err());
    }
}
