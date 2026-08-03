use super::{
    error::MemoryPackError,
    limits::MemoryPackDecodeLimits,
    serializer::MemoryPackSerializer,
    traits::{MemoryPackDeserialize, MemoryPackSerialize},
};
use crate::{CatgaError, CatgaResult, ErrorCode, PayloadDecoder, PayloadEncoder};

/// Catga's statically typed adapter for the directly integrated crates.io MemoryPack source.
///
/// The codec uses upstream [`MemoryPackSerializer`] for every wire operation. Its only Catga
/// policy is a receive-side [`MemoryPackDecodeLimits`] budget and an equal outbound frame ceiling,
/// so a transport cannot enqueue a frame that a peer configured with the same codec would reject.
/// The outbound check runs after serialization and is a protocol compatibility guard, not an
/// allocation budget for application-owned values. No runtime type registry, reflection, or
/// background work is used.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPackCodec {
    decode_limits: MemoryPackDecodeLimits,
}

impl MemoryPackCodec {
    /// Creates a codec that applies `decode_limits` to every received payload frame.
    pub const fn new(decode_limits: MemoryPackDecodeLimits) -> Self {
        Self { decode_limits }
    }

    /// Returns the bounded receive policy applied before upstream deserialization allocates.
    pub const fn decode_limits(self) -> MemoryPackDecodeLimits {
        self.decode_limits
    }
}

impl Default for MemoryPackCodec {
    fn default() -> Self {
        Self::new(MemoryPackDecodeLimits::default())
    }
}

impl<T> PayloadEncoder<T> for MemoryPackCodec
where
    T: MemoryPackSerialize,
{
    fn encode_payload(&self, value: &T) -> CatgaResult<Vec<u8>> {
        let bytes = MemoryPackSerializer::serialize(value).map_err(map_memorypack_error)?;
        if bytes.len() > self.decode_limits.max_frame_bytes() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "MemoryPack payload exceeds the configured frame limit",
            ));
        }
        Ok(bytes)
    }
}

impl<T> PayloadDecoder<T> for MemoryPackCodec
where
    T: MemoryPackDeserialize,
{
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<T> {
        MemoryPackSerializer::deserialize_bounded(bytes, self.decode_limits)
            .map_err(map_memorypack_error)
    }
}

fn map_memorypack_error(error: MemoryPackError) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}
