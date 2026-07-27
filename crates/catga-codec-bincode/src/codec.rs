use bincode_next::{Decode, Encode, config};
use catga_core::{CatgaError, CatgaResult, ErrorCode, PayloadDecoder, PayloadEncoder};

/// Maximum complete Bincode frame accepted or emitted by [`BincodeCodec`].
pub const MAX_BINCODE_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// A bounded Bincode-next codec for statically typed Catga payloads.
///
/// The codec uses Bincode's native derive contracts rather than its optional Serde compatibility
/// layer. It rejects oversized input before decoding and requires the decoder to consume exactly
/// the supplied frame, preventing trailing bytes from being silently accepted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BincodeCodec;

impl<T> PayloadEncoder<T> for BincodeCodec
where
    T: Encode,
{
    fn encode_payload(&self, value: &T) -> CatgaResult<Vec<u8>> {
        bincode_next::encode_to_vec(
            value,
            config::standard().with_limit::<MAX_BINCODE_FRAME_BYTES>(),
        )
        .map_err(map_bincode_error)
    }
}

impl<T> PayloadDecoder<T> for BincodeCodec
where
    T: Decode<()>,
{
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<T> {
        if bytes.len() > MAX_BINCODE_FRAME_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Bincode payload exceeds the configured frame limit",
            ));
        }
        let (value, consumed) = bincode_next::decode_from_slice(
            bytes,
            config::standard().with_limit::<MAX_BINCODE_FRAME_BYTES>(),
        )
        .map_err(map_bincode_error)?;
        if consumed != bytes.len() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Bincode payload contains trailing bytes",
            ));
        }
        Ok(value)
    }
}

fn map_bincode_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}
