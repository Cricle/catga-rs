use crate::{CatgaResult, Envelope};

/// Encodes and decodes transport envelopes without coupling the core to a format.
pub trait EnvelopeCodec: Send + Sync {
    /// Converts an envelope to transport bytes.
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>>;

    /// Restores an envelope from transport bytes.
    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope>;
}

/// Encodes one statically known application payload type for a typed transport.
///
/// This contract is intentionally separate from [`PayloadDecoder`]. A publisher only needs an
/// encoder, while a consumer only needs a decoder; joining both bounds would unnecessarily make
/// serialize-only outbound message types implement a decode contract.
pub trait PayloadEncoder<T: ?Sized>: Send + Sync {
    /// Encodes `value` into an owned payload frame.
    fn encode_payload(&self, value: &T) -> CatgaResult<Vec<u8>>;
}

/// Decodes one statically known application payload type for a typed transport.
///
/// Payload codecs are configured explicitly at both transport endpoints. Catga does not use
/// runtime type lookup or attach a format registry to envelopes.
pub trait PayloadDecoder<T>: Send + Sync {
    /// Decodes one exact payload frame into an owned application value.
    fn decode_payload(&self, bytes: &[u8]) -> CatgaResult<T>;
}
