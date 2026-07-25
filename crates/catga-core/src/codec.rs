use crate::{CatgaResult, Envelope};

/// Encodes and decodes transport envelopes without coupling the core to a format.
pub trait EnvelopeCodec: Send + Sync {
    /// Converts an envelope to transport bytes.
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>>;

    /// Restores an envelope from transport bytes.
    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope>;
}
