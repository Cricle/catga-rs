use std::sync::Arc;

use crate::CatgaResult;

/// Encodes cached idempotent responses without coupling core to a serialization format.
pub trait CachedResultCodec<T>: Send + Sync {
    /// Encodes one response for durable or in-memory storage.
    fn encode(&self, value: &T) -> CatgaResult<Arc<[u8]>>;

    /// Decodes a previously cached response.
    fn decode(&self, bytes: &[u8]) -> CatgaResult<T>;
}
