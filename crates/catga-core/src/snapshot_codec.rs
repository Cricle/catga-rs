//! State serialization contracts for persistent snapshot stores.

use crate::CatgaResult;

/// Converts one concrete snapshot state type to and from durable bytes.
pub trait SnapshotCodec<S>: Send + Sync {
    /// Serializes immutable aggregate state for persistence.
    fn encode_state(&self, state: &S) -> CatgaResult<Vec<u8>>;

    /// Restores aggregate state from persisted bytes.
    fn decode_state(&self, bytes: &[u8]) -> CatgaResult<S>;
}
