//! Fixed-width identifiers for SQL indexes.

use sha2::{Digest, Sha256};

/// Hashes a caller-supplied identity into a fixed-width SQL primary-key value.
///
/// The source identity is stored and compared alongside this key, making a cryptographic hash
/// collision an explicit database error rather than an accidental alias.
pub(crate) fn flow_key(flow_id: &str) -> [u8; 32] {
    Sha256::digest(flow_id.as_bytes()).into()
}

/// Hashes a two-part schedule target without allowing delimiter ambiguity.
pub(crate) fn schedule_target_key(flow_id: &str, state_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in [flow_id.as_bytes(), state_id.as_bytes()] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}
