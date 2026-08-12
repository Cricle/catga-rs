//! Fixed-width identifiers for SQL indexes.

use catga_core::hash::{sha256_digest, sha256_framed_digest};

/// Hashes a caller-supplied identity into a fixed-width SQL primary-key value.
///
/// The source identity is stored and compared alongside this key, making a cryptographic hash
/// collision an explicit database error rather than an accidental alias.
pub(crate) fn flow_key(flow_id: &str) -> [u8; 32] {
    sha256_digest(flow_id.as_bytes())
}

/// Hashes a two-part schedule target without allowing delimiter ambiguity.
pub(crate) fn schedule_target_key(flow_id: &str, state_id: &str) -> [u8; 32] {
    sha256_framed_digest(&[flow_id.as_bytes(), state_id.as_bytes()])
}
