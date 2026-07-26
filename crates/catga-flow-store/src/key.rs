//! Fixed-width identifiers for SQL indexes.

use sha2::{Digest, Sha256};

/// Hashes a caller-supplied identity into a fixed-width SQL primary-key value.
///
/// The source identity is stored and compared alongside this key, making a cryptographic hash
/// collision an explicit database error rather than an accidental alias.
pub(crate) fn flow_key(flow_id: &str) -> [u8; 32] {
    Sha256::digest(flow_id.as_bytes()).into()
}
