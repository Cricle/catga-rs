//! SHA-256 digest helpers producing fixed-width storage keys.
//!
//! Adapters hash caller-supplied identities (flow ids, stream ids, names)
//! into fixed-width key material. Three framing families exist and are
//! **not** interchangeable: changing the framing of an existing key family
//! changes persisted keys. New code should prefer [`sha256_framed_digest`]
//! for multi-part keys; the other helpers exist to keep legacy key families
//! byte-identical.

use sha2::{Digest, Sha256};

/// Hashes one byte string with SHA-256.
///
/// ```
/// let digest = catga_core::hash::sha256_digest(b"catga");
/// assert_eq!(digest.len(), 32);
/// ```
#[must_use]
pub fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Hashes the concatenation of `parts` with SHA-256, in order, without any
/// framing.
///
/// Callers that need unambiguous part boundaries must include explicit
/// length prefixes or fixed-width fields in `parts` themselves.
#[must_use]
pub fn sha256_concat_digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

/// Hashes `parts` with SHA-256, framing each part with its big-endian `u64`
/// length so part boundaries cannot alias.
///
/// `sha256_framed_digest(&[a, b])` never equals a digest produced by
/// splitting the same bytes at a different boundary.
///
/// ```
/// let digest = catga_core::hash::sha256_framed_digest(&[b"flow", b"state"]);
/// assert_ne!(
///     digest,
///     catga_core::hash::sha256_framed_digest(&[b"flows", b"tate"])
/// );
/// ```
#[must_use]
pub fn sha256_framed_digest(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}
