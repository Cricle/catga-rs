//! Store-internal `CNR1` record framing for corrupt- and legacy-record injection tests.

/// Magic prefix of the store-internal record envelope.
pub const PREFIX: &[u8; 4] = b"CNR1";

/// Total header size: magic prefix plus the 16-byte write token.
pub const HEADER_BYTES: usize = PREFIX.len() + 16;

/// Frames a payload with the store-internal `CNR1` record envelope.
pub fn framed(payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(HEADER_BYTES + payload.len());
    value.extend_from_slice(PREFIX);
    value.extend_from_slice(&[0x5Au8; 16]);
    value.extend_from_slice(payload);
    value
}
