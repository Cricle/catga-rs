//! Shared envelope fixture for Redis service-backed contract tests.

use catga_core::{Envelope, MessageMetadata};

/// Builds a minimal envelope with a deterministic payload and correlation.
pub fn envelope(id: u64, message_type: &str) -> Envelope {
    Envelope::new(
        id,
        message_type,
        vec![(id % 251) as u8, 2, 3],
        MessageMetadata::new(id, Some(id)),
    )
}
