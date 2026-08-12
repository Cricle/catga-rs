//! Deterministic envelope builder for transport and store tests.

use catga_core::{Envelope, MessageMetadata, QualityOfService};

/// Builds a small deterministic envelope with the given delivery guarantee.
pub fn envelope(id: u64, quality_of_service: QualityOfService) -> Envelope {
    Envelope::new(
        id,
        "nats.coverage",
        vec![u8::try_from(id).unwrap_or_default()],
        MessageMetadata::new(id, None).with_quality_of_service(quality_of_service),
    )
}
