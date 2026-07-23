use catga_core::{Envelope, MessageMetadata};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(crate) struct EnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    #[serde(default = "default_schema_version")]
    schema_version: u32,
}

const fn default_schema_version() -> u32 {
    1
}

impl From<&Envelope> for EnvelopeWire {
    fn from(envelope: &Envelope) -> Self {
        Self {
            id: envelope.id(),
            message_type: envelope.message_type().to_owned(),
            payload: envelope.payload().to_vec(),
            message_id: envelope.metadata().message_id(),
            correlation_id: envelope.metadata().correlation_id(),
            schema_version: envelope.schema_version(),
        }
    }
}

impl From<EnvelopeWire> for Envelope {
    fn from(wire: EnvelopeWire) -> Self {
        Self::versioned(
            wire.id,
            wire.message_type,
            wire.payload,
            MessageMetadata::new(wire.message_id, wire.correlation_id),
            wire.schema_version,
        )
    }
}
