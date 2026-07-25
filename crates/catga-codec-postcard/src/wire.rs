use catga_core::{
    CatgaResult, DeliveryMode, Envelope, EnvelopeHeaders, MessageMetadata, MessagePriority,
    QualityOfService,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct HeaderWire {
    key: String,
    value: String,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct EnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    #[serde(default)]
    quality_of_service: QualityOfService,
    #[serde(default)]
    delivery_mode: DeliveryMode,
    #[serde(default)]
    priority: MessagePriority,
    #[serde(default)]
    not_before_unix_ms: Option<u64>,
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default)]
    headers: Vec<HeaderWire>,
    #[serde(default)]
    sent_at_unix_ms: Option<u64>,
}

/// The Postcard envelope layout written after headers but before sent-at support.
///
/// Like all Postcard structures, this positional layout needs a dedicated
/// decoder because Serde defaults cannot fill a missing trailing field.
#[derive(Deserialize)]
pub(crate) struct HeadersEnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: QualityOfService,
    delivery_mode: DeliveryMode,
    priority: MessagePriority,
    not_before_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<String>,
    headers: Vec<HeaderWire>,
}

/// The Postcard envelope layout written before application headers existed.
///
/// Postcard serializes structs as positional sequences, so `#[serde(default)]`
/// cannot recover an absent trailing field. The codec uses this exact layout
/// only after the current layout reaches EOF and verifies that it consumed the
/// complete input.
#[derive(Deserialize)]
pub(crate) struct LegacyEnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: QualityOfService,
    delivery_mode: DeliveryMode,
    priority: MessagePriority,
    not_before_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<String>,
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
            quality_of_service: envelope.metadata().quality_of_service(),
            delivery_mode: envelope.metadata().delivery_mode(),
            priority: envelope.metadata().priority(),
            not_before_unix_ms: envelope.metadata().not_before_unix_ms(),
            schema_version: envelope.schema_version(),
            reply_to: envelope.reply_to().map(str::to_owned),
            headers: envelope
                .headers()
                .map(|(key, value)| HeaderWire {
                    key: key.to_owned(),
                    value: value.to_owned(),
                })
                .collect(),
            sent_at_unix_ms: envelope.sent_at_unix_ms(),
        }
    }
}

impl TryFrom<EnvelopeWire> for Envelope {
    type Error = catga_core::CatgaError;

    fn try_from(wire: EnvelopeWire) -> CatgaResult<Self> {
        let EnvelopeWire {
            id,
            message_type,
            payload,
            message_id,
            correlation_id,
            quality_of_service,
            delivery_mode,
            priority,
            not_before_unix_ms,
            schema_version,
            reply_to,
            headers: wire_headers,
            sent_at_unix_ms,
        } = wire;
        let headers = EnvelopeHeaders::try_new(
            wire_headers
                .into_iter()
                .map(|header| (header.key, header.value)),
        )?;
        Ok(Envelope::from(LegacyEnvelopeWire {
            id,
            message_type,
            payload,
            message_id,
            correlation_id,
            quality_of_service,
            delivery_mode,
            priority,
            not_before_unix_ms,
            schema_version,
            reply_to,
        })
        .with_headers(headers)
        .with_sent_at_unix_ms(sent_at_unix_ms))
    }
}

impl TryFrom<HeadersEnvelopeWire> for Envelope {
    type Error = catga_core::CatgaError;

    fn try_from(wire: HeadersEnvelopeWire) -> CatgaResult<Self> {
        let HeadersEnvelopeWire {
            id,
            message_type,
            payload,
            message_id,
            correlation_id,
            quality_of_service,
            delivery_mode,
            priority,
            not_before_unix_ms,
            schema_version,
            reply_to,
            headers,
        } = wire;
        Envelope::try_from(EnvelopeWire {
            id,
            message_type,
            payload,
            message_id,
            correlation_id,
            quality_of_service,
            delivery_mode,
            priority,
            not_before_unix_ms,
            schema_version,
            reply_to,
            headers,
            sent_at_unix_ms: None,
        })
    }
}

impl From<LegacyEnvelopeWire> for Envelope {
    fn from(wire: LegacyEnvelopeWire) -> Self {
        let envelope = Self::versioned(
            wire.id,
            wire.message_type,
            wire.payload,
            MessageMetadata::new(wire.message_id, wire.correlation_id)
                .with_quality_of_service(wire.quality_of_service)
                .with_delivery_mode(wire.delivery_mode)
                .with_priority(wire.priority)
                .with_not_before_unix_ms(wire.not_before_unix_ms),
            wire.schema_version,
        );
        match wire.reply_to {
            Some(reply_to) => envelope.with_reply_to(reply_to),
            None => envelope,
        }
        .with_sent_at_unix_ms(None)
    }
}
