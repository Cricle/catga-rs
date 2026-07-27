//! MemoryPack wire records for Catga transport envelopes.

use catga_core::{
    CatgaError, CatgaResult, DeliveryMode, Envelope, EnvelopeCodec, EnvelopeHeaders, ErrorCode,
    MessageMetadata, MessagePriority, QualityOfService,
};

use crate::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};

#[derive(MemoryPackable)]
struct HeaderWire {
    key: String,
    value: String,
}

#[derive(MemoryPackable)]
pub(crate) struct EnvelopeWire {
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
    sent_at_unix_ms: Option<u64>,
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
    type Error = CatgaError;

    fn try_from(wire: EnvelopeWire) -> CatgaResult<Self> {
        let headers = EnvelopeHeaders::try_new(
            wire.headers
                .into_iter()
                .map(|header| (header.key, header.value)),
        )?;
        let metadata = MessageMetadata::new(wire.message_id, wire.correlation_id)
            .with_quality_of_service(wire.quality_of_service)
            .with_delivery_mode(wire.delivery_mode)
            .with_priority(wire.priority)
            .with_not_before_unix_ms(wire.not_before_unix_ms);
        let envelope = Envelope::versioned(
            wire.id,
            wire.message_type,
            wire.payload,
            metadata,
            wire.schema_version,
        )
        .with_sent_at_unix_ms(wire.sent_at_unix_ms);
        Ok(match wire.reply_to {
            Some(reply_to) => envelope.with_reply_to(reply_to),
            None => envelope,
        }
        .with_headers(headers))
    }
}

impl EnvelopeCodec for crate::MemoryPackCodec {
    fn encode(&self, envelope: &Envelope) -> CatgaResult<Vec<u8>> {
        let bytes = MemoryPackSerializer::serialize(&EnvelopeWire::from(envelope))
            .map_err(map_memorypack_error)?;
        if bytes.len() > self.decode_limits().max_frame_bytes() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "MemoryPack envelope exceeds the configured frame limit",
            ));
        }
        Ok(bytes)
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<Envelope> {
        MemoryPackSerializer::deserialize_bounded::<EnvelopeWire>(bytes, self.decode_limits())
            .map_err(map_memorypack_error)
            .and_then(Envelope::try_from)
    }
}

impl MemoryPackSerialize for QualityOfService {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_u8(*self as u8)
    }
}

impl MemoryPackDeserialize for QualityOfService {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            value => Err(MemoryPackError::DeserializationError(format!(
                "invalid quality of service: {value}"
            ))),
        }
    }
}

impl MemoryPackSerialize for DeliveryMode {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_u8(*self as u8)
    }
}

impl MemoryPackDeserialize for DeliveryMode {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::WaitForResult),
            1 => Ok(Self::AsyncRetry),
            value => Err(MemoryPackError::DeserializationError(format!(
                "invalid delivery mode: {value}"
            ))),
        }
    }
}

impl MemoryPackSerialize for MessagePriority {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_u8(*self as u8)
    }
}

impl MemoryPackDeserialize for MessagePriority {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::Low),
            1 => Ok(Self::Normal),
            2 => Ok(Self::High),
            3 => Ok(Self::Critical),
            value => Err(MemoryPackError::DeserializationError(format!(
                "invalid message priority: {value}"
            ))),
        }
    }
}

fn map_memorypack_error(error: MemoryPackError) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}
