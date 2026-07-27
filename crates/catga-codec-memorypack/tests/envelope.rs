//! Public envelope codec regression tests.

use catga_codec_memorypack::{
    MemoryPackCodec, MemoryPackDecodeLimits, MemoryPackSerializer, MemoryPackable,
};
use catga_core::{
    DeliveryMode, Envelope, EnvelopeCodec, EnvelopeHeaders, MessageMetadata, MessagePriority,
    QualityOfService,
};

#[derive(MemoryPackable)]
struct HeaderWire {
    key: String,
    value: String,
}

/// Matches the public envelope wire layout, except enum fields stay raw so malformed values can
/// be supplied without invoking the codec's enum serializers.
#[derive(MemoryPackable)]
struct InvalidEnumEnvelopeWire {
    id: u64,
    message_type: String,
    payload: Vec<u8>,
    message_id: u64,
    correlation_id: Option<u64>,
    quality_of_service: u8,
    delivery_mode: u8,
    priority: u8,
    not_before_unix_ms: Option<u64>,
    schema_version: u32,
    reply_to: Option<String>,
    headers: Vec<HeaderWire>,
    sent_at_unix_ms: Option<u64>,
}

fn full_envelope() -> Envelope {
    Envelope::versioned(
        7,
        "inventory.adjustment",
        vec![1, 2, 3, 4],
        MessageMetadata::new(11, Some(13))
            .with_quality_of_service(QualityOfService::ExactlyOnce)
            .with_delivery_mode(DeliveryMode::AsyncRetry)
            .with_priority(MessagePriority::Critical)
            .with_not_before_unix_ms(Some(17)),
        19,
    )
    .with_reply_to("inventory.reply")
    .with_headers(
        EnvelopeHeaders::try_new([
            ("tenant".to_owned(), "acme".to_owned()),
            ("trace".to_owned(), "trace-123".to_owned()),
        ])
        .expect("test headers are valid"),
    )
    .with_sent_at_unix_ms(Some(23))
}

#[test]
fn envelope_round_trips_every_metadata_field() {
    let codec = MemoryPackCodec::default();
    let envelope = full_envelope();
    let mut output = Vec::with_capacity(256);
    let capacity = output.capacity();

    codec
        .encode_into(&envelope, &mut output)
        .expect("envelope encodes into the caller buffer");
    assert_eq!(output.capacity(), capacity);

    let decoded = codec.decode(&output).expect("envelope decodes");

    assert_eq!(decoded.id(), envelope.id());
    assert_eq!(decoded.message_type(), envelope.message_type());
    assert_eq!(decoded.payload(), envelope.payload());
    assert_eq!(decoded.metadata(), envelope.metadata());
    assert_eq!(decoded.schema_version(), envelope.schema_version());
    assert_eq!(decoded.reply_to(), envelope.reply_to());
    assert_eq!(
        decoded.headers().collect::<Vec<_>>(),
        envelope.headers().collect::<Vec<_>>()
    );
    assert_eq!(decoded.sent_at_unix_ms(), envelope.sent_at_unix_ms());
}

#[test]
fn envelope_decode_rejects_an_invalid_enum_discriminant() {
    let bytes = MemoryPackSerializer::serialize(&InvalidEnumEnvelopeWire {
        id: 7,
        message_type: "inventory.adjustment".into(),
        payload: vec![],
        message_id: 11,
        correlation_id: None,
        quality_of_service: 99,
        delivery_mode: DeliveryMode::WaitForResult as u8,
        priority: MessagePriority::Normal as u8,
        not_before_unix_ms: None,
        schema_version: 1,
        reply_to: None,
        headers: vec![],
        sent_at_unix_ms: None,
    })
    .expect("test wire record encodes");

    let error = MemoryPackCodec::default()
        .decode(&bytes)
        .expect_err("invalid enum discriminants must be rejected");

    assert!(error.message().contains("quality of service"));
}

#[test]
fn envelope_decode_rejects_trailing_bytes() {
    let codec = MemoryPackCodec::default();
    let mut bytes = codec.encode(&full_envelope()).expect("envelope encodes");
    bytes.push(0);

    let error = codec
        .decode(&bytes)
        .expect_err("the envelope decoder must consume exactly one frame");

    assert!(error.message().contains("trailing"));
}

#[test]
fn envelope_decode_rejects_a_frame_above_its_limit() {
    let codec = MemoryPackCodec::new(
        MemoryPackDecodeLimits::new(8, 64, 64, 16, 8).expect("test limits are valid"),
    );
    let bytes = MemoryPackCodec::default()
        .encode(&full_envelope())
        .expect("envelope encodes under default limits");

    let error = codec
        .decode(&bytes)
        .expect_err("an oversized inbound envelope must be rejected before decoding");

    assert!(error.message().contains("frame"));
}
