//! Integration coverage for Catga's MemoryPack-only codec surface.

use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackRpcResponse, MemoryPackSnapshotCodec, MemoryPackable,
};
use catga_core::{
    DeliveryMode, Envelope, EnvelopeCodec, EnvelopeHeaders, ErrorCode, MessageMetadata,
    MessagePriority, PayloadDecoder, PayloadEncoder, QualityOfService, SnapshotCodec,
};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct InventoryAdjustment {
    sku: String,
    quantity: i32,
}

#[test]
fn memorypack_codec_round_trips_a_complete_envelope() {
    let codec = MemoryPackCodec::default();
    let headers = EnvelopeHeaders::try_new([
        ("tenant".to_owned(), "acme".to_owned()),
        ("traceparent".to_owned(), "00-abc-def-01".to_owned()),
    ])
    .expect("test headers are valid");
    let envelope = Envelope::versioned(
        17,
        "inventory.adjusted",
        vec![7, 9],
        MessageMetadata::new(17, Some(5))
            .with_quality_of_service(QualityOfService::ExactlyOnce)
            .with_delivery_mode(DeliveryMode::AsyncRetry)
            .with_priority(MessagePriority::Critical)
            .with_not_before_unix_ms(Some(1_725_000_000_000)),
        4,
    )
    .with_reply_to("inventory.reply")
    .with_headers(headers)
    .with_sent_at_unix_ms(Some(1_725_000_000_111));

    let encoded = codec.encode(&envelope).expect("envelope encodes");
    let decoded = codec.decode(&encoded).expect("envelope decodes");

    assert_eq!(decoded, envelope);
}

#[test]
fn memorypack_payload_and_rpc_frames_reject_trailing_input() {
    let codec = MemoryPackCodec::default();
    let value = InventoryAdjustment {
        sku: "A-42".into(),
        quantity: -3,
    };
    let mut payload =
        <MemoryPackCodec as PayloadEncoder<InventoryAdjustment>>::encode_payload(&codec, &value)
            .expect("payload encodes");
    payload.push(0);

    let error =
        <MemoryPackCodec as PayloadDecoder<InventoryAdjustment>>::decode_payload(&codec, &payload)
            .expect_err("payload decoder must consume one exact frame");
    assert_eq!(error.code(), ErrorCode::Validation);

    let mut response = codec
        .encode_value(&MemoryPackRpcResponse::Success(value))
        .expect("response encodes");
    response.push(0);
    let error = codec
        .decode_rpc_response::<InventoryAdjustment>(&response)
        .expect_err("RPC decoder must consume one exact frame");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn memorypack_snapshot_codec_round_trips_a_typed_state() {
    let codec = MemoryPackSnapshotCodec::<InventoryAdjustment>::default();
    let state = InventoryAdjustment {
        sku: "SNAP-1".into(),
        quantity: 18,
    };

    let bytes = codec.encode_state(&state).expect("snapshot encodes");
    assert_eq!(codec.decode_state(&bytes).expect("snapshot decodes"), state);
}
