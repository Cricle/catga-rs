//! Integration coverage for Catga's single local MemoryPack codec.

use std::sync::Arc;

use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    ErrorCode, Message, PayloadDecoder, PayloadEncoder, SnowflakeIdGenerator, SnowflakeLayout,
    TypedTransport,
};
use catga_memory::MemoryTransport;

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct Order {
    id: i64,
    reference: String,
    description: String,
}

impl Message for Order {}

#[test]
fn local_memorypack_codec_round_trips_and_rejects_trailing_input() {
    let codec = MemoryPackCodec::default();
    let order = Order {
        id: 42,
        reference: "A1".into(),
        description: "bounded payload".into(),
    };

    let encoded = <MemoryPackCodec as PayloadEncoder<Order>>::encode_payload(&codec, &order)
        .expect("MemoryPack payload encodes");
    let decoded = <MemoryPackCodec as PayloadDecoder<Order>>::decode_payload(&codec, &encoded)
        .expect("MemoryPack payload decodes");
    assert_eq!(decoded, order);

    let mut trailing = encoded;
    trailing.push(0);
    let error = <MemoryPackCodec as PayloadDecoder<Order>>::decode_payload(&codec, &trailing)
        .expect_err("untrusted payload must consume its exact frame");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn typed_transport_round_trips_local_memorypack_payloads() {
    let backend = Arc::new(MemoryTransport::new(1).expect("queue capacity is valid"));
    let ids = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("Snowflake configuration is valid"),
    );
    let transport = TypedTransport::new_with_codec(backend, ids, MemoryPackCodec::default());
    let order = Order {
        id: 91,
        reference: "TYPED-91".into(),
        description: "generic transport".into(),
    };

    transport
        .publish(&order)
        .await
        .expect("typed MemoryPack message publishes");
    let delivery = transport
        .receive::<Order>()
        .await
        .expect("typed MemoryPack message decodes");

    assert_eq!(delivery.message(), &order);
    delivery
        .acknowledge()
        .await
        .expect("typed MemoryPack acknowledgement succeeds");
}
