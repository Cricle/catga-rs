//! Upstream crates.io MemoryPack payload adapter tests.

use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{PayloadDecoder, PayloadEncoder};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct UpstreamOrder {
    id: i64,
    reference: String,
}

#[test]
fn upstream_memorypack_codec_round_trips_a_derived_payload() {
    let codec = MemoryPackCodec::default();
    let order = UpstreamOrder {
        id: 42,
        reference: "upstream-memorypack".into(),
    };

    let bytes = <MemoryPackCodec as PayloadEncoder<UpstreamOrder>>::encode_payload(&codec, &order)
        .expect("derived payload encodes");
    let decoded =
        <MemoryPackCodec as PayloadDecoder<UpstreamOrder>>::decode_payload(&codec, &bytes)
            .expect("derived payload decodes");

    assert_eq!(decoded, order);
}
