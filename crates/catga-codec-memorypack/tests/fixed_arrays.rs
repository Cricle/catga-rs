//! Fixed-size array support through the `MemoryPackable` derive macro.

use catga_codec_memorypack::{MemoryPackDecodeLimits, MemoryPackSerializer, MemoryPackable};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct FixedArrayRecord {
    values: [u16; 3],
}

#[test]
fn derived_record_round_trips_fixed_array_fields() {
    let value = FixedArrayRecord {
        values: [7, 11, 13],
    };

    let bytes = MemoryPackSerializer::serialize(&value).expect("fixed array serializes");
    let decoded: FixedArrayRecord =
        MemoryPackSerializer::deserialize(&bytes).expect("fixed array deserializes");

    assert_eq!(decoded, value);
}

#[test]
fn bounded_decode_rejects_fixed_array_header_above_item_limit() {
    let bytes = [1, 5, 0, 0, 0];
    let limits = MemoryPackDecodeLimits::new(32, 32, 32, 4, 8).expect("limits are valid");

    let error = MemoryPackSerializer::deserialize_bounded::<FixedArrayRecord>(&bytes, limits)
        .expect_err("declared fixed array length must obey the item limit");

    assert!(error.to_string().contains("collection"));
}
