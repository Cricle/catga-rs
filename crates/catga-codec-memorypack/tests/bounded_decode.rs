//! Regression coverage for Catga's bounded untrusted-frame decode path.

use catga_codec_memorypack::{
    MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackReader, MemoryPackSerializer,
    MemoryPackable,
};

#[derive(Debug, MemoryPackable)]
struct Inner {
    value: u8,
}

#[derive(Debug, MemoryPackable)]
struct Outer {
    inner: Inner,
}

fn limits() -> MemoryPackDecodeLimits {
    MemoryPackDecodeLimits::new(32, 16, 8, 4, 4).expect("test limits are valid")
}

#[test]
fn rejects_a_frame_before_decoding() {
    let error = MemoryPackSerializer::deserialize_bounded::<u8>(&[0; 33], limits())
        .expect_err("frame above the configured limit must be rejected");

    assert!(error.to_string().contains("frame"));
}

#[test]
fn rejects_a_collection_length_before_allocating() {
    let bytes = 5_i32.to_le_bytes();
    let mut reader = MemoryPackReader::new_bounded(&bytes, limits())
        .expect("small frame fits the configured frame limit");

    let error = Vec::<u8>::deserialize(&mut reader)
        .expect_err("declared collection above the configured item limit must be rejected");

    assert!(error.to_string().contains("collection"));
}

#[test]
fn rejects_a_string_before_allocating() {
    let mut bytes = (!9_i32).to_le_bytes().to_vec();
    bytes.extend_from_slice(&9_i32.to_le_bytes());
    let mut reader = MemoryPackReader::new_bounded(&bytes, limits())
        .expect("small frame fits the configured frame limit");

    let error = reader
        .read_string()
        .expect_err("declared string above the configured byte limit must be rejected");

    assert!(error.to_string().contains("string"));
}

#[test]
fn rejects_trailing_input() {
    let error = MemoryPackSerializer::deserialize_bounded::<u8>(&[7, 8], limits())
        .expect_err("bounded decode must consume the exact frame");

    assert!(error.to_string().contains("trailing"));
}

#[test]
fn default_deserialize_is_also_exact_and_bounded() {
    let error = MemoryPackSerializer::deserialize::<u8>(&[7, 8])
        .expect_err("the default public decoder must not accept trailing untrusted input");

    assert!(error.to_string().contains("trailing"));
}

#[test]
fn default_deserialize_rejects_a_frame_above_its_default_limit() {
    let bytes = vec![0_u8; MemoryPackDecodeLimits::default().max_frame_bytes() + 1];

    let error = MemoryPackSerializer::deserialize::<u8>(&bytes)
        .expect_err("the public default decoder must retain its frame limit");

    assert!(error.to_string().contains("frame"));
}

#[test]
fn zero_copy_deserialize_is_also_an_exact_bounded_frame_decoder() {
    let mut bytes =
        MemoryPackSerializer::serialize(&"memorypack").expect("test zero-copy string serializes");
    bytes.push(0);

    let error = MemoryPackSerializer::deserialize_zero_copy::<&str>(&bytes)
        .expect_err("zero-copy decoding must not accept trailing input");

    assert!(error.to_string().contains("trailing"));
}

#[test]
fn rejects_invalid_boolean_wire_values() {
    let mut reader = MemoryPackReader::new_bounded(&[2], limits())
        .expect("small frame fits the configured frame limit");

    let error = reader
        .read_bool()
        .expect_err("boolean wire values must be exactly zero or one");

    assert!(error.to_string().contains("boolean"));
}

#[test]
fn accounts_for_collection_capacity_before_allocating() {
    let bytes = 3_i32.to_le_bytes();
    let mut reader = MemoryPackReader::new_bounded(
        &bytes,
        MemoryPackDecodeLimits::new(32, 16, 8, 4, 4).expect("test limits are valid"),
    )
    .expect("small frame fits the configured frame limit");

    let error = Vec::<u64>::deserialize(&mut reader)
        .expect_err("declared capacity above the allocation budget must be rejected");

    assert!(error.to_string().contains("allocation"));
}

#[test]
fn rejects_derived_structs_beyond_the_nesting_limit() {
    let bytes = MemoryPackSerializer::serialize(&Outer {
        inner: Inner { value: 7 },
    })
    .expect("test record serializes");
    let limits = MemoryPackDecodeLimits::new(32, 16, 8, 4, 1).expect("test limits are valid");

    let error = MemoryPackSerializer::deserialize_bounded::<Outer>(&bytes, limits)
        .expect_err("nested derived object above the configured depth must be rejected");

    assert!(error.to_string().contains("nesting"));
}
