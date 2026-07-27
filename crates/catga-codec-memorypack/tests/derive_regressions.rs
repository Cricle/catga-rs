//! Regression coverage for the `MemoryPackable` derive macro.

use catga_codec_memorypack::{MemoryPackError, MemoryPackSerializer, MemoryPackable};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct TwoFields {
    first: u8,
    second: u8,
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
#[memorypack(zero_copy)]
struct BorrowedBytes<'a> {
    bytes: &'a [u8],
    marker: u8,
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum ExplicitTags {
    #[tag = 17]
    First(u8),
    Second(u8),
}

#[test]
fn derived_object_rejects_a_mismatched_field_count() {
    let error = MemoryPackSerializer::deserialize::<TwoFields>(&[1, 9])
        .expect_err("a one-field frame must not decode as the two-field schema");

    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}

#[test]
fn zero_copy_borrowed_bytes_use_a_length_prefixed_binary_frame() {
    let value = BorrowedBytes {
        bytes: &[4, 5, 6],
        marker: 7,
    };

    let bytes = MemoryPackSerializer::serialize(&value)
        .expect("borrowed byte slices serialize with their binary framing");
    assert_eq!(bytes, [2, 3, 0, 0, 0, 4, 5, 6, 7]);

    let decoded = MemoryPackSerializer::deserialize_zero_copy::<BorrowedBytes<'_>>(&bytes)
        .expect("borrowed byte slices deserialize from their binary framing");
    assert_eq!(decoded, value);
    assert_eq!(decoded.bytes.as_ptr(), bytes[5..8].as_ptr());
}

#[test]
fn explicit_union_tags_drive_encoding_and_decoding() {
    let bytes = MemoryPackSerializer::serialize(&ExplicitTags::First(9))
        .expect("explicit union tags serialize");
    assert_eq!(bytes, [17, 9]);
    assert_eq!(
        MemoryPackSerializer::deserialize::<ExplicitTags>(&bytes)
            .expect("explicit union tags deserialize"),
        ExplicitTags::First(9)
    );
}

#[test]
fn invalid_union_tags_are_rejected_at_macro_expansion() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/duplicate_union_tags.rs");
    tests.compile_fail("tests/ui/out_of_range_union_tag.rs");
    tests.compile_fail("tests/ui/too_many_fields.rs");
}
