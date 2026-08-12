//! Behavioral tests for union variant resolution and frame format.
//
//! Exercises union tag resolution, duplicate detection (compile-fail path),
//! and round-trip serialization for all supported union configurations.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

// Test union with two explicit-tag variants
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum TwoVariantUnion {
    #[tag = 10]
    Value(i32),
    #[tag = 20]
    Text(String),
}

#[test]
fn two_variant_union_with_explicit_tags_round_trips() -> Result<(), MemoryPackError> {
    let cases = [
        TwoVariantUnion::Value(-1),
        TwoVariantUnion::Text("hello".to_owned()),
    ];
    for v in &cases {
        let bytes = MemoryPackSerializer::serialize(v)?;
        let decoded: TwoVariantUnion = MemoryPackSerializer::deserialize(&bytes)?;
        assert_eq!(&decoded, v);
    }
    Ok(())
}

#[test]
fn two_variant_union_frame_format() -> Result<(), MemoryPackError> {
    let val = TwoVariantUnion::Value(0x01020304);
    let bytes = MemoryPackSerializer::serialize(&val)?;
    assert_eq!(bytes, [10, 4, 3, 2, 1]);

    let txt = TwoVariantUnion::Text(String::new());
    let bytes = MemoryPackSerializer::serialize(&txt)?;
    assert_eq!(bytes, [20, 0, 0, 0, 0]);
    Ok(())
}

// Test union with auto-assigned tags (starting from 0)
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum AutoTagUnion {
    First(bool),
    Second(u32),
}

#[test]
fn auto_tag_union_assigns_tags_from_zero() -> Result<(), MemoryPackError> {
    let first = AutoTagUnion::First(true);
    let bytes = MemoryPackSerializer::serialize(&first)?;
    assert_eq!(bytes, [0, 1]);

    let second = AutoTagUnion::Second(0);
    let bytes = MemoryPackSerializer::serialize(&second)?;
    assert_eq!(bytes, [1, 0, 0, 0, 0]);

    Ok(())
}

// Test union with mixed explicit and auto-assigned tags
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum MixedTagUnion {
    #[tag = 100]
    Explicit(u8),
    Auto(String),
}

#[test]
fn mixed_explicit_and_auto_tags() -> Result<(), MemoryPackError> {
    let explicit = MixedTagUnion::Explicit(99);
    let bytes = MemoryPackSerializer::serialize(&explicit)?;
    assert_eq!(bytes, [100, 99]);

    let auto = MixedTagUnion::Auto("x".to_owned());
    let bytes = MemoryPackSerializer::serialize(&auto)?;
    // Tag = 1 (ordinal position), then the string "x"
    assert_eq!(bytes[0], 1);
    Ok(())
}

// Test union with struct payload
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct InnerStruct {
    x: i32,
    y: i32,
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum StructPayloadUnion {
    #[tag = 5]
    Point(InnerStruct),
    #[tag = 6]
    Label(String),
}

#[test]
fn union_with_struct_payload_round_trips() -> Result<(), MemoryPackError> {
    let pt = StructPayloadUnion::Point(InnerStruct { x: 1, y: 2 });
    let bytes = MemoryPackSerializer::serialize(&pt)?;
    // Tag 5, then struct frame: field count (2), x, y
    assert_eq!(bytes, [5, 2, 1, 0, 0, 0, 2, 0, 0, 0]);
    let decoded: StructPayloadUnion = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, pt);
    Ok(())
}

// Test union with bool and option payloads
#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum OptionPayloadUnion {
    #[tag = 1]
    Flag(bool),
    #[tag = 2]
    Count(u64),
}

#[test]
fn union_with_bool_and_u64_payloads() -> Result<(), MemoryPackError> {
    let flag = OptionPayloadUnion::Flag(true);
    let bytes = MemoryPackSerializer::serialize(&flag)?;
    assert_eq!(bytes, [1, 1]);

    let count = OptionPayloadUnion::Count(255);
    let bytes = MemoryPackSerializer::serialize(&count)?;
    assert_eq!(bytes, [2, 255, 0, 0, 0, 0, 0, 0, 0]);
    Ok(())
}

// Test rejection of unknown union tag (runtime behavior)
#[test]
fn unknown_union_tag_returns_error() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(9).expect("write tag");
    writer.write_u8(0).expect("write payload");

    let result: Result<TwoVariantUnion, _> =
        MemoryPackSerializer::deserialize(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(msg)) if msg.contains("Unknown union tag")
    ));
}
