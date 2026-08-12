//! Compile-pass and behavioral tests for derived named, tuple, and unit structs.
//!
//! The wire frame for structs is a `u8` field count followed by the field values in wire
//! order; skipped fields are excluded from the count and decode as `Default::default()`.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct Point {
    x: i32,
    y: i32,
}

#[test]
fn named_struct_uses_field_count_prefix_and_declaration_order() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Point { x: 1, y: 2 })?;
    assert_eq!(bytes, [2, 1, 0, 0, 0, 2, 0, 0, 0]);

    let decoded: Point = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Point { x: 1, y: 2 });
    Ok(())
}

#[test]
fn named_struct_rejects_a_frame_with_a_different_field_count() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(3).expect("write field count");
    writer.write_i32(1).expect("write x");
    writer.write_i32(2).expect("write y");

    let result = MemoryPackSerializer::deserialize::<Point>(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(message))
            if message.contains("field count mismatch")
    ));
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct Reordered {
    /// Doc attributes precede the `memorypack` one during attribute scanning.
    #[memorypack(order = 1)]
    first: u8,
    #[memorypack(order = 0)]
    second: u8,
}

#[test]
fn order_attribute_controls_the_wire_position() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Reordered {
        first: 9,
        second: 7,
    })?;
    assert_eq!(bytes, [2, 7, 9]);

    let decoded: Reordered = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(
        decoded,
        Reordered {
            first: 9,
            second: 7
        }
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct OrderFallback {
    #[memorypack(order)]
    bare: u8,
    #[memorypack(order = not_a_number)]
    malformed: u8,
}

#[test]
fn order_attributes_without_a_usable_value_keep_declaration_order() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&OrderFallback {
        bare: 4,
        malformed: 6,
    })?;
    assert_eq!(bytes, [2, 4, 6]);

    let decoded: OrderFallback = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(
        decoded,
        OrderFallback {
            bare: 4,
            malformed: 6
        }
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct BareFieldAttribute {
    #[memorypack]
    value: u8,
}

#[test]
fn a_bare_memorypack_field_attribute_is_ignored() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&BareFieldAttribute { value: 6 })?;
    assert_eq!(bytes, [1, 6]);

    let decoded: BareFieldAttribute = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, BareFieldAttribute { value: 6 });
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, MemoryPackable)]
struct WithSkipped {
    kept: u32,
    #[memorypack(skip)]
    skipped: u32,
    #[memorypack(ignore)]
    ignored: String,
    _cached: u32,
}

#[test]
fn skip_ignore_and_underscore_fields_leave_the_frame_and_decode_as_default()
-> Result<(), MemoryPackError> {
    let value = WithSkipped {
        kept: 0x0102_0304,
        skipped: 99,
        ignored: "scratch".to_owned(),
        _cached: 7,
    };
    let bytes = MemoryPackSerializer::serialize(&value)?;
    assert_eq!(bytes, [1, 4, 3, 2, 1]);

    let decoded: WithSkipped = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(
        decoded,
        WithSkipped {
            kept: 0x0102_0304,
            ..WithSkipped::default()
        }
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct Pair(u8, u16);

#[test]
fn tuple_struct_uses_field_count_prefix_and_positional_order() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Pair(0xAB, 0x0102))?;
    assert_eq!(bytes, [2, 0xAB, 0x02, 0x01]);

    let decoded: Pair = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Pair(0xAB, 0x0102));
    Ok(())
}

#[test]
fn tuple_struct_rejects_a_frame_with_a_different_field_count() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(1).expect("write field count");
    writer.write_u8(0xAB).expect("write payload");

    let result = MemoryPackSerializer::deserialize::<Pair>(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(message))
            if message.contains("field count mismatch")
    ));
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct Marker;

#[test]
fn unit_struct_encodes_as_an_empty_frame() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Marker)?;
    assert!(bytes.is_empty());

    let decoded: Marker = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Marker);
    Ok(())
}
