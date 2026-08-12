//! Compile-pass and behavioral tests for `#[repr(...)]` forms and attribute tolerance.
//!
//! `#[repr(i32)]` marks a C-like enum, `#[repr(transparent)]` only switches to bare-`i32`
//! framing for a single-`i32` tuple newtype, and unknown `#[memorypack(...)]` options are
//! ignored so newer options stay forward-compatible.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, MemoryPackable)]
#[repr(i32)]
enum Color {
    Red = 0,
    Green = 5,
    Blue = -2,
}

#[test]
fn repr_i32_enum_round_trips_as_its_discriminant() -> Result<(), MemoryPackError> {
    for color in [Color::Red, Color::Green, Color::Blue] {
        let bytes = MemoryPackSerializer::serialize(&color)?;
        assert_eq!(bytes.len(), 4);
        let decoded: Color = MemoryPackSerializer::deserialize(&bytes)?;
        assert_eq!(decoded, color);
    }

    let green = MemoryPackSerializer::serialize(&Color::Green)?;
    assert_eq!(green, [5, 0, 0, 0]);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[repr(C)]
struct Pod {
    a: u8,
    b: u8,
}

#[test]
fn repr_c_struct_uses_the_regular_struct_frame() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Pod { a: 1, b: 2 })?;
    assert_eq!(bytes, [2, 1, 2]);

    let decoded: Pod = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Pod { a: 1, b: 2 });
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[repr(transparent)]
struct Wrapper {
    value: u64,
}

#[test]
fn transparent_named_struct_without_an_i32_field_uses_the_regular_frame()
-> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Wrapper { value: 0x0102 })?;
    assert_eq!(bytes, [1, 0x02, 0x01, 0, 0, 0, 0, 0, 0]);

    let decoded: Wrapper = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Wrapper { value: 0x0102 });
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[repr(transparent)]
struct TransparentMarker;

#[test]
fn transparent_unit_struct_encodes_as_an_empty_frame() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&TransparentMarker)?;
    assert!(bytes.is_empty());

    let decoded: TransparentMarker = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, TransparentMarker);
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[repr(align(8))]
struct Aligned {
    value: u8,
}

#[test]
fn repr_align_struct_uses_the_regular_struct_frame() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&Aligned { value: 3 })?;
    assert_eq!(bytes, [1, 3]);

    let decoded: Aligned = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, Aligned { value: 3 });
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(unknown = 1, other_unknown)]
struct UnknownOptionList {
    value: u8,
}

#[test]
fn unknown_memorypack_options_with_values_and_commas_are_ignored() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&UnknownOptionList { value: 5 })?;
    assert_eq!(bytes, [1, 5]);

    let decoded: UnknownOptionList = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, UnknownOptionList { value: 5 });
    Ok(())
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(unknown_option)]
struct UnknownOption {
    value: u8,
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(custom = 1)]
struct UnknownOptionWithValue {
    value: u8,
}

#[test]
fn unknown_memorypack_options_are_ignored() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&UnknownOption { value: 8 })?;
    assert_eq!(bytes, [1, 8]);
    let decoded: UnknownOption = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, UnknownOption { value: 8 });

    let bytes = MemoryPackSerializer::serialize(&UnknownOptionWithValue { value: 9 })?;
    assert_eq!(bytes, [1, 9]);
    let decoded: UnknownOptionWithValue = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, UnknownOptionWithValue { value: 9 });
    Ok(())
}
