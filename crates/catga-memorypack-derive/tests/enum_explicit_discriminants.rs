//! Compile-pass and behavioral tests for C-like enums with explicit discriminants.
//!
//! The derive's own error message promises "either `#[repr(i32)]` or explicit
//! discriminants", so an enum carrying an explicit discriminant on every variant must be
//! accepted without `#[repr(i32)]`.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, MemoryPackable)]
enum Status {
    Open = 1,
    Closed = 2,
    Archived = 7,
}

#[test]
fn enum_with_explicit_discriminants_round_trips() -> Result<(), MemoryPackError> {
    for status in [Status::Open, Status::Closed, Status::Archived] {
        let bytes = MemoryPackSerializer::serialize(&status)?;
        let decoded: Status = MemoryPackSerializer::deserialize(&bytes)?;
        assert_eq!(decoded, status);
    }
    Ok(())
}

#[test]
fn enum_deserialize_rejects_unknown_discriminant() {
    let mut writer = MemoryPackWriter::new();
    writer.write_i32(99).expect("write unknown discriminant");
    let result: Result<Status, _> = MemoryPackSerializer::deserialize(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(_))
    ));
}

#[test]
fn enum_rejects_negative_unknown_discriminant() {
    let mut writer = MemoryPackWriter::new();
    writer
        .write_i32(-9999)
        .expect("write unknown negative discriminant");
    let result: Result<Status, _> = MemoryPackSerializer::deserialize(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(msg))
            if msg.contains("Invalid discriminant") && msg.contains("-9999")
    ));
}

#[test]
fn enum_rejects_zero_when_not_a_variant() {
    // Status enum has values 1, 2, 7 — 0 is not valid
    let mut writer = MemoryPackWriter::new();
    writer.write_i32(0).expect("write zero");
    let result: Result<Status, _> = MemoryPackSerializer::deserialize(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(msg))
            if msg.contains("Invalid discriminant") && msg.contains("0")
    ));
}

// Test single-variant enum with explicit discriminant
#[derive(Clone, Copy, Debug, PartialEq, Eq, MemoryPackable)]
enum SingleVariant {
    Value = 42,
}

#[test]
fn single_variant_enum_round_trips() -> Result<(), MemoryPackError> {
    let bytes = MemoryPackSerializer::serialize(&SingleVariant::Value)?;
    assert_eq!(bytes, [42, 0, 0, 0]);
    let decoded: SingleVariant = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, SingleVariant::Value);
    Ok(())
}
