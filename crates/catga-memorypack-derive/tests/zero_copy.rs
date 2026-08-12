//! Compile-pass and behavioral tests for `#[memorypack(zero_copy)]` structs.
//!
//! Zero-copy structs borrow `&str` / `&[u8]` fields from the reader's buffer instead of
//! allocating; the expansion provides `MemoryPackDeserializeZeroCopy` instead of the owned
//! `MemoryPackDeserialize`.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::traits::MemoryPackDeserializeZeroCopy;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(zero_copy)]
struct Frame<'a> {
    title: &'a str,
    payload: &'a [u8],
}

#[test]
fn zero_copy_struct_borrows_its_fields_from_the_input_buffer() -> Result<(), MemoryPackError> {
    let frame = Frame {
        title: "hdr",
        payload: &[10, 20, 30],
    };
    let bytes = MemoryPackSerializer::serialize(&frame)?;

    let decoded: Frame = MemoryPackSerializer::deserialize_zero_copy(&bytes)?;
    assert_eq!(decoded, frame);

    let buffer = bytes.as_ptr_range();
    assert!(buffer.contains(&decoded.title.as_ptr()));
    assert!(buffer.contains(&decoded.payload.as_ptr()));
    Ok(())
}

#[test]
fn zero_copy_byte_slice_serializes_as_a_length_prefix_plus_raw_bytes() -> Result<(), MemoryPackError>
{
    let frame = Frame {
        title: "",
        payload: &[0xAB],
    };
    let bytes = MemoryPackSerializer::serialize(&frame)?;
    // Field count, empty string (`i32` zero), slice length 1, then the raw byte.
    assert_eq!(bytes, [2, 0, 0, 0, 0, 1, 0, 0, 0, 0xAB]);

    let decoded: Frame = MemoryPackSerializer::deserialize_zero_copy(&bytes)?;
    assert_eq!(decoded, frame);
    Ok(())
}

#[test]
fn zero_copy_decode_maps_empty_and_null_lengths_to_empty_borrows() -> Result<(), MemoryPackError> {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(2).expect("write field count");
    writer.write_i32(0).expect("write empty string");
    writer.write_i32(-1).expect("write null slice marker");
    let bytes = writer.into_bytes();

    let decoded: Frame = MemoryPackSerializer::deserialize_zero_copy(&bytes)?;
    assert_eq!(
        decoded,
        Frame {
            title: "",
            payload: &[]
        }
    );
    Ok(())
}

#[test]
fn zero_copy_decode_rejects_an_invalid_byte_slice_length() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(2).expect("write field count");
    writer.write_i32(0).expect("write empty string");
    writer.write_i32(-2).expect("write invalid slice length");
    let bytes = writer.into_bytes();

    let result = MemoryPackSerializer::deserialize_zero_copy::<Frame>(&bytes);
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(message))
            if message.contains("invalid zero-copy byte slice length")
    ));
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(zero_copy)]
struct Envelope<'a> {
    id: u32,
    #[memorypack(zero_copy)]
    frame: Frame<'a>,
}

#[test]
fn zero_copy_struct_supports_owned_fields_and_nested_zero_copy_fields()
-> Result<(), MemoryPackError> {
    let envelope = Envelope {
        id: 42,
        frame: Frame {
            title: "inner",
            payload: &[1, 2],
        },
    };
    let bytes = MemoryPackSerializer::serialize(&envelope)?;

    let decoded: Envelope = MemoryPackSerializer::deserialize_zero_copy(&bytes)?;
    assert_eq!(decoded, envelope);

    let buffer = bytes.as_ptr_range();
    assert!(buffer.contains(&decoded.frame.title.as_ptr()));
    Ok(())
}
