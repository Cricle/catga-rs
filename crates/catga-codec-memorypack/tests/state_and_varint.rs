//! Public wire-level tests for compact integers and reference state.

use catga_codec_memorypack::{
    MemoryPackDecodeLimits, MemoryPackError, MemoryPackReader, MemoryPackReaderOptionalState,
    MemoryPackWriter, MemoryPackWriterOptionalState,
    varint::{read_varint, write_varint},
};

fn decode_varint(frame: &[u8]) -> Result<i64, MemoryPackError> {
    let mut reader = MemoryPackReader::new_bounded(frame, MemoryPackDecodeLimits::default())?;
    read_varint(&mut reader)
}

#[test]
fn signed_varints_round_trip_every_compact_encoding_boundary() {
    for value in [
        i64::MIN,
        i32::MIN as i64,
        i32::MIN as i64 + 1,
        i16::MIN as i64,
        i16::MIN as i64 + 1,
        i8::MIN as i64,
        -121,
        -120,
        -1,
        0,
        127,
        128,
        i16::MAX as i64,
        i16::MAX as i64 + 1,
        i32::MAX as i64,
        i32::MAX as i64 + 1,
        i64::MAX,
    ] {
        let mut writer = MemoryPackWriter::new();
        write_varint(&mut writer, value).expect("value serializes as a compact integer");

        assert_eq!(
            decode_varint(writer.as_bytes()).expect("compact integer deserializes"),
            value
        );
    }
}

#[test]
fn varint_reader_accepts_unsigned_wire_widths() {
    assert_eq!(
        decode_varint(&[0x87, 255]).expect("BYTE value is valid"),
        255
    );
    assert_eq!(
        decode_varint(&[0x85, 0xff, 0xff]).expect("UINT16 value is valid"),
        u16::MAX as i64
    );
    assert_eq!(
        decode_varint(&[0x83, 0xff, 0xff, 0xff, 0xff]).expect("UINT32 value is valid"),
        u32::MAX as i64
    );
    assert_eq!(
        decode_varint(&[0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f])
            .expect("UINT64 value is valid"),
        i64::MAX
    );
}

#[test]
fn varint_reader_rejects_truncated_compact_values() {
    let error = decode_varint(&[0x80]).expect_err("INT64 tag requires eight payload bytes");
    assert!(matches!(error, MemoryPackError::Io(_)));
}

#[test]
fn writer_reference_state_is_stable_per_frame_and_resets() {
    let first = String::from("first");
    let second = String::from("second");
    let mut state = MemoryPackWriterOptionalState::new();

    assert_eq!(state.get_or_add_reference(&first), (false, 0));
    assert_eq!(state.get_or_add_reference(&first), (true, 0));
    assert_eq!(state.get_or_add_reference(&second), (false, 1));

    state.reset();
    assert_eq!(state.get_or_add_reference(&second), (false, 0));
}

#[test]
fn reader_reference_state_enforces_ids_types_and_updates() {
    let mut state = MemoryPackReaderOptionalState::new();
    state
        .add_object_reference(7, String::from("initial"))
        .expect("new reference id registers");
    assert_eq!(
        state
            .get_object_reference::<String>(7)
            .expect("matching type retrieves a clone"),
        "initial"
    );

    let duplicate = state
        .add_object_reference(7, String::from("duplicate"))
        .expect_err("duplicate ids are rejected");
    assert!(matches!(
        duplicate,
        MemoryPackError::DeserializationError(_)
    ));

    let wrong_type = state
        .get_object_reference::<u64>(7)
        .expect_err("a reference cannot be read through another type");
    assert!(matches!(
        wrong_type,
        MemoryPackError::DeserializationError(_)
    ));

    state
        .update_object_reference(7, String::from("updated"))
        .expect("registered ids update");
    assert_eq!(
        state
            .get_object_reference::<String>(7)
            .expect("updated value retrieves"),
        "updated"
    );

    let missing = state
        .update_object_reference(8, String::from("missing"))
        .expect_err("unknown ids cannot update");
    assert!(matches!(missing, MemoryPackError::DeserializationError(_)));

    state.reset();
    assert!(state.get_object_reference::<String>(7).is_err());
}

#[test]
fn stateful_writer_exposes_its_optional_reference_table() {
    let mut writer = MemoryPackWriter::new_with_state();
    let value = String::from("tracked");
    let state = writer
        .optional_state
        .as_mut()
        .expect("stateful writers enable object reference tracking");

    assert_eq!(state.get_or_add_reference(&value), (false, 0));
}
