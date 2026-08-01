//! Public reader/writer and optional-type boundary coverage.

use catga_codec_memorypack::{
    MemoryPackDecodeLimits, MemoryPackError, MemoryPackReader, MemoryPackSerializer,
    MemoryPackWriter,
};

fn reader(bytes: &[u8]) -> MemoryPackReader<'_> {
    MemoryPackReader::new_bounded(bytes, MemoryPackDecodeLimits::default())
        .expect("small test frames fit the default limit")
}

#[test]
fn reader_and_writer_preserve_scalar_boundaries_and_cursor_position() {
    let mut writer = MemoryPackWriter::new();
    writer.write_bool(true).expect("boolean writes");
    writer.write_i16(i16::MIN).expect("i16 writes");
    writer.write_u32(u32::MAX).expect("u32 writes");
    writer.write_i128(i128::MIN).expect("i128 writes");
    writer.write_u128(u128::MAX).expect("u128 writes");
    writer.write_char('🦀').expect("surrogate-pair char writes");

    let bytes = writer.into_bytes();
    let mut input = reader(&bytes);
    assert!(input.read_bool().expect("boolean reads"));
    assert_eq!(input.read_i16().expect("i16 reads"), i16::MIN);
    assert_eq!(input.read_u32().expect("u32 reads"), u32::MAX);
    assert_eq!(input.read_i128().expect("i128 reads"), i128::MIN);
    assert_eq!(input.read_u128().expect("u128 reads"), u128::MAX);
    assert_eq!(input.read_char().expect("char reads"), '🦀');
    assert_eq!(input.position(), bytes.len() as u64);
}

#[test]
fn string_wire_forms_support_zero_copy_and_distinguish_none_from_empty() {
    let mut writer = MemoryPackWriter::new();
    writer.write_string_option(None).expect("none writes");
    writer.write_string("").expect("empty writes");
    writer.write_string("memory 🦀").expect("UTF-8 text writes");

    let bytes = writer.into_bytes();
    let mut input = reader(&bytes);
    assert_eq!(input.read_string().expect("none reads as empty"), "");
    assert_eq!(input.read_str().expect("empty is zero-copy"), "");
    assert_eq!(input.read_str().expect("UTF-8 is zero-copy"), "memory 🦀");
    assert_eq!(input.position(), bytes.len() as u64);
}

#[test]
fn reader_rejects_malformed_utf8_and_utf16_for_their_respective_apis() {
    let mut invalid_utf8 = (!1_i32).to_le_bytes().to_vec();
    invalid_utf8.extend_from_slice(&1_i32.to_le_bytes());
    invalid_utf8.push(0xff);

    assert!(matches!(
        reader(&invalid_utf8).read_string(),
        Err(MemoryPackError::InvalidUtf8)
    ));
    assert!(matches!(
        reader(&invalid_utf8).read_str(),
        Err(MemoryPackError::InvalidUtf8)
    ));

    let lone_low_surrogate = [1_i32.to_le_bytes().as_slice(), &[0x00, 0xdc]].concat();
    assert!(matches!(
        reader(&lone_low_surrogate).read_string(),
        Err(MemoryPackError::InvalidUtf8)
    ));
    assert!(matches!(
        reader(&lone_low_surrogate).read_str(),
        Err(MemoryPackError::Utf16NotSupportedForZeroCopy)
    ));
}

#[test]
fn reader_owned_bytes_and_cursor_bounds_are_checked_without_overconsuming() {
    let mut input = reader(&[1, 2, 3, 4]);
    assert_eq!(input.read_bytes_vec(2).expect("prefix copies"), [1, 2]);
    assert_eq!(input.position(), 2);
    assert!(matches!(
        input.read_fixed_bytes::<3>(),
        Err(MemoryPackError::Io(_))
    ));
    assert_eq!(
        input.position(),
        4,
        "a failed Read::read_exact may consume the available suffix"
    );
    assert!(matches!(
        input.read_bytes(1),
        Err(MemoryPackError::UnexpectedEndOfBuffer)
    ));
}

#[test]
fn writer_reuses_buffers_and_rejects_lengths_outside_the_wire_range() {
    let backing = Vec::with_capacity(64);
    let allocation = backing.as_ptr();
    let mut writer = MemoryPackWriter::from_reusable_buffer(backing);
    assert!(writer.is_empty());
    writer
        .write_object_reference_id(128)
        .expect("reference writes");
    assert_eq!(writer.as_bytes(), &[250, 132, 128, 0]);
    assert_eq!(writer.as_bytes().as_ptr(), allocation);
    assert_eq!(writer.len(), 4);

    let error = MemoryPackWriter::checked_i32_length(i32::MAX as usize + 1)
        .expect_err("wire lengths are signed 32-bit values");
    assert!(matches!(error, MemoryPackError::SerializationError(_)));
}

#[cfg(feature = "chrono")]
#[test]
fn chrono_wire_forms_keep_tick_precision_and_reject_negative_clock_ticks() {
    use chrono::{NaiveTime, TimeDelta, TimeZone, Utc};

    let utc = Utc
        .timestamp_opt(-1, 999_999_900)
        .single()
        .expect("pre-epoch timestamp is valid");
    let time = NaiveTime::from_hms_nano_opt(0, 0, 0, 100).expect("time is valid");

    for value in [TimeDelta::nanoseconds(-100), TimeDelta::nanoseconds(100)] {
        let frame = MemoryPackSerializer::serialize(&value).expect("duration serializes");
        assert_eq!(frame.len(), 8);
        assert_eq!(
            MemoryPackSerializer::deserialize::<TimeDelta>(&frame).expect("duration reads"),
            value
        );
    }
    let frame = MemoryPackSerializer::serialize(&utc).expect("UTC serializes");
    let utc_wire: [u8; 8] = frame.as_slice().try_into().expect("UTC is fixed-width");
    assert_ne!(i64::from_le_bytes(utc_wire) & (1_i64 << 62), 0);
    assert_eq!(
        MemoryPackSerializer::deserialize::<chrono::DateTime<Utc>>(&frame).expect("UTC reads"),
        utc
    );
    assert!(matches!(
        MemoryPackSerializer::deserialize::<NaiveTime>(&(-1_i64).to_le_bytes()),
        Err(MemoryPackError::DeserializationError(_))
    ));
    let time_frame = MemoryPackSerializer::serialize(&time).expect("time serializes");
    assert_eq!(
        MemoryPackSerializer::deserialize::<NaiveTime>(&time_frame).expect("time reads"),
        time
    );
}

#[cfg(feature = "num-complex")]
#[test]
fn complex_numbers_use_two_little_endian_f64_components() {
    let value = num_complex::Complex::new(-1.5_f64, 2.25);
    let frame = MemoryPackSerializer::serialize(&value).expect("complex value serializes");

    assert_eq!(
        frame,
        [(-1.5_f64).to_le_bytes(), 2.25_f64.to_le_bytes()].concat()
    );
    assert_eq!(
        MemoryPackSerializer::deserialize::<num_complex::Complex<f64>>(&frame)
            .expect("complex value deserializes"),
        value
    );
}

#[cfg(feature = "glam")]
#[test]
fn glam_math_types_have_expected_fixed_width_wire_frames() {
    use glam::{Mat4, Vec3};

    let vector = Vec3::new(-1.0, 0.0, 2.5);
    let vector_frame = MemoryPackSerializer::serialize(&vector).expect("vector serializes");
    assert_eq!(vector_frame.len(), 3 * std::mem::size_of::<f32>());
    assert_eq!(
        MemoryPackSerializer::deserialize::<Vec3>(&vector_frame).expect("vector reads"),
        vector
    );

    let matrix = Mat4::from_cols_array(&[
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
    ]);
    let matrix_frame = MemoryPackSerializer::serialize(&matrix).expect("matrix serializes");
    assert_eq!(matrix_frame.len(), 16 * std::mem::size_of::<f32>());
    assert_eq!(
        MemoryPackSerializer::deserialize::<Mat4>(&matrix_frame).expect("matrix reads"),
        matrix
    );
}
