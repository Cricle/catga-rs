//! Tests for codec module

use catga_core::codec::memorypack::varint::{INT64, read_varint, write_varint};
use catga_core::codec::memorypack::{MemoryPackDecodeLimits, MemoryPackReader, MemoryPackWriter};

#[test]
fn all_wire_widths_round_trip_and_truncation_is_reported() {
    for value in [
        i64::MIN,
        i32::MIN as i64,
        i16::MIN as i64,
        -121,
        -120,
        0,
        127,
        128,
        i16::MAX as i64,
        i32::MAX as i64,
        i64::MAX,
    ] {
        let mut writer = MemoryPackWriter::new();
        write_varint(&mut writer, value).expect("value writes");
        let mut reader =
            MemoryPackReader::new_bounded(writer.as_bytes(), MemoryPackDecodeLimits::default())
                .expect("frame is bounded");
        assert_eq!(read_varint(&mut reader).expect("value reads"), value);
    }
    let mut reader =
        MemoryPackReader::new_bounded(&[INT64 as u8], MemoryPackDecodeLimits::default())
            .expect("truncated frame is bounded");
    assert!(matches!(
        read_varint(&mut reader),
        Err(catga_core::codec::memorypack::MemoryPackError::Io(_))
    ));
}
