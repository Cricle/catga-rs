#![forbid(unsafe_code)]
//! Bounded MemoryPack 1.21.3 compatibility for explicit Catga persistence records.

mod error;
mod fixtures;
mod limits;
mod reader;
mod records;
mod value;
mod writer;

pub use limits::MemoryPackLimits;
pub use reader::MemoryPackReader;
pub use records::{
    DeadLetterMessageRecord, FlowStateRecord, ForEachProgressRecord, InboxMessageRecord,
    NatsStoredSnapshotRecord, OutboxMessageRecord, StoredSnapshotMetadataRecord,
};
pub use value::{MemoryPackValueCodec, decode_value, encode_value};
pub use writer::MemoryPackWriter;

#[cfg(test)]
mod tests {
    use catga_core::ErrorCode;

    use super::{MemoryPackLimits, MemoryPackReader, MemoryPackWriter};

    #[test]
    fn fixed_and_null_object_headers_are_distinct() {
        let limits = MemoryPackLimits::default();
        let mut reader = MemoryPackReader::new(&[0xff], limits).expect("bounded input");
        assert!(!reader.read_object_header(4).expect("null header"));
        reader.finish().expect("exact frame");

        let mut reader = MemoryPackReader::new(&[4], limits).expect("bounded input");
        assert!(reader.read_object_header(4).expect("fixed header"));
        reader.finish_object().expect("close fixed object");
        reader.finish().expect("exact frame");

        let mut reader = MemoryPackReader::new(&[3], limits).expect("bounded input");
        assert_eq!(
            reader
                .read_object_header(4)
                .expect_err("member mismatch")
                .code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn sibling_fixed_objects_do_not_consume_nesting_depth() {
        let limits = MemoryPackLimits::new(32, 32, 32, 8, 1).expect("valid limits");
        let mut writer = MemoryPackWriter::new(limits);
        for _ in 0..3 {
            writer.write_object_header(0).expect("open sibling");
            writer.finish_object().expect("close sibling");
        }
        let bytes = writer.finish().expect("closed frame");

        let mut reader = MemoryPackReader::new(&bytes, limits).expect("bounded input");
        for _ in 0..3 {
            assert!(reader.read_object_header(0).expect("read sibling"));
            reader.finish_object().expect("close sibling");
        }
        reader.finish().expect("exact frame");
    }

    #[test]
    fn nested_fixed_objects_exceeding_depth_are_rejected() {
        let limits = MemoryPackLimits::new(32, 32, 32, 8, 1).expect("valid limits");
        let mut writer = MemoryPackWriter::new(limits);
        writer.write_object_header(0).expect("open outer");
        assert_eq!(
            writer
                .write_object_header(0)
                .expect_err("unclosed object scope prevents another sibling header")
                .code(),
            ErrorCode::Validation
        );

        let mut reader = MemoryPackReader::new(&[0, 0], limits).expect("bounded input");
        assert!(reader.read_object_header(0).expect("read outer"));
        assert_eq!(
            reader
                .read_object_header(0)
                .expect_err("unclosed object scope prevents another sibling header")
                .code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn little_endian_primitives_and_datetime_binary_round_trip() {
        let limits = MemoryPackLimits::default();
        let mut writer = MemoryPackWriter::new(limits);
        writer.write_bool(true).expect("bool");
        writer.write_u8(7).expect("u8");
        writer.write_u16(0x2010).expect("u16");
        writer.write_i32(-17).expect("i32");
        writer.write_i64(-99).expect("i64");
        writer.write_u64(42).expect("u64");
        writer
            .write_datetime_binary(0x48dc_4438_bd07_5700)
            .expect("datetime binary");
        let bytes = writer.finish().expect("bounded output");

        let mut reader = MemoryPackReader::new(&bytes, limits).expect("bounded input");
        assert!(reader.read_bool().expect("bool"));
        assert_eq!(reader.read_u8().expect("u8"), 7);
        assert_eq!(reader.read_u16().expect("u16"), 0x2010);
        assert_eq!(reader.read_i32().expect("i32"), -17);
        assert_eq!(reader.read_i64().expect("i64"), -99);
        assert_eq!(reader.read_u64().expect("u64"), 42);
        assert_eq!(
            reader.read_datetime_binary().expect("datetime binary"),
            0x48dc_4438_bd07_5700
        );
        reader.finish().expect("exact frame");
    }

    #[test]
    fn utf8_strings_and_arrays_preserve_null_empty_and_non_ascii_values() {
        let limits = MemoryPackLimits::default();
        let mut writer = MemoryPackWriter::new(limits);
        writer.write_string(None).expect("null string");
        writer.write_string(Some("")).expect("empty string");
        writer.write_string(Some("流程-雪")).expect("utf8 string");
        writer.write_bytes(None).expect("null bytes");
        writer.write_bytes(Some(&[])).expect("empty bytes");
        writer.write_bytes(Some(&[0, 127, 255])).expect("bytes");
        writer.write_i32_array(Some(&[0, 1, 3])).expect("i32 array");
        let bytes = writer.finish().expect("bounded output");

        let mut reader = MemoryPackReader::new(&bytes, limits).expect("bounded input");
        assert_eq!(reader.read_string().expect("null string"), None);
        assert_eq!(
            reader.read_string().expect("empty string").as_deref(),
            Some("")
        );
        assert_eq!(
            reader.read_string().expect("utf8 string").as_deref(),
            Some("流程-雪")
        );
        assert_eq!(reader.read_bytes().expect("null bytes"), None);
        assert_eq!(
            reader.read_bytes().expect("empty bytes").as_deref(),
            Some(&[][..])
        );
        assert_eq!(
            reader.read_bytes().expect("bytes").as_deref(),
            Some(&[0, 127, 255][..])
        );
        assert_eq!(
            reader.read_i32_array().expect("i32 array").as_deref(),
            Some(&[0, 1, 3][..])
        );
        reader.finish().expect("exact frame");
    }

    #[test]
    fn malformed_values_trailing_input_and_budgets_are_rejected() {
        let limits = MemoryPackLimits::new(32, 8, 8, 2, 1).expect("valid limits");
        assert!(MemoryPackLimits::new(0, 1, 1, 1, 1).is_err());
        assert!(MemoryPackReader::new(&[0; 33], limits).is_err());

        let mut reader = MemoryPackReader::new(&[2], limits).expect("bounded input");
        assert_eq!(
            reader.read_bool().expect_err("strict bool").code(),
            ErrorCode::Validation
        );

        let mut reader =
            MemoryPackReader::new(&[3, 0, 0, 0, 1, 2, 3], limits).expect("bounded input");
        assert_eq!(
            reader
                .read_bytes()
                .expect_err("collection item limit")
                .code(),
            ErrorCode::Validation
        );

        let mut reader = MemoryPackReader::new(&[0, 0, 0, 0, 7], limits).expect("bounded input");
        assert_eq!(
            reader.read_string().expect("empty string").as_deref(),
            Some("")
        );
        assert_eq!(
            reader.finish().expect_err("trailing byte").code(),
            ErrorCode::Validation
        );

        let malformed_utf8 = [0xfb, 0xff, 0xff, 0xff, 2, 0, 0, 0, 0xff, 0xff, 0xff, 0xff];
        let mut reader = MemoryPackReader::new(&malformed_utf8, limits).expect("bounded input");
        assert_eq!(
            reader.read_string().expect_err("invalid utf8").code(),
            ErrorCode::Validation
        );

        let utf16_snow = [1, 0, 0, 0, 0xea, 0x96];
        let mut reader =
            MemoryPackReader::new(&utf16_snow, MemoryPackLimits::default()).expect("bounded input");
        assert_eq!(
            reader.read_string().expect("valid utf16").as_deref(),
            Some("雪")
        );
        reader.finish().expect("exact utf16 frame");

        let invalid_utf16 = [1, 0, 0, 0, 0x00, 0xd8];
        let mut reader = MemoryPackReader::new(&invalid_utf16, MemoryPackLimits::default())
            .expect("bounded input");
        assert_eq!(
            reader.read_string().expect_err("invalid utf16").code(),
            ErrorCode::Validation
        );

        let cumulative = [5, 0, 0, 0, 1, 2, 3, 4, 5, 5, 0, 0, 0, 6, 7, 8, 9, 10];
        let cumulative_limits =
            MemoryPackLimits::new(32, 8, 8, 8, 1).expect("valid cumulative limits");
        let mut reader =
            MemoryPackReader::new(&cumulative, cumulative_limits).expect("bounded input");
        assert_eq!(
            reader.read_bytes().expect("first allocation").as_deref(),
            Some(&[1, 2, 3, 4, 5][..])
        );
        assert_eq!(
            reader
                .read_bytes()
                .expect_err("cumulative allocation budget")
                .code(),
            ErrorCode::Validation
        );

        let writer_limits = MemoryPackLimits::new(8, 8, 8, 8, 1).expect("valid writer limits");
        let mut writer = MemoryPackWriter::new(writer_limits);
        writer.write_i64(1).expect("within output budget");
        assert_eq!(
            writer.write_u8(1).expect_err("output budget").code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn writer_rejects_strings_over_the_utf16_unit_limit() {
        let limits = MemoryPackLimits::new(64, 64, 64, 1, 1).expect("valid limits");
        let mut writer = MemoryPackWriter::new(limits);

        assert_eq!(
            writer
                .write_string(Some("ab"))
                .expect_err("two UTF-16 units exceed the one-unit limit")
                .code(),
            ErrorCode::Validation
        );
    }
}
