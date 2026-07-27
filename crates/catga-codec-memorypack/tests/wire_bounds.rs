//! Regression coverage for checked MemoryPack wire lengths and flags.

use catga_codec_memorypack::{
    MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackError, MemoryPackReader,
    MemoryPackSerialize, MemoryPackWriter, MultiDimArray,
};

#[test]
fn checked_i32_length_rejects_unrepresentable_wire_lengths() {
    assert_eq!(
        MemoryPackWriter::checked_i32_length(i32::MAX as usize)
            .expect("i32::MAX is a valid MemoryPack wire length"),
        i32::MAX
    );

    if let Some(too_large) = (i32::MAX as usize).checked_add(1) {
        assert!(matches!(
            MemoryPackWriter::checked_i32_length(too_large),
            Err(MemoryPackError::SerializationError(_))
        ));
    }
}

#[test]
fn multidimensional_array_rejects_a_rank_that_does_not_fit_the_wire_header() {
    let array = MultiDimArray {
        dimensions: vec![0; 255],
        data: Vec::<u8>::new(),
    };

    let error = array
        .serialize(&mut MemoryPackWriter::new())
        .expect_err("rank 255 cannot be represented by the rank-plus-one u8 header");

    assert!(matches!(error, MemoryPackError::SerializationError(_)));
}

#[test]
fn multidimensional_array_rejects_a_dimension_that_does_not_fit_i32() {
    let Some(too_large) = (i32::MAX as usize).checked_add(1) else {
        return;
    };
    let array = MultiDimArray {
        dimensions: vec![0, too_large],
        data: Vec::<u8>::new(),
    };

    let error = array
        .serialize(&mut MemoryPackWriter::new())
        .expect_err("dimensions above i32::MAX cannot be serialized");

    assert!(matches!(error, MemoryPackError::SerializationError(_)));
}

#[test]
fn generic_option_rejects_an_invalid_presence_flag_before_reading_a_value() {
    let bytes = 2_i32.to_le_bytes();
    let limits = MemoryPackDecodeLimits::new(32, 16, 8, 4, 4).expect("test limits are valid");
    let mut reader =
        MemoryPackReader::new_bounded(&bytes, limits).expect("test frame is within its limit");

    let error = Option::<u8>::deserialize(&mut reader)
        .expect_err("only zero and one are valid generic Option flags");

    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}
