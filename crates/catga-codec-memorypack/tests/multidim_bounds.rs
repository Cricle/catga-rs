//! Invariant coverage for multidimensional MemoryPack arrays.

use catga_codec_memorypack::{
    MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MultiDimArray,
};

#[test]
fn constructor_returns_an_error_for_mismatched_shape() {
    assert!(MultiDimArray::<u8>::new(vec![2, 2], vec![1, 2, 3]).is_err());
}

#[test]
fn decoder_rejects_a_shape_that_does_not_match_its_data_length() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(3).expect("rank header writes");
    writer.write_i32(2).expect("first dimension writes");
    writer.write_i32(2).expect("second dimension writes");
    writer.write_i32(3).expect("data length writes");
    for value in [1_u8, 2, 3] {
        value.serialize(&mut writer).expect("data element writes");
    }
    let bytes = writer.into_bytes();
    let limits = MemoryPackDecodeLimits::new(64, 64, 16, 8, 4).expect("test limits are valid");
    let mut reader = MemoryPackReader::new_bounded(&bytes, limits).expect("test frame is valid");

    assert!(MultiDimArray::<u8>::deserialize(&mut reader).is_err());
}
