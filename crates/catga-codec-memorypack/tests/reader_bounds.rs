//! Regression coverage for reader cursor bounds used by version-tolerant derives.

use catga_codec_memorypack::{MemoryPackDecodeLimits, MemoryPackReader};

#[test]
fn skip_rejects_a_length_past_the_received_frame() {
    let limits = MemoryPackDecodeLimits::new(16, 16, 8, 4, 4).expect("test limits are valid");
    let mut reader =
        MemoryPackReader::new_bounded(&[1, 2], limits).expect("test frame is within its limit");

    reader
        .skip(2)
        .expect("the exact remaining frame may be skipped");
    assert!(reader.skip(1).is_err());
}

#[test]
fn rewind_rejects_a_length_before_the_frame_start() {
    let limits = MemoryPackDecodeLimits::new(16, 16, 8, 4, 4).expect("test limits are valid");
    let mut reader =
        MemoryPackReader::new_bounded(&[1, 2], limits).expect("test frame is within its limit");

    assert!(reader.rewind(1).is_err());
}
