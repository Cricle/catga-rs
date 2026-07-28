//! Public reader boundary and malformed-wire coverage.

use catga_codec_memorypack::{MemoryPackDecodeLimits, MemoryPackError, MemoryPackReader};

fn limits() -> MemoryPackDecodeLimits {
    MemoryPackDecodeLimits::new(32, 16, 16, 4, 4).expect("test limits are valid")
}

#[test]
fn read_bytes_rejects_an_overflowing_requested_length() {
    let mut reader =
        MemoryPackReader::new_bounded(&[1, 2], limits()).expect("test frame is within its limit");
    reader.read_u8().expect("the first byte is available");

    let error = reader
        .read_bytes(usize::MAX)
        .expect_err("an unrepresentable read length must not overflow the cursor calculation");

    assert!(matches!(error, MemoryPackError::UnexpectedEndOfBuffer));
}

#[test]
fn bounded_reader_rejects_frames_larger_than_its_public_limit() {
    let limits = MemoryPackDecodeLimits::new(1, 16, 16, 4, 4).expect("test limits are valid");

    let error = match MemoryPackReader::new_bounded(&[1, 2], limits) {
        Ok(_) => panic!("a frame beyond the configured public limit is rejected"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        MemoryPackError::LimitExceeded {
            resource: "frame bytes",
            limit: 1
        }
    ));
}

#[test]
fn primitive_reader_rejects_noncanonical_booleans_and_invalid_surrogates() {
    let mut boolean = MemoryPackReader::new_bounded(&[2], limits()).expect("test frame is valid");
    assert!(matches!(
        boolean.read_bool(),
        Err(MemoryPackError::DeserializationError(_))
    ));

    let low_surrogate_frame = 0xDC00_u16.to_le_bytes();
    let mut low_surrogate =
        MemoryPackReader::new_bounded(&low_surrogate_frame, limits()).expect("test frame is valid");
    assert!(matches!(
        low_surrogate.read_char(),
        Err(MemoryPackError::DeserializationError(_))
    ));

    let unpaired_high_frame = 0xD800_u16.to_le_bytes();
    let mut unpaired_high =
        MemoryPackReader::new_bounded(&unpaired_high_frame, limits()).expect("test frame is valid");
    assert!(matches!(
        unpaired_high.read_char(),
        Err(MemoryPackError::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn zero_copy_reader_declines_utf16_string_frames() {
    let mut frame = Vec::new();
    frame.extend_from_slice(&1_i32.to_le_bytes());
    frame.extend_from_slice(&(b'x' as u16).to_le_bytes());
    let mut reader = MemoryPackReader::new_bounded(&frame, limits()).expect("test frame is valid");

    assert!(matches!(
        reader.read_str(),
        Err(MemoryPackError::Utf16NotSupportedForZeroCopy)
    ));
}
