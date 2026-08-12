//! Gap-filling contracts for [`MemoryPackReader`]: UTF-16 and zero-copy
//! string shapes, surrogate-pair char decoding, scalar edge values, cursor
//! movement, and bounded-decode budget enforcement.

use catga_core::codec::memorypack::{MemoryPackDecodeLimits, MemoryPackError, MemoryPackReader};

fn i32le(value: i32) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn u16le(value: u16) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

fn utf16_frame(char_count: i32, units: &[u16]) -> Vec<u8> {
    let mut frame = i32le(char_count);
    for unit in units {
        frame.extend(u16le(*unit));
    }
    frame
}

fn utf8_owned_frame(bytes: &[u8], char_count: i32) -> Vec<u8> {
    let mut frame = i32le(!(bytes.len() as i32));
    frame.extend(i32le(char_count));
    frame.extend_from_slice(bytes);
    frame
}

fn generous_limits() -> MemoryPackDecodeLimits {
    MemoryPackDecodeLimits::new(1024 * 1024, 1024 * 1024, 256 * 1024, 65_536, 32)
        .expect("valid limits")
}

/// Wire marker for an empty owned string.
const EMPTY_MARKER: [u8; 4] = (-1_i32).to_le_bytes();
/// Wire marker for a zero-character string.
const ZERO_MARKER: [u8; 4] = 0_i32.to_le_bytes();

#[test]
fn read_string_decodes_empty_utf8_and_utf16_shapes() {
    let mut reader =
        MemoryPackReader::new_bounded(&EMPTY_MARKER, generous_limits()).expect("bounded reader");
    assert_eq!(reader.read_string().expect("string decodes"), "");

    let mut reader =
        MemoryPackReader::new_bounded(&ZERO_MARKER, generous_limits()).expect("bounded reader");
    assert_eq!(reader.read_string().expect("string decodes"), "");

    let frame = utf16_frame(2, &[0x0041, 0x00E9]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_string().expect("string decodes"), "Aé");

    let frame = utf8_owned_frame("多字节".as_bytes(), 3);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_string().expect("string decodes"), "多字节");

    let frame = utf8_owned_frame(&[0xFF, 0xFE], 1);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(
        matches!(reader.read_string(), Err(MemoryPackError::InvalidUtf8)),
        "invalid UTF-8 bytes are rejected"
    );
}

#[test]
fn read_string_decodes_and_validates_utf16_surrogates() {
    // A surrogate pair decodes into one astral character.
    let frame = utf16_frame(2, &[0xD83D, 0xDE00]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_string().expect("string decodes"), "😀");

    // A trailing high surrogate without its low half is invalid.
    let frame = utf16_frame(1, &[0xD800]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(matches!(
        reader.read_string(),
        Err(MemoryPackError::InvalidUtf8)
    ));

    // A high surrogate followed by a non-surrogate unit is invalid.
    let frame = utf16_frame(2, &[0xD800, 0x0041]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(matches!(
        reader.read_string(),
        Err(MemoryPackError::InvalidUtf8)
    ));

    // A lone low surrogate is invalid.
    let frame = utf16_frame(1, &[0xDC00]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(matches!(
        reader.read_string(),
        Err(MemoryPackError::InvalidUtf8)
    ));
}

#[test]
fn read_string_enforces_string_and_allocation_budgets() {
    let limits = MemoryPackDecodeLimits::new(1024, 64, 2, 64, 8).expect("valid limits");
    let frame = utf16_frame(2, &[0x0041, 0x0042]);
    let mut reader = MemoryPackReader::new_bounded(&frame, limits).expect("bounded reader");
    assert!(
        matches!(
            reader.read_string(),
            Err(MemoryPackError::LimitExceeded {
                resource: "string bytes",
                limit: 2
            })
        ),
        "a UTF-16 payload over the string budget is rejected before allocation"
    );

    // The string bytes pass the string budget but overflow the cumulative
    // allocation budget once the owned expansion is reserved.
    let limits = MemoryPackDecodeLimits::new(1024, 8, 8, 64, 8).expect("valid limits");
    let frame = utf16_frame(4, &[0x0041, 0x0042, 0x0043, 0x0044]);
    let mut reader = MemoryPackReader::new_bounded(&frame, limits).expect("bounded reader");
    assert!(
        matches!(
            reader.read_string(),
            Err(MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: 8
            })
        ),
        "the cumulative allocation budget caps UTF-16 expansion"
    );
}

#[test]
fn read_str_borrows_utf8_and_refuses_utf16() {
    let mut reader =
        MemoryPackReader::new_bounded(&EMPTY_MARKER, generous_limits()).expect("bounded reader");
    assert_eq!(reader.read_str().expect("str decodes"), "");

    let mut reader =
        MemoryPackReader::new_bounded(&ZERO_MARKER, generous_limits()).expect("bounded reader");
    assert_eq!(reader.read_str().expect("str decodes"), "");

    let frame = utf8_owned_frame(b"hello", 5);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_str().expect("str decodes"), "hello");

    let frame = utf8_owned_frame(&[0xFF], 1);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(matches!(
        reader.read_str(),
        Err(MemoryPackError::InvalidUtf8)
    ));

    let frame = utf16_frame(1, &[0x0041]);
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(
        matches!(
            reader.read_str(),
            Err(MemoryPackError::Utf16NotSupportedForZeroCopy)
        ),
        "zero-copy decoding cannot materialize UTF-16 wire values"
    );
}

#[test]
fn read_bool_rejects_non_canonical_wire_values() {
    let mut reader =
        MemoryPackReader::new_bounded(&[0_u8, 1], generous_limits()).expect("bounded reader");
    assert!(!reader.read_bool().expect("bool decodes"));
    assert!(reader.read_bool().expect("bool decodes"));

    let mut reader =
        MemoryPackReader::new_bounded(&[2_u8], generous_limits()).expect("bounded reader");
    assert!(
        matches!(
            reader.read_bool(),
            Err(MemoryPackError::DeserializationError(_))
        ),
        "a non-canonical boolean wire value is rejected"
    );
}

#[test]
fn wide_scalar_readers_decode_fixed_width_values() {
    let mut frame = Vec::new();
    frame.extend_from_slice(&(-2.5_f64).to_le_bytes());
    frame.extend_from_slice(&(-170_141_183_460_469_231_731_687_303_715_884_i128).to_le_bytes());
    frame.extend_from_slice(&340_282_366_920_938_463_463_374_607_431_768_u128.to_le_bytes());
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_f64().expect("f64 decodes"), -2.5);
    assert_eq!(
        reader.read_i128().expect("i128 decodes"),
        -170_141_183_460_469_231_731_687_303_715_884_i128
    );
    assert_eq!(
        reader.read_u128().expect("u128 decodes"),
        340_282_366_920_938_463_463_374_607_431_768_u128
    );
    assert_eq!(reader.position() as usize, frame.len());

    let buffer: [u8; 4] = [9, 8, 7, 6];
    let mut reader =
        MemoryPackReader::new_bounded(&buffer, generous_limits()).expect("bounded reader");
    assert_eq!(
        reader.read_fixed_bytes::<4>().expect("array decodes"),
        buffer
    );
    assert!(
        reader.read_fixed_bytes::<1>().is_err(),
        "a truncated fixed array reports the missing bytes"
    );
}

#[test]
fn read_bytes_vec_respects_the_allocation_budget() {
    let limits = MemoryPackDecodeLimits::new(1024, 4, 4, 64, 8).expect("valid limits");
    let frame = [1_u8, 2, 3, 4, 5, 6, 7, 8];
    let mut reader = MemoryPackReader::new_bounded(&frame, limits).expect("bounded reader");
    assert!(
        matches!(
            reader.read_bytes_vec(8),
            Err(MemoryPackError::LimitExceeded {
                resource: "cumulative allocation bytes",
                limit: 4
            })
        ),
        "an owned byte vector over the allocation budget is rejected"
    );

    let mut reader =
        MemoryPackReader::new_bounded(&frame, generous_limits()).expect("bounded reader");
    assert_eq!(
        reader.read_bytes_vec(3).expect("bytes decode"),
        vec![1, 2, 3]
    );
    assert!(
        matches!(
            reader.read_bytes_vec(1024),
            Err(MemoryPackError::UnexpectedEndOfBuffer)
        ),
        "a read past the frame end is bounded"
    );
}

#[test]
fn read_char_validates_surrogate_pairs() {
    let mut frame = u16le(0x0041);
    frame.extend(u16le(0xD83D));
    frame.extend(u16le(0xDE00));
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert_eq!(reader.read_char().expect("char decodes"), 'A');
    assert_eq!(reader.read_char().expect("char decodes"), '😀');

    let lone_low = u16le(0xDC00);
    let mut reader =
        MemoryPackReader::new_bounded(&lone_low, generous_limits()).expect("bounded reader");
    assert!(
        matches!(
            reader.read_char(),
            Err(MemoryPackError::DeserializationError(_))
        ),
        "a low surrogate cannot start a char"
    );

    let mut frame = u16le(0xD800);
    frame.extend(u16le(0x0041));
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    assert!(
        matches!(
            reader.read_char(),
            Err(MemoryPackError::DeserializationError(_))
        ),
        "a high surrogate must be followed by a low surrogate"
    );
}

#[test]
fn skip_and_rewind_move_the_cursor_within_the_frame() {
    let frame = [1_u8, 2, 3, 4];
    let mut reader = MemoryPackReader::new_bounded(&frame, generous_limits()).expect("reader");
    reader.skip(2).expect("skip succeeds");
    assert_eq!(reader.position(), 2);
    assert_eq!(reader.read_u8().expect("u8 decodes"), 3);
    reader.rewind(1).expect("rewind succeeds");
    assert_eq!(reader.read_u8().expect("u8 decodes"), 3);
    assert!(
        matches!(reader.skip(99), Err(MemoryPackError::UnexpectedEndOfBuffer)),
        "skip cannot move past the frame end"
    );
    assert!(
        matches!(
            reader.rewind(99),
            Err(MemoryPackError::UnexpectedEndOfBuffer)
        ),
        "rewind cannot move before the frame start"
    );
}

#[test]
fn object_nesting_depth_is_bounded_and_saturates_on_leave() {
    let limits = MemoryPackDecodeLimits::new(1024, 1024, 1024, 64, 2).expect("valid limits");
    let mut reader = MemoryPackReader::new_bounded(&[], limits).expect("bounded reader");
    reader.enter_object().expect("first level enters");
    reader.enter_object().expect("second level enters");
    assert!(
        matches!(
            reader.enter_object(),
            Err(MemoryPackError::LimitExceeded {
                resource: "nesting depth",
                limit: 2
            })
        ),
        "the nesting budget rejects deeper object scopes"
    );
    reader.leave_object();
    reader.leave_object();
    reader.leave_object();
    reader.leave_object();
    reader.enter_object().expect("leaving frees nesting budget");
}

#[test]
fn new_bounded_rejects_frames_over_the_frame_budget() {
    let limits = MemoryPackDecodeLimits::new(4, 1024, 1024, 64, 8).expect("valid limits");
    let oversized = [0_u8; 8];
    match MemoryPackReader::new_bounded(&oversized, limits) {
        Err(MemoryPackError::LimitExceeded {
            resource: "frame bytes",
            limit: 4,
        }) => {}
        Err(error) => panic!("an oversized frame must report the frame budget: {error}"),
        Ok(_) => panic!("an oversized frame must be rejected before decoding"),
    }
}
