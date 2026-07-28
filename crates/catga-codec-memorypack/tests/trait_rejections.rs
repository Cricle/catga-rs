//! Malformed-frame coverage for public MemoryPack trait implementations.

use std::collections::{BTreeMap, HashMap, VecDeque};

use catga_codec_memorypack::{MemoryPackError, MemoryPackSerializer, NullableVec};

fn i32_frame(value: i32) -> [u8; 4] {
    value.to_le_bytes()
}

#[test]
fn collection_implementations_accept_empty_wire_markers() {
    let null = i32_frame(-1);
    let empty = i32_frame(0);

    assert!(
        MemoryPackSerializer::deserialize::<Vec<u8>>(&null)
            .expect("null vector marker is empty")
            .is_empty()
    );
    assert!(
        MemoryPackSerializer::deserialize::<VecDeque<u8>>(&empty)
            .expect("empty deque marker is empty")
            .is_empty()
    );
    assert!(
        MemoryPackSerializer::deserialize::<HashMap<u8, u8>>(&null)
            .expect("null hash map marker is empty")
            .is_empty()
    );
    assert!(
        MemoryPackSerializer::deserialize::<BTreeMap<u8, u8>>(&empty)
            .expect("empty B-tree map marker is empty")
            .is_empty()
    );
}

#[test]
fn nullable_vectors_reject_negative_lengths_other_than_null() {
    let error = MemoryPackSerializer::deserialize::<NullableVec<u8>>(&i32_frame(-2))
        .expect_err("only -1 is the nullable vector null marker");

    assert!(matches!(error, MemoryPackError::InvalidLength(-2)));
}

#[cfg(feature = "chrono")]
#[test]
fn chrono_decoders_reject_invalid_clock_date_and_offset_values() {
    let invalid_time = (864_000_000_000_i64).to_le_bytes();
    let error = MemoryPackSerializer::deserialize::<chrono::NaiveTime>(&invalid_time)
        .expect_err("a 24-hour clock tick count is invalid");
    assert!(matches!(error, MemoryPackError::DeserializationError(_)));

    let invalid_date = i32::MAX.to_le_bytes();
    let error = MemoryPackSerializer::deserialize::<chrono::NaiveDate>(&invalid_date)
        .expect_err("an out-of-range date offset is invalid");
    assert!(matches!(error, MemoryPackError::DeserializationError(_)));

    let mut invalid_offset = Vec::new();
    invalid_offset.extend_from_slice(&i16::MAX.to_le_bytes());
    invalid_offset.extend_from_slice(&[0_u8; 6]);
    invalid_offset.extend_from_slice(&0_i64.to_le_bytes());
    let error =
        MemoryPackSerializer::deserialize::<chrono::DateTime<chrono::FixedOffset>>(&invalid_offset)
            .expect_err("an offset beyond a day is invalid");
    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}

#[cfg(feature = "num-bigint")]
#[test]
fn arbitrary_precision_integer_decoders_reject_negative_lengths() {
    let invalid_length = i32_frame(-1);

    for error in [
        MemoryPackSerializer::deserialize::<num_bigint::BigInt>(&invalid_length)
            .expect_err("BigInt rejects a negative byte length"),
        MemoryPackSerializer::deserialize::<num_bigint::BigUint>(&invalid_length)
            .expect_err("BigUint rejects a negative byte length"),
    ] {
        assert!(matches!(error, MemoryPackError::DeserializationError(_)));
    }
}

#[cfg(feature = "url")]
#[test]
fn urls_reject_non_url_text_frames() {
    let invalid_url =
        MemoryPackSerializer::serialize(&String::from("not a URL")).expect("text serializes");
    let error = MemoryPackSerializer::deserialize::<url::Url>(&invalid_url)
        .expect_err("invalid URL text is rejected");

    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}
