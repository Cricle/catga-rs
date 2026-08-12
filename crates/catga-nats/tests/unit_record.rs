//! Pure-logic contract tests for the internal NATS record framing.
//!
//! The framing module is crate-private, so this target compiles it directly to
//! exercise its encode/decode contract without a server.

#[path = "../src/record.rs"]
mod record;

use catga_core::ErrorCode;
use record::{create_record, decode_record};

#[test]
fn created_records_round_trip_their_payload_and_token() {
    let created = create_record(b"payload-bytes");
    let decoded = decode_record(created.value()).expect("record must decode");
    assert_eq!(decoded.payload(), b"payload-bytes");
    assert!(created.matches(&decoded));
}

#[test]
fn created_records_have_unique_tokens() {
    let first = create_record(b"same-payload");
    let second = create_record(b"same-payload");
    let first_decoded = decode_record(first.value()).expect("first record must decode");
    assert!(first.matches(&first_decoded));
    assert!(!second.matches(&first_decoded));
}

#[test]
fn legacy_records_pass_through_and_reencode_as_bare_payloads() {
    let decoded = decode_record(b"legacy-unframed-value").expect("legacy record must decode");
    assert_eq!(decoded.payload(), b"legacy-unframed-value");
    // A tokenless record re-encodes as the bare replacement payload.
    assert_eq!(decoded.with_payload(b"next"), b"next");
}

#[test]
fn framed_records_keep_their_token_when_the_payload_is_replaced() {
    let created = create_record(b"old");
    let decoded = decode_record(created.value()).expect("framed record must decode");
    let reframed = decoded.with_payload(b"next");
    let redecoded = decode_record(&reframed).expect("reframed record must decode");
    assert_eq!(redecoded.payload(), b"next");
    assert!(created.matches(&redecoded));
}

#[test]
fn prefixed_but_truncated_records_are_rejected() {
    let error = decode_record(b"CNR1short")
        .err()
        .expect("truncated record must fail");
    assert_eq!(error.code(), ErrorCode::Internal);

    // Boundary: one byte short of a full 20-byte header must also fail.
    let mut almost = b"CNR1".to_vec();
    almost.extend_from_slice(&[0xAB; 15]);
    assert!(decode_record(&almost).is_err());
}

#[test]
fn empty_payloads_and_empty_values_are_handled() {
    let created = create_record(b"");
    let decoded = decode_record(created.value()).expect("record must decode");
    assert!(decoded.payload().is_empty());

    let decoded = decode_record(b"").expect("empty value is a legacy record");
    assert!(decoded.payload().is_empty());
}
