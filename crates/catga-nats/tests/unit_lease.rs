use super::*;
use std::time::Duration;

#[test]
fn value_format_matches_parse_roundtrip() {
    let owner = "worker-1";
    let ttl = Duration::from_secs(30);
    let val = value(owner, ttl);
    let parsed = parse(val.as_bytes());
    assert!(parsed.is_some(), "value string should be parseable: {val}");
    let (parsed_owner, parsed_expires) = parsed.expect("value string should be parseable");
    assert_eq!(parsed_owner, owner);
    let expected_min = now_millis() + ttl.as_millis() as u64;
    let expected_max = expected_min + 10_000;
    assert!(
        parsed_expires >= expected_min && parsed_expires <= expected_max,
        "expiry {parsed_expires} should be between {expected_min} and {expected_max}"
    );
}

#[test]
fn value_uses_tab_separator() {
    let owner = "test-worker";
    let ttl = Duration::from_secs(60);
    let val = value(owner, ttl);
    assert!(
        val.contains('\t'),
        "value should contain tab separator: {val}"
    );
    let parts: Vec<&str> = val.split('\t').collect();
    assert_eq!(parts.len(), 2, "should have exactly two parts");
    assert_eq!(parts[0], owner);
    assert!(parts[1].parse::<u64>().is_ok());
}

#[test]
fn parse_rejects_invalid_utf8() {
    let invalid = vec![0x80, 0x81, 0x82];
    assert!(parse(&invalid).is_none());
}

#[test]
fn parse_rejects_missing_tab() {
    let no_tab = b"owner_only_no_expiry";
    assert!(parse(no_tab).is_none());
}

#[test]
fn parse_rejects_invalid_expiry() {
    let invalid_expiry = "owner\tnot_a_number";
    assert!(parse(invalid_expiry.as_bytes()).is_none());
}

#[test]
fn parse_rejects_empty_expiry() {
    let empty_expiry = "owner\t";
    assert!(parse(empty_expiry.as_bytes()).is_none());
}

#[test]
fn parse_accepts_empty_owner() {
    let empty_owner = "\t1234567890";
    assert!(parse(empty_owner.as_bytes()).is_some());
    let parsed = parse(empty_owner.as_bytes()).expect("empty owner should parse");
    assert_eq!(parsed.0, "");
    assert_eq!(parsed.1, 1234567890);
}

#[test]
fn parse_accepts_various_owners() {
    let val = value("worker", Duration::from_secs(1));
    assert!(parse(val.as_bytes()).is_some());

    let val2 = value("worker-1@region-2", Duration::from_secs(1));
    assert!(parse(val2.as_bytes()).is_some());

    let val3 = value("工作器-1", Duration::from_secs(1));
    assert!(parse(val3.as_bytes()).is_some());
}

#[test]
fn value_ttl_must_be_at_least_1_millis() {
    let owner = "test";
    let val = value(owner, Duration::ZERO);
    let parsed = parse(val.as_bytes()).expect("should parse");
    let (_, expires) = parsed;
    assert!(
        expires > now_millis(),
        "zero-ttl expiry should still be in the future"
    );
}

#[test]
fn value_handles_very_long_ttl() {
    let owner = "test";
    let very_long_ttl = Duration::from_secs(60 * 60 * 24 * 365 * 100);
    let val = value(owner, very_long_ttl);
    let parsed = parse(val.as_bytes()).expect("should parse");
    let (_, expires) = parsed;
    assert!(expires > now_millis());
}

#[test]
fn now_millis_returns_positive_value() {
    let now = now_millis();
    assert!(now > 1_577_836_800_000u64, "timestamp should be after 2020");
    assert!(
        now < 4_104_451_840_000u64,
        "timestamp should be before 2100"
    );
}

#[test]
fn now_millis_is_monotonic_increasing() {
    let now1 = now_millis();
    let now2 = now_millis();
    assert!(now2 >= now1, "subsequent calls should return >= value");
}

#[test]
fn map_error_creates_transient_error() {
    let err = map_error("connection refused");
    assert_eq!(err.code(), ErrorCode::Transient);
    assert!(err.to_string().contains("connection refused"));
}
