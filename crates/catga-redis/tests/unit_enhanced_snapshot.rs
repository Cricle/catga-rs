//! Unit tests for enhanced_snapshot helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MUTATION_BATCH: i64 = 128;

fn version_member(version: i64) -> String {
    format!("{:016x}", (version as u64) ^ (1_u64 << 63))
}

fn parse_version_member(member: &[u8]) -> Result<i64, &'static str> {
    let member = std::str::from_utf8(member).map_err(|_| "invalid utf8")?;
    let encoded = u64::from_str_radix(member, 16).map_err(|_| "invalid hex")?;
    Ok((encoded ^ (1_u64 << 63)) as i64)
}

fn unix_millis(time: SystemTime) -> Result<i64, &'static str> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis())
            .map_err(|_| "time exceeds i64 range"),
        Err(error) => i64::try_from(error.duration().as_millis())
            .ok()
            .and_then(|millis| millis.checked_neg())
            .ok_or("negative time range exceeds i64"),
    }
}

fn from_unix_millis(millis: i64) -> Result<SystemTime, &'static str> {
    let duration = Duration::from_millis(millis.unsigned_abs());
    if millis >= 0 {
        UNIX_EPOCH.checked_add(duration)
    } else {
        UNIX_EPOCH.checked_sub(duration)
    }
    .ok_or("time out of range")
}

#[test]
fn mutation_batch_value() {
    assert_eq!(MUTATION_BATCH, 128);
}

#[test]
fn mutation_batch_reasonable() {
    assert!(MUTATION_BATCH > 0);
    assert!(MUTATION_BATCH <= 1024);
}

// =============================================================================
// version_member tests
// =============================================================================

#[test]
fn version_member_positive_version() {
    let member = version_member(1);
    assert_eq!(member.len(), 16);
    assert!(member.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn version_member_zero_version() {
    let member = version_member(0);
    // 0 ^ (1 << 63) = 0x8000000000000000
    assert_eq!(member, "8000000000000000");
}

#[test]
fn version_member_negative_version() {
    let member = version_member(-1);
    // -1 as u64 = 0xFFFFFFFFFFFFFFFF, XOR with 0x8000000000000000 = 0x7FFFFFFFFFFFFFFF
    assert_eq!(member, "7fffffffffffffff");
}

#[test]
fn version_member_large_version() {
    let member = version_member(i64::MAX);
    // i64::MAX as u64 = 0x7FFFFFFFFFFFFFFF, XOR = 0xFFFFFFFFFFFFFFFF
    assert_eq!(member, "ffffffffffffffff");
}

#[test]
fn version_member_pads_to_16_chars() {
    let member = version_member(0xABC);
    assert_eq!(member.len(), 16);
    // XOR with 0x8000000000000000 sets the high bit
    assert!(member.starts_with("800"));
}

#[test]
fn version_member_roundtrip_positive() {
    let version = 42_i64;
    let member = version_member(version);
    let parsed = parse_version_member(member.as_bytes()).unwrap();
    assert_eq!(parsed, version);
}

#[test]
fn version_member_roundtrip_negative() {
    let version = -12345_i64;
    let member = version_member(version);
    let parsed = parse_version_member(member.as_bytes()).unwrap();
    assert_eq!(parsed, version);
}

#[test]
fn version_member_roundtrip_zero() {
    let version = 0_i64;
    let member = version_member(version);
    let parsed = parse_version_member(member.as_bytes()).unwrap();
    assert_eq!(parsed, version);
}

#[test]
fn version_member_roundtrip_max() {
    let version = i64::MAX;
    let member = version_member(version);
    let parsed = parse_version_member(member.as_bytes()).unwrap();
    assert_eq!(parsed, version);
}

#[test]
fn version_member_roundtrip_min() {
    let version = i64::MIN;
    let member = version_member(version);
    let parsed = parse_version_member(member.as_bytes()).unwrap();
    assert_eq!(parsed, version);
}

// =============================================================================
// parse_version_member tests
// =============================================================================

#[test]
fn parse_version_member_valid_hex() {
    // Note: parse_version_member does NOT validate length, only hex validity
    let result = parse_version_member(b"0000000000000001");
    assert!(result.is_ok());
    // 0x0000000000000001 ^ 0x8000000000000000 = 0x8000000000000001
    // This decodes to a large positive number (i64::MIN + 1), not exactly 1
}

#[test]
fn parse_version_member_invalid_utf8() {
    let result = parse_version_member(&[0xFF, 0xFE]);
    assert!(result.is_err());
}

#[test]
fn parse_version_member_invalid_hex() {
    let result = parse_version_member(b"zzzzzzzzzzzzzzzz");
    assert!(result.is_err());
}

#[test]
fn parse_version_member_short_hex() {
    // Short hex strings are valid hex and will parse (no length check in implementation)
    let result = parse_version_member(b"abc");
    assert!(result.is_ok());
}

// =============================================================================
// unix_millis tests
// =============================================================================

#[test]
fn unix_millis_at_epoch() {
    let result = unix_millis(UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn unix_millis_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn unix_millis_one_millisecond() {
    let time = UNIX_EPOCH + Duration::from_millis(1);
    let result = unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

#[test]
fn unix_millis_before_epoch() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = unix_millis(before);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1000);
}

#[test]
fn unix_millis_before_epoch_one_ms() {
    let before = UNIX_EPOCH - Duration::from_millis(1);
    let result = unix_millis(before);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

// =============================================================================
// from_unix_millis tests
// =============================================================================

#[test]
fn from_unix_millis_zero() {
    let result = from_unix_millis(0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH);
}

#[test]
fn from_unix_millis_positive() {
    let result = from_unix_millis(1000);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH + Duration::from_secs(1));
}

#[test]
fn from_unix_millis_negative() {
    let result = from_unix_millis(-1000);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH - Duration::from_secs(1));
}

#[test]
fn from_unix_millis_roundtrip() {
    let original = SystemTime::now();
    let millis = unix_millis(original).unwrap();
    let restored = from_unix_millis(millis).unwrap();
    // Allow for small timing differences
    let diff = original.duration_since(restored).unwrap();
    assert!(diff.as_millis() < 100);
}

// =============================================================================
// Roundtrip consistency tests
// =============================================================================

#[test]
fn version_encoding_is_symmetric() {
    let versions = vec![i64::MIN, -1, 0, 1, i64::MAX];
    for v in versions {
        let member = version_member(v);
        let parsed = parse_version_member(member.as_bytes()).unwrap();
        assert_eq!(v, parsed, "version {} should roundtrip", v);
    }
}

#[test]
fn time_encoding_is_symmetric() {
    let times = vec![
        UNIX_EPOCH,
        UNIX_EPOCH + Duration::from_secs(1),
        UNIX_EPOCH + Duration::from_millis(12345),
        UNIX_EPOCH - Duration::from_secs(1),
        UNIX_EPOCH - Duration::from_millis(12345),
    ];
    for t in times {
        let millis = unix_millis(t).unwrap();
        let restored = from_unix_millis(millis).unwrap();
        let diff = t.duration_since(restored).unwrap();
        assert!(diff.as_millis() == 0 || diff.as_nanos() < 1_000_000);
    }
}
