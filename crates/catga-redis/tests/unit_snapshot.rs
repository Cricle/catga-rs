//! Unit tests for snapshot helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn snapshot_key(prefix: &str, stream_id: &str) -> String {
    format!("{prefix}:snapshot:{stream_id}")
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn from_unix_millis(millis: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis)
}

fn map_save_error(error_message: &str) -> catga_core::CatgaError {
    use catga_core::{CatgaError, ErrorCode};
    if error_message.contains("CATGA_SNAPSHOT_CONFLICT") {
        CatgaError::new(
            ErrorCode::Conflict,
            "a newer snapshot already exists for this stream",
        )
    } else {
        CatgaError::new(ErrorCode::Transient, error_message)
    }
}

// Enhanced snapshot helper functions (from enhanced_snapshot.rs)

fn version_member(version: i64) -> String {
    format!("{:016x}", (version as u64) ^ (1_u64 << 63))
}

fn parse_version_member(member: &str) -> Result<i64, &'static str> {
    let encoded = u64::from_str_radix(member, 16).map_err(|_| "invalid hex")?;
    Ok((encoded ^ (1_u64 << 63)) as i64)
}

fn enhanced_unix_millis(time: SystemTime) -> Result<i64, &'static str> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).map_err(|_| "time range error"),
        Err(error) => i64::try_from(error.duration().as_millis())
            .ok()
            .and_then(|millis| millis.checked_neg())
            .ok_or("time range error"),
    }
}

fn enhanced_from_unix_millis(millis: i64) -> Result<SystemTime, &'static str> {
    let duration = Duration::from_millis(millis.unsigned_abs());
    if millis >= 0 {
        UNIX_EPOCH.checked_add(duration).ok_or("time range error")
    } else {
        UNIX_EPOCH.checked_sub(duration).ok_or("time range error")
    }
}

// ==================== snapshot_key tests ====================

#[test]
fn snapshot_key_format() {
    let key = snapshot_key("catga", "stream-123");
    assert_eq!(key, "catga:snapshot:stream-123");
}

#[test]
fn snapshot_key_with_colons_in_id() {
    let key = snapshot_key("prefix", "ns:entity:123");
    assert_eq!(key, "prefix:snapshot:ns:entity:123");
}

#[test]
fn snapshot_key_empty_stream_id() {
    let key = snapshot_key("prefix", "");
    assert_eq!(key, "prefix:snapshot:");
}

#[test]
fn snapshot_key_empty_prefix() {
    let key = snapshot_key("", "stream");
    assert_eq!(key, ":snapshot:stream");
}

#[test]
fn snapshot_key_unicode() {
    let key = snapshot_key("prefix", "流程-123");
    assert_eq!(key, "prefix:snapshot:流程-123");
}

#[test]
fn snapshot_key_multiple_colons() {
    let key = snapshot_key("app:v1", "order:123:detail");
    assert_eq!(key, "app:v1:snapshot:order:123:detail");
}

// ==================== unix_millis tests ====================

#[test]
fn snapshot_unix_millis_at_epoch() {
    let result = unix_millis(UNIX_EPOCH);
    assert_eq!(result, 0);
}

#[test]
fn snapshot_unix_millis_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = unix_millis(time);
    assert_eq!(result, 1000);
}

#[test]
fn snapshot_unix_millis_one_millisecond() {
    let time = UNIX_EPOCH + Duration::from_millis(1);
    let result = unix_millis(time);
    assert_eq!(result, 1);
}

#[test]
fn snapshot_unix_millis_before_epoch() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = unix_millis(before);
    assert_eq!(result, 0);
}

#[test]
fn snapshot_unix_millis_large_time() {
    let time = UNIX_EPOCH + Duration::from_millis(u64::MAX);
    let result = unix_millis(time);
    assert_eq!(result, u64::MAX);
}

// ==================== from_unix_millis tests ====================

#[test]
fn snapshot_from_unix_millis_zero() {
    let result = from_unix_millis(0);
    assert_eq!(result, UNIX_EPOCH);
}

#[test]
fn snapshot_from_unix_millis_one_second() {
    let result = from_unix_millis(1000);
    assert_eq!(result, UNIX_EPOCH + Duration::from_secs(1));
}

#[test]
fn snapshot_from_unix_millis_one_millisecond() {
    let result = from_unix_millis(1);
    assert_eq!(result, UNIX_EPOCH + Duration::from_millis(1));
}

#[test]
fn snapshot_from_unix_millis_roundtrip() {
    let original = UNIX_EPOCH + Duration::from_millis(9876543210_u64);
    let millis = unix_millis(original);
    let restored = from_unix_millis(millis);
    assert_eq!(original, restored);
}

#[test]
fn snapshot_from_unix_millis_max() {
    let result = from_unix_millis(u64::MAX);
    assert_eq!(result, UNIX_EPOCH + Duration::from_millis(u64::MAX));
}

// ==================== map_save_error tests ====================

#[test]
fn snapshot_map_save_error_conflict() {
    let error = map_save_error("ERR CATGA_SNAPSHOT_CONFLICT");
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
    assert!(error.to_string().contains("newer snapshot"));
}

#[test]
fn snapshot_map_save_error_partial_conflict() {
    let error = map_save_error("CATGA_SNAPSHOT_CONFLICT: some message");
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
}

#[test]
fn snapshot_map_save_error_other_error() {
    let error = map_save_error("WRONGTYPE Operation against a key");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
    assert!(error.to_string().contains("WRONGTYPE"));
}

#[test]
fn snapshot_map_save_error_empty() {
    let error = map_save_error("");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
}

#[test]
fn snapshot_map_save_error_connection_error() {
    let error = map_save_error("connection refused");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
    assert!(error.to_string().contains("connection refused"));
}

// ==================== version_member tests ====================

#[test]
fn version_member_zero() {
    // Version 0 XOR with (1 << 63) gives us the sign bit flipped
    let result = version_member(0);
    let parsed = parse_version_member(&result).unwrap();
    assert_eq!(parsed, 0);
}

#[test]
fn version_member_positive() {
    let result = version_member(42);
    let parsed = parse_version_member(&result).unwrap();
    assert_eq!(parsed, 42);
}

#[test]
fn version_member_negative() {
    let result = version_member(-1);
    let parsed = parse_version_member(&result).unwrap();
    assert_eq!(parsed, -1);
}

#[test]
fn version_member_large() {
    let result = version_member(i64::MAX);
    let parsed = parse_version_member(&result).unwrap();
    assert_eq!(parsed, i64::MAX);
}

#[test]
fn version_member_format() {
    let result = version_member(0);
    assert_eq!(result.len(), 16);
    assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn version_member_roundtrip() {
    let versions = [0, 1, 42, -1, i64::MAX, i64::MIN];
    for version in versions {
        let encoded = version_member(version);
        let decoded = parse_version_member(&encoded).unwrap();
        assert_eq!(decoded, version, "roundtrip failed for {}", version);
    }
}

#[test]
fn parse_version_member_invalid_hex() {
    let result = parse_version_member("gggggggggggggggg");
    assert!(result.is_err());
}

#[test]
fn parse_version_member_invalid_length() {
    // Valid hex but wrong length (must be exactly 16 chars for 64-bit)
    let result = parse_version_member("12345678901234567"); // 17 chars
    assert!(result.is_err());
}

// ==================== enhanced_unix_millis tests ====================

#[test]
fn enhanced_unix_millis_at_epoch() {
    let result = enhanced_unix_millis(UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn enhanced_unix_millis_after_epoch() {
    let time = UNIX_EPOCH + Duration::from_secs(3600);
    let result = enhanced_unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3600000);
}

#[test]
fn enhanced_unix_millis_before_epoch() {
    let before = UNIX_EPOCH - Duration::from_secs(3600);
    let result = enhanced_unix_millis(before);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -3600000);
}

#[test]
fn enhanced_unix_millis_negative_boundary() {
    let time = UNIX_EPOCH - Duration::from_millis(1);
    let result = enhanced_unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), -1);
}

// ==================== enhanced_from_unix_millis tests ====================

#[test]
fn enhanced_from_unix_millis_zero() {
    let result = enhanced_from_unix_millis(0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH);
}

#[test]
fn enhanced_from_unix_millis_positive() {
    let result = enhanced_from_unix_millis(3600000);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH + Duration::from_secs(3600));
}

#[test]
fn enhanced_from_unix_millis_negative() {
    let result = enhanced_from_unix_millis(-3600000);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH - Duration::from_secs(3600));
}

#[test]
fn enhanced_from_unix_millis_roundtrip_positive() {
    let original = UNIX_EPOCH + Duration::from_secs(100000);
    let millis = enhanced_unix_millis(original).unwrap();
    let restored = enhanced_from_unix_millis(millis).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn enhanced_from_unix_millis_roundtrip_negative() {
    let original = UNIX_EPOCH - Duration::from_secs(100000);
    let millis = enhanced_unix_millis(original).unwrap();
    let restored = enhanced_from_unix_millis(millis).unwrap();
    assert_eq!(original, restored);
}
