//! Unit tests for event_store helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn version_key(prefix: &str, stream_id: &str) -> String {
    format!("{prefix}:version:{stream_id}")
}

fn stream_key(prefix: &str, stream_id: &str) -> String {
    format!("{prefix}:stream:{stream_id}")
}

fn ids_key(prefix: &str) -> String {
    format!("{prefix}:ids")
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

fn stream_entry_id(version: u64) -> Result<String, &'static str> {
    let id = version.checked_add(1).ok_or("version overflow")?;
    Ok(format!("{id}-0"))
}

fn map_append_error(error_message: &str) -> catga_core::CatgaError {
    use catga_core::{CatgaError, ErrorCode};
    if error_message.contains("CATGA_VERSION_CONFLICT") {
        CatgaError::new(ErrorCode::Conflict, "event stream version conflict")
    } else if error_message.contains("CATGA_VERSION_EXHAUSTED") {
        CatgaError::new(ErrorCode::Internal, "event stream version is exhausted")
    } else {
        CatgaError::new(ErrorCode::Transient, error_message)
    }
}

// ==================== version_key tests ====================

#[test]
fn version_key_format() {
    let key = version_key("catga", "stream-123");
    assert_eq!(key, "catga:version:stream-123");
}

#[test]
fn version_key_with_colons_in_id() {
    let key = version_key("prefix", "ns:entity:123");
    assert_eq!(key, "prefix:version:ns:entity:123");
}

#[test]
fn version_key_empty_stream_id() {
    let key = version_key("prefix", "");
    assert_eq!(key, "prefix:version:");
}

#[test]
fn version_key_empty_prefix() {
    let key = version_key("", "stream");
    assert_eq!(key, ":version:stream");
}

#[test]
fn version_key_unicode() {
    let key = version_key("prefix", "流程-123");
    assert_eq!(key, "prefix:version:流程-123");
}

// ==================== stream_key tests ====================

#[test]
fn stream_key_format() {
    let key = stream_key("catga", "stream-123");
    assert_eq!(key, "catga:stream:stream-123");
}

#[test]
fn stream_key_with_colons_in_id() {
    let key = stream_key("prefix", "ns:entity:123");
    assert_eq!(key, "prefix:stream:ns:entity:123");
}

#[test]
fn stream_key_empty_stream_id() {
    let key = stream_key("prefix", "");
    assert_eq!(key, "prefix:stream:");
}

#[test]
fn stream_key_empty_prefix() {
    let key = stream_key("", "stream");
    assert_eq!(key, ":stream:stream");
}

#[test]
fn stream_key_unicode() {
    let key = stream_key("prefix", "流程-123");
    assert_eq!(key, "prefix:stream:流程-123");
}

// ==================== ids_key tests ====================

#[test]
fn ids_key_format() {
    let key = ids_key("catga");
    assert_eq!(key, "catga:ids");
}

#[test]
fn ids_key_empty_prefix() {
    let key = ids_key("");
    assert_eq!(key, ":ids");
}

#[test]
fn ids_key_with_path_like_prefix() {
    let key = ids_key("app/data");
    assert_eq!(key, "app/data:ids");
}

// ==================== unix_millis tests ====================

#[test]
fn unix_millis_at_epoch() {
    let result = unix_millis(UNIX_EPOCH);
    assert_eq!(result, 0);
}

#[test]
fn unix_millis_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = unix_millis(time);
    assert_eq!(result, 1000);
}

#[test]
fn unix_millis_one_millisecond() {
    let time = UNIX_EPOCH + Duration::from_millis(1);
    let result = unix_millis(time);
    assert_eq!(result, 1);
}

#[test]
fn unix_millis_before_epoch() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = unix_millis(before);
    assert_eq!(result, 0);
}

#[test]
fn unix_millis_large_time() {
    let time = UNIX_EPOCH + Duration::from_millis(u64::MAX);
    let result = unix_millis(time);
    assert_eq!(result, u64::MAX);
}

// unix_millis tests are designed to test values that fit within Duration bounds

// ==================== from_unix_millis tests ====================

#[test]
fn from_unix_millis_zero() {
    let result = from_unix_millis(0);
    assert_eq!(result, UNIX_EPOCH);
}

#[test]
fn from_unix_millis_one_second() {
    let result = from_unix_millis(1000);
    assert_eq!(result, UNIX_EPOCH + Duration::from_secs(1));
}

#[test]
fn from_unix_millis_one_millisecond() {
    let result = from_unix_millis(1);
    assert_eq!(result, UNIX_EPOCH + Duration::from_millis(1));
}

#[test]
fn from_unix_millis_roundtrip() {
    let original = UNIX_EPOCH + Duration::from_millis(1234567890);
    let millis = unix_millis(original);
    let restored = from_unix_millis(millis);
    assert_eq!(original, restored);
}

#[test]
fn from_unix_millis_max() {
    let result = from_unix_millis(u64::MAX);
    assert_eq!(result, UNIX_EPOCH + Duration::from_millis(u64::MAX));
}

#[test]
fn from_unix_millis_large_value() {
    let millis = 9999999999999_u64;
    let result = from_unix_millis(millis);
    assert!(result > UNIX_EPOCH);
}

// ==================== stream_entry_id tests ====================

#[test]
fn stream_entry_id_zero() {
    let result = stream_entry_id(0);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "1-0");
}

#[test]
fn stream_entry_id_one() {
    let result = stream_entry_id(1);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "2-0");
}

#[test]
fn stream_entry_id_large_value() {
    let result = stream_entry_id(u64::MAX - 1);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), format!("{}-0", u64::MAX));
}

#[test]
fn stream_entry_id_overflow() {
    let result = stream_entry_id(u64::MAX);
    assert!(result.is_err());
}

#[test]
fn stream_entry_id_format() {
    let id = stream_entry_id(42).unwrap();
    assert!(id.ends_with("-0"));
    let base: u64 = id.trim_end_matches("-0").parse().unwrap();
    assert_eq!(base, 43);
}

// ==================== map_append_error tests ====================

#[test]
fn map_append_error_version_conflict() {
    let error = map_append_error("ERR CATGA_VERSION_CONFLICT something");
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
    assert!(error.to_string().contains("version conflict"));
}

#[test]
fn map_append_error_version_exhausted() {
    let error = map_append_error("ERR CATGA_VERSION_EXHAUSTED");
    assert_eq!(error.code(), catga_core::ErrorCode::Internal);
    assert!(error.to_string().contains("exhausted"));
}

#[test]
fn map_append_error_other_error() {
    let error = map_append_error("WRONGTYPE Operation against a key");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
    assert!(error.to_string().contains("WRONGTYPE"));
}

#[test]
fn map_append_error_empty_message() {
    let error = map_append_error("");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
}

#[test]
fn map_append_error_partial_match() {
    let error = map_append_error("CATGA_VERSION_CONFLICT: another error");
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
}

#[test]
fn map_append_error_partial_exhausted_match() {
    let error = map_append_error("CATGA_VERSION_EXHAUSTED: message");
    assert_eq!(error.code(), catga_core::ErrorCode::Internal);
}

// ==================== APPEND Lua script logic tests ====================

#[test]
fn append_script_version_increment_logic() {
    // Test version increment from -1 (uninitialized) -> 0
    let current = "-1";
    let final_val = increment_version(current);
    assert_eq!(final_val, "0");
}

#[test]
fn append_script_version_increment_normal() {
    // Test normal version increment
    let current = "5";
    let final_val = increment_version(current);
    assert_eq!(final_val, "6");
}

#[test]
fn append_script_version_increment_with_carry() {
    // Test increment with carry propagation (9 -> 10)
    let current = "9";
    let final_val = increment_version(current);
    assert_eq!(final_val, "10");
}

#[test]
fn append_script_version_increment_double_digit() {
    // Test increment with carry propagation through multiple digits
    let current = "99";
    let final_val = increment_version(current);
    assert_eq!(final_val, "100");
}

#[test]
fn append_script_version_increment_large() {
    // Test increment on a larger number: 123456789 -> 123456790
    let current = "123456789";
    let final_val = increment_version(current);
    assert_eq!(final_val, "123456790");
}

#[test]
fn append_script_version_increment_max_safe() {
    // Test increment near max digit 9: 9 -> 10
    let current = "9";
    let final_val = increment_version(current);
    assert_eq!(final_val, "10");
}

// Lua version increment simulation (copied from APPEND script logic)
fn increment_version(value: &str) -> String {
    if value == "-1" {
        return "0".to_string();
    }
    let digits: Vec<u8> = value.as_bytes().to_vec();
    let mut carry = 1usize;
    let mut result = digits.clone();

    // Process from right to left (like Lua's reverse iteration)
    for i in (0..result.len()).rev() {
        let digit_val = result[i].wrapping_sub(b'0') as usize;
        let sum = digit_val + carry;
        if sum == 10 {
            result[i] = b'0';
            // carry stays 1
        } else {
            result[i] = b'0' + (sum as u8);
            carry = 0;
            break;
        }
    }

    if carry == 1 {
        // Insert '1' at the front
        result.insert(0, b'1');
    }

    String::from_utf8(result).unwrap()
}
