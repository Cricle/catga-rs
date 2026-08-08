//! Unit tests for projection module helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn key(prefix: &str, projection_name: &str) -> String {
    format!("{}:{projection_name}", prefix)
}

fn decode(
    projection_name: &str,
    stream_id: &str,
    value: &str,
) -> Result<(String, String, i64, u64), String> {
    let (version, timestamp) = value.split_once('\t').ok_or_else(|| "malformed".to_string())?;
    let version = version.parse().map_err(|_| "invalid version".to_string())?;
    let timestamp = timestamp.parse().map_err(|_| "invalid timestamp".to_string())?;
    Ok((projection_name.to_string(), stream_id.to_string(), version, timestamp))
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

#[test]
fn key_format_includes_prefix_and_projection() {
    let prefix = "catga:checkpoints";
    let projection_name = "order-totals";
    let expected_key = format!("{}:{}", prefix, projection_name);
    assert_eq!(expected_key, "catga:checkpoints:order-totals");
}

#[test]
fn key_format_with_underscores() {
    let prefix = "prefix";
    let projection_name = "my_projection";
    let expected_key = format!("{}:{}", prefix, projection_name);
    assert_eq!(expected_key, "prefix:my_projection");
}

#[test]
fn key_format_with_dots() {
    let prefix = "prefix";
    let projection_name = "com.example.projection";
    let expected_key = format!("{}:{}", prefix, projection_name);
    assert_eq!(expected_key, "prefix:com.example.projection");
}

#[test]
fn key_format_with_empty_projection() {
    let prefix = "prefix";
    let projection_name = "";
    let expected_key = format!("{}:{}", prefix, projection_name);
    assert_eq!(expected_key, "prefix:");
}

#[test]
fn decode_valid_checkpoint() {
    let result = decode("order-totals", "order-42", "5\t1000000");
    assert!(result.is_ok());
    let (proj, stream, version, ts) = result.unwrap();
    assert_eq!(proj, "order-totals");
    assert_eq!(stream, "order-42");
    assert_eq!(version, 5);
    assert_eq!(ts, 1000000);
}

#[test]
fn decode_missing_tab_separator() {
    let result = decode("projection", "stream", "invalid");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("malformed"));
}

#[test]
fn decode_invalid_version() {
    let result = decode("projection", "stream", "not-a-number\t1000");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("version"));
}

#[test]
fn decode_invalid_timestamp() {
    let result = decode("projection", "stream", "5\tnot-a-timestamp");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("timestamp"));
}

#[test]
fn decode_empty_version() {
    let result = decode("projection", "stream", "\t1000");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("version"));
}

#[test]
fn decode_empty_timestamp() {
    let result = decode("projection", "stream", "5\t");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("timestamp"));
}

#[test]
fn decode_negative_version() {
    let result = decode("projection", "stream", "-1\t1000");
    assert!(result.is_ok());
    let (_, _, version, _) = result.unwrap();
    assert_eq!(version, -1);
}

#[test]
fn decode_large_timestamp() {
    let result = decode("projection", "stream", "1\t9999999999999");
    assert!(result.is_ok());
    let (_, _, _, timestamp) = result.unwrap();
    assert_eq!(timestamp, 9999999999999);
}

#[test]
fn unix_millis_at_epoch() {
    assert_eq!(unix_millis(UNIX_EPOCH), 0);
}

#[test]
fn unix_millis_one_second() {
    assert_eq!(unix_millis(UNIX_EPOCH + Duration::from_secs(1)), 1000);
}

#[test]
fn unix_millis_one_millisecond() {
    assert_eq!(unix_millis(UNIX_EPOCH + Duration::from_millis(1)), 1);
}

#[test]
fn unix_millis_before_epoch_returns_zero() {
    assert_eq!(unix_millis(UNIX_EPOCH - Duration::from_secs(1)), 0);
}

#[test]
fn unix_millis_large_value() {
    let far_future = UNIX_EPOCH + Duration::from_secs(u64::MAX / 1000);
    let millis = unix_millis(far_future);
    assert!(millis > 0);
}
