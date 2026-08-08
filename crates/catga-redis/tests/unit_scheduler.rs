//! Unit tests for scheduler module helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn target_key(flow_id: &str, state_id: &str) -> Result<Vec<u8>, String> {
    let flow_len = u64::try_from(flow_id.len()).map_err(|_| "flow identifier is too long".to_string())?;
    let state_len = u64::try_from(state_id.len()).map_err(|_| "state identifier is too long".to_string())?;
    let mut key = Vec::with_capacity(16 + flow_id.len() + state_id.len());
    key.extend_from_slice(&flow_len.to_be_bytes());
    key.extend_from_slice(flow_id.as_bytes());
    key.extend_from_slice(&state_len.to_be_bytes());
    key.extend_from_slice(state_id.as_bytes());
    Ok(key)
}

fn unix_millis(value: SystemTime) -> Result<i64, String> {
    let elapsed = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "due time precedes the Unix epoch".to_string())?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| "due time exceeds range".to_string())
}

fn duration_millis(value: Duration) -> Result<i64, String> {
    i64::try_from(value.as_millis())
        .map_err(|_| "lease duration exceeds range".to_string())
}

fn from_unix_millis(value: &str) -> Result<SystemTime, String> {
    let millis = value.parse::<u64>().map_err(|_| "invalid due time".to_string())?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or_else(|| "due time is out of range".to_string())
}

#[test]
fn target_key_format() {
    let result = target_key("flow-123", "state-456");
    assert!(result.is_ok());
    let key = result.unwrap();
    assert!(key.len() > 16);
    assert!(key.starts_with(&8u64.to_be_bytes())); // flow_len for "flow-123" (8 chars)
}

#[test]
fn target_key_empty_ids() {
    let result = target_key("", "");
    assert!(result.is_ok());
    let key = result.unwrap();
    assert!(key.starts_with(&0u64.to_be_bytes()));
    let state_len = u64::from_be_bytes(key[8..16].try_into().unwrap());
    assert_eq!(state_len, 0);
}

#[test]
fn target_key_preserves_content() {
    let flow_id = "my-flow";
    let state_id = "my-state";
    let result = target_key(flow_id, state_id);
    assert!(result.is_ok());
    let key = result.unwrap();
    let flow_len = u64::from_be_bytes(key[0..8].try_into().unwrap()) as usize;
    let extracted_flow = String::from_utf8(key[8..8 + flow_len].to_vec()).unwrap();
    assert_eq!(extracted_flow, flow_id);
}

#[test]
fn unix_millis_valid_time() {
    let time = UNIX_EPOCH + Duration::from_millis(1000);
    let result = unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn unix_millis_at_epoch() {
    let result = unix_millis(UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn unix_millis_before_epoch_error() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = unix_millis(before);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unix epoch"));
}

#[test]
fn duration_millis_valid() {
    let result = duration_millis(Duration::from_millis(500));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 500);
}

#[test]
fn duration_millis_zero() {
    let result = duration_millis(Duration::ZERO);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn duration_millis_seconds() {
    let result = duration_millis(Duration::from_secs(5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 5000);
}

#[test]
fn from_unix_millis_valid() {
    let result = from_unix_millis("1000");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH + Duration::from_millis(1000));
}

#[test]
fn from_unix_millis_zero() {
    let result = from_unix_millis("0");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), UNIX_EPOCH);
}

#[test]
fn from_unix_millis_invalid_string() {
    let result = from_unix_millis("not-a-number");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("invalid"));
}

#[test]
fn from_unix_millis_empty() {
    let result = from_unix_millis("");
    assert!(result.is_err());
}

#[test]
fn from_unix_millis_large_value() {
    let result = from_unix_millis("18446744073709551614");
    assert!(result.is_ok());
}

fn record_key(prefix: &str, schedule_id: &str) -> String {
    format!("{}:schedule:{schedule_id}", prefix)
}

fn record_prefix(prefix: &str) -> String {
    format!("{}:schedule:", prefix)
}

fn due_key(prefix: &str) -> String {
    format!("{}:due", prefix)
}

fn leased_key(prefix: &str) -> String {
    format!("{}:leased", prefix)
}

fn targets_key(prefix: &str) -> String {
    format!("{}:targets", prefix)
}

#[test]
fn scheduler_key_methods() {
    let prefix = "catga:scheduler";
    let record_key = record_key(prefix, "sched-123");
    assert_eq!(record_key, "catga:scheduler:schedule:sched-123");

    let record_prefix = record_prefix(prefix);
    assert_eq!(record_prefix, "catga:scheduler:schedule:");

    let due_key = due_key(prefix);
    assert_eq!(due_key, "catga:scheduler:due");

    let leased_key = leased_key(prefix);
    assert_eq!(leased_key, "catga:scheduler:leased");

    let targets_key = targets_key(prefix);
    assert_eq!(targets_key, "catga:scheduler:targets");
}

#[test]
fn scheduler_key_methods_empty_id() {
    let prefix = "prefix";
    let record_key = record_key(prefix, "");
    assert_eq!(record_key, "prefix:schedule:");
}
