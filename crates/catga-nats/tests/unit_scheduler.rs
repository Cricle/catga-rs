//! Unit tests for scheduler helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sha2::{Digest, Sha256};

fn target_record_key(target: &[u8]) -> String {
    format!("r{}", hex::encode(Sha256::digest(target)))
}

fn schedule_key(schedule_id: &str) -> Option<&str> {
    let (key, generation) = schedule_id.rsplit_once(':')?;
    if key.len() != 65
        || !key.starts_with('r')
        || !key[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    // Check if generation is a valid UUID
    uuid::Uuid::parse_str(generation).ok()?;
    Some(key)
}

const fn metadata_key() -> &'static str {
    "m"
}

fn page_key(page: u64) -> String {
    format!("p{page}")
}

fn marker_key(key: &str) -> String {
    format!("i{}", hex::encode(Sha256::digest(key.as_bytes())))
}

fn target_bytes(flow_id: &str, state_id: &str) -> Result<Vec<u8>, String> {
    let flow_len = u64::try_from(flow_id.len()).map_err(|_| "flow identifier is too long".to_string())?;
    let state_len = u64::try_from(state_id.len()).map_err(|_| "state identifier is too long".to_string())?;
    let capacity = 16_usize
        .checked_add(flow_id.len())
        .and_then(|value| value.checked_add(state_id.len()))
        .ok_or_else(|| "scheduler target is too long".to_string())?;
    let mut target = Vec::with_capacity(capacity);
    target.extend_from_slice(&flow_len.to_be_bytes());
    target.extend_from_slice(flow_id.as_bytes());
    target.extend_from_slice(&state_len.to_be_bytes());
    target.extend_from_slice(state_id.as_bytes());
    Ok(target)
}

fn to_millis(value: SystemTime) -> Result<u64, String> {
    let elapsed = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "due time precedes the Unix epoch".to_string())?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| "due time exceeds NATS range".to_string())
}

fn duration_millis(value: Duration) -> Result<u64, String> {
    u64::try_from(value.as_millis())
        .map_err(|_| "lease duration exceeds NATS range".to_string())
}

fn from_millis(value: u64) -> Result<SystemTime, String> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(value))
        .ok_or_else(|| "NATS scheduler due time is out of range".to_string())
}

#[test]
fn target_bytes_format() {
    let target = target_bytes("flow", "state").expect("encode target");
    assert_eq!(&target[..8], &(4_u64).to_be_bytes());
    assert_eq!(&target[8..12], b"flow");
    assert_eq!(&target[12..20], &(5_u64).to_be_bytes());
    assert_eq!(&target[20..], b"state");
}

#[test]
fn target_bytes_empty_flow_id() {
    let target = target_bytes("", "state").expect("encode target");
    // When flow_id is empty, it has 0 length at [0..8], then state_id length at [8..16]
    assert_eq!(&target[..8], &(0_u64).to_be_bytes());
    assert_eq!(&target[8..16], &(5_u64).to_be_bytes());
    assert_eq!(&target[16..], b"state");
}

#[test]
fn target_bytes_empty_state_id() {
    let target = target_bytes("flow", "").expect("encode target");
    assert_eq!(&target[..8], &(4_u64).to_be_bytes());
    assert_eq!(&target[8..12], b"flow");
    assert_eq!(&target[12..20], &(0_u64).to_be_bytes());
}

#[test]
fn target_bytes_different_ids() {
    let target1 = target_bytes("flow-a", "state-a").expect("encode target");
    let target2 = target_bytes("flow-b", "state-b").expect("encode target");
    assert_ne!(target1, target2);
}

#[test]
fn target_record_key_format() {
    let target = target_bytes("flow", "state").expect("encode target");
    let record_key = target_record_key(&target);
    assert_eq!(record_key.len(), 65);
    assert!(record_key.starts_with('r'));
    assert!(&record_key[1..].chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn target_record_key_consistency() {
    let target = target_bytes("flow", "state").expect("encode target");
    let key1 = target_record_key(&target);
    let key2 = target_record_key(&target);
    assert_eq!(key1, key2);
}

#[test]
fn target_record_key_different_targets() {
    let target1 = target_bytes("flow-a", "state").expect("encode target");
    let target2 = target_bytes("flow-b", "state").expect("encode target");
    let key1 = target_record_key(&target1);
    let key2 = target_record_key(&target2);
    assert_ne!(key1, key2);
}

#[test]
fn marker_key_format() {
    let record_key = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let marker = marker_key(record_key);
    assert!(marker.starts_with('i'));
    assert_eq!(marker.len(), 65);
}

#[test]
fn marker_key_consistency() {
    let key = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    assert_eq!(marker_key(key), marker_key(key));
}

#[test]
fn marker_key_different_keys() {
    let key1 = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let key2 = "rabcdef0123456789abcdef0123456789abcdef0123456789abcdef01234567";
    assert_ne!(marker_key(key1), marker_key(key2));
}

#[test]
fn metadata_key_value() {
    assert_eq!(metadata_key(), "m");
}

#[test]
fn page_key_format() {
    assert_eq!(page_key(0), "p0");
    assert_eq!(page_key(1), "p1");
    assert_eq!(page_key(100), "p100");
}

#[test]
fn schedule_key_valid() {
    let record_key = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let schedule_id = format!("{record_key}:550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(schedule_key(&schedule_id), Some(record_key));
}

#[test]
fn schedule_key_invalid_empty() {
    assert_eq!(schedule_key(""), None);
}

#[test]
fn schedule_key_invalid_short() {
    assert_eq!(schedule_key("not-a-schedule-id"), None);
}

#[test]
fn schedule_key_invalid_short_prefix() {
    assert_eq!(schedule_key("r0123:550e8400-e29b-41d4-a716-446655440000"), None);
}

#[test]
fn schedule_key_invalid_uuid() {
    let record_key = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let schedule_id = format!("{record_key}:not-a-uuid");
    assert_eq!(schedule_key(&schedule_id), None);
}

#[test]
fn schedule_key_invalid_prefix() {
    let schedule_id = "x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:550e8400-e29b-41d4-a716-446655440000";
    assert_eq!(schedule_key(schedule_id), None);
}

#[test]
fn schedule_key_invalid_hex() {
    let record_key = "r0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"; // 'g' is not hex
    let schedule_id = format!("{record_key}:550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(schedule_key(&schedule_id), None);
}

#[test]
fn to_millis_at_epoch() {
    let result = to_millis(UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn to_millis_before_epoch_error() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = to_millis(before);
    assert!(result.is_err());
}

#[test]
fn to_millis_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = to_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn to_millis_large_value() {
    let time = UNIX_EPOCH + Duration::from_millis(99);
    let result = to_millis(time);
    assert_eq!(result.unwrap(), 99);
}

#[test]
fn duration_millis_zero() {
    assert_eq!(duration_millis(Duration::ZERO).unwrap(), 0);
}

#[test]
fn duration_millis_one_second() {
    assert_eq!(duration_millis(Duration::from_secs(1)).unwrap(), 1000);
}

#[test]
fn duration_millis_large() {
    assert_eq!(duration_millis(Duration::from_millis(99)).unwrap(), 99);
}

#[test]
fn duration_millis_overflow() {
    assert!(duration_millis(Duration::new(u64::MAX, 0)).is_err());
}

#[test]
fn from_millis_zero() {
    let result = from_millis(0).expect("restore deadline");
    assert_eq!(result, UNIX_EPOCH);
}

#[test]
fn from_millis_nonzero() {
    let result = from_millis(99).expect("restore deadline");
    assert_eq!(result, UNIX_EPOCH + Duration::from_millis(99));
}
