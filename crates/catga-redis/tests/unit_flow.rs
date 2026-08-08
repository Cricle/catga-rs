//! Unit tests for flow module helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use sha2::{Digest, Sha256};

fn flow_key(prefix: &str, id: &str) -> String {
    hashed_key(prefix, "flow", id)
}

fn type_index_key(prefix: &str, flow_type: &str) -> String {
    hashed_key(prefix, "flow-type", flow_type)
}

fn hashed_key(prefix: &str, kind: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(value.len().to_be_bytes());
    digest.update(value.as_bytes());
    format!("{prefix}:{kind}:{}", hex::encode(digest.finalize()))
}

fn unix_millis(time: SystemTime) -> Result<u64, String> {
    let duration = time.duration_since(UNIX_EPOCH)
        .map_err(|_| "precedes Unix epoch".to_string())?;
    u64::try_from(duration.as_millis())
        .map_err(|_| "exceeds range".to_string())
}

fn stale_before(stale_after: Duration) -> Result<u64, String> {
    let now = unix_millis(SystemTime::now())?;
    let elapsed = stale_after.as_millis().min(u128::from(u64::MAX)) as u64;
    Ok(now.saturating_sub(elapsed))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum FlowStatus {
    Running,
    Compensating,
    Suspended,
    Done,
    Failed,
    Cancelled,
}

const fn status_code(status: FlowStatus) -> &'static str {
    match status {
        FlowStatus::Running => "running",
        FlowStatus::Compensating => "compensating",
        FlowStatus::Suspended => "suspended",
        FlowStatus::Done => "done",
        FlowStatus::Failed => "failed",
        FlowStatus::Cancelled => "cancelled",
    }
}

#[test]
fn flow_key_derives_consistent_hash() {
    let key1 = flow_key("prefix", "flow-123");
    let key2 = flow_key("prefix", "flow-123");
    assert_eq!(key1, key2);
    assert!(key1.starts_with("prefix:flow:"));
}

#[test]
fn flow_key_different_ids_different_keys() {
    let key1 = flow_key("prefix", "flow-1");
    let key2 = flow_key("prefix", "flow-2");
    assert_ne!(key1, key2);
}

#[test]
fn flow_key_different_prefixes_different_keys() {
    let key1 = flow_key("prefix1", "flow-123");
    let key2 = flow_key("prefix2", "flow-123");
    assert_ne!(key1, key2);
}

#[test]
fn flow_key_empty_id() {
    let key = flow_key("prefix", "");
    assert!(key.starts_with("prefix:flow:"));
}

#[test]
fn type_index_key_format() {
    let key = type_index_key("prefix", "order-flow");
    assert!(key.starts_with("prefix:flow-type:"));
}

#[test]
fn type_index_key_consistent() {
    let key1 = type_index_key("prefix", "order-flow");
    let key2 = type_index_key("prefix", "order-flow");
    assert_eq!(key1, key2);
}

#[test]
fn type_index_key_different_types_different_keys() {
    let key1 = type_index_key("prefix", "type-a");
    let key2 = type_index_key("prefix", "type-b");
    assert_ne!(key1, key2);
}

#[test]
fn type_index_key_with_special_chars() {
    let key = type_index_key("prefix", "my.flow/type");
    assert!(key.starts_with("prefix:flow-type:"));
}

#[test]
fn hashed_key_format() {
    let key = hashed_key("prefix", "kind", "value");
    assert!(key.starts_with("prefix:kind:"));
    assert_eq!(key.matches(':').count(), 2);
}

#[test]
fn hashed_key_is_hex_encoded() {
    let key = hashed_key("prefix", "kind", "test");
    let parts: Vec<&str> = key.split(':').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[2].len(), 64);
    assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
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
    assert!(result.unwrap_err().contains("precedes"));
}

#[test]
fn unix_millis_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = unix_millis(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn stale_before_returns_past_timestamp() {
    let stale = Duration::from_secs(30);
    let result = stale_before(stale);
    assert!(result.is_ok());
    let stale_before_time = result.unwrap();
    let now = unix_millis(SystemTime::now()).unwrap();
    assert!(stale_before_time < now);
    assert!(stale_before_time >= now - 31000);
}

#[test]
fn stale_before_zero_duration() {
    let result = stale_before(Duration::ZERO);
    assert!(result.is_ok());
    let stale_before_time = result.unwrap();
    let now = unix_millis(SystemTime::now()).unwrap();
    assert!(stale_before_time <= now);
}

#[test]
fn stale_before_large_duration() {
    let stale = Duration::from_secs(u64::MAX);
    let result = stale_before(stale);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn status_code_running() {
    assert_eq!(status_code(FlowStatus::Running), "running");
}

#[test]
fn status_code_compensating() {
    assert_eq!(status_code(FlowStatus::Compensating), "compensating");
}

#[test]
fn status_code_suspended() {
    assert_eq!(status_code(FlowStatus::Suspended), "suspended");
}

#[test]
fn status_code_done() {
    assert_eq!(status_code(FlowStatus::Done), "done");
}

#[test]
fn status_code_failed() {
    assert_eq!(status_code(FlowStatus::Failed), "failed");
}

#[test]
fn status_code_cancelled() {
    assert_eq!(status_code(FlowStatus::Cancelled), "cancelled");
}
