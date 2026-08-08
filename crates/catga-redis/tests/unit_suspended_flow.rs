//! Unit tests for suspended_flow module helper functions.

use std::time::SystemTime;

fn key(prefix: &str, flow_id: &str) -> String {
    format!("{}:{flow_id}", prefix)
}

fn timeout_key(prefix: &str, suffix: &str) -> String {
    format!("{}.__timeout_{suffix}", prefix)
}

fn records_key(prefix: &str) -> String {
    format!("{}.__records", prefix)
}

fn wait_correlation_key(prefix: &str, correlation_id: &str) -> String {
    format!("{}.__wait_correlation:{correlation_id}", prefix)
}

fn system_time_unix_ms(time: SystemTime) -> Result<u64, String> {
    let elapsed = time.duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| "precedes Unix epoch".to_string())?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| "exceeds range".to_string())
}

#[test]
fn key_format() {
    let prefix = "catga:flows";
    let flow_id = "flow-123";
    let expected = format!("{}:{}", prefix, flow_id);
    assert_eq!(expected, "catga:flows:flow-123");
}

#[test]
fn key_with_special_chars() {
    let prefix = "prefix";
    let flow_id = "flow:with:colons";
    let expected = format!("{}:{}", prefix, flow_id);
    assert_eq!(expected, "prefix:flow:with:colons");
}

#[test]
fn timeout_key_format() {
    let prefix = "catga:flows";
    let suffix = "due";
    let expected = format!("{}.__timeout_{}", prefix, suffix);
    assert_eq!(expected, "catga:flows.__timeout_due");
}

#[test]
fn timeout_key_inflight() {
    let prefix = "prefix";
    let suffix = "inflight";
    let expected = format!("{}.__timeout_{}", prefix, suffix);
    assert_eq!(expected, "prefix.__timeout_inflight");
}

#[test]
fn records_key_format() {
    let prefix = "catga:flows";
    let expected = format!("{}.__records", prefix);
    assert_eq!(expected, "catga:flows.__records");
}

#[test]
fn wait_correlation_key_format() {
    let prefix = "catga:flows";
    let correlation_id = "corr-abc";
    let expected = format!("{}.__wait_correlation:{}", prefix, correlation_id);
    assert_eq!(expected, "catga:flows.__wait_correlation:corr-abc");
}

#[test]
fn wait_correlation_key_empty_id() {
    let prefix = "prefix";
    let correlation_id = "";
    let expected = format!("{}.__wait_correlation:{}", prefix, correlation_id);
    assert_eq!(expected, "prefix.__wait_correlation:");
}

#[test]
fn system_time_unix_ms_valid() {
    let time = SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1000);
    let result = system_time_unix_ms(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn system_time_unix_ms_at_epoch() {
    let result = system_time_unix_ms(SystemTime::UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn system_time_unix_ms_before_epoch_error() {
    let before_epoch = SystemTime::UNIX_EPOCH - std::time::Duration::from_secs(1);
    let result = system_time_unix_ms(before_epoch);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unix epoch"));
}
