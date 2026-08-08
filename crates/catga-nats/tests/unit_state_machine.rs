use super::*;

#[test]
fn kv_key_produces_consistent_hashes() {
    let key1 = kv_key("instance-123");
    let key2 = kv_key("instance-123");
    assert_eq!(key1, key2);
    assert!(key1.starts_with('s'));
}

#[test]
fn kv_key_differs_for_different_instance_ids() {
    let key1 = kv_key("instance-A");
    let key2 = kv_key("instance-B");
    assert_ne!(key1, key2);
}

#[test]
fn kv_key_hash_length_is_consistent() {
    let key1 = kv_key("short");
    let key2 = kv_key("this-is-a-very-long-instance-identifier");
    // SHA256 produces 32 bytes = 64 hex chars, plus 's' prefix
    assert_eq!(key1.len(), 65);
    assert_eq!(key2.len(), 65);
}

#[test]
fn max_cas_retries_constant() {
    assert_eq!(MAX_CAS_RETRIES, 8);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection refused");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection refused"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn map_error_includes_nats_error_details() {
    let error = map_error("jetstream error: wrong last sequence");
    assert!(error.to_string().contains("jetstream"));
}

#[test]
fn kv_key_prefix_is_s() {
    let key = kv_key("test-instance");
    assert!(key.starts_with('s'));
}

#[test]
fn kv_key_hex_encoding() {
    let key = kv_key("");
    // SHA256 of empty = e3b0c44298fc1c149afbf4c8996fb924... (64 hex chars)
    assert!(key.starts_with("s"));
    assert_eq!(key.len(), 65); // 's' + 64 hex chars
}

#[test]
fn kv_key_deterministic() {
    let id = "consistent-id";
    let key1 = kv_key(id);
    let key2 = kv_key(id);
    assert_eq!(key1, key2);
}

#[test]
fn kv_key_unique_per_instance() {
    let key_a = kv_key("instance-a");
    let key_b = kv_key("instance-b");
    assert_ne!(key_a, key_b);
}
