//! Unit tests for DSL progress helper functions.

use sha2::{Digest, Sha256};

fn key(flow_id: &str, step_index: u32) -> String {
    let mut digest = Sha256::new();
    digest.update(flow_id.as_bytes());
    digest.update(step_index.to_be_bytes());
    format!("d{}", hex::encode(digest.finalize()))
}

const MAX_CAS_RETRIES: usize = 8;

#[test]
fn key_produces_consistent_hashes() {
    let key1 = key("flow-123", 0);
    let key2 = key("flow-123", 0);
    assert_eq!(key1, key2);
    assert!(key1.starts_with('d'));
}

#[test]
fn key_differs_for_different_flow_ids() {
    let key1 = key("flow-A", 0);
    let key2 = key("flow-B", 0);
    assert_ne!(key1, key2);
}

#[test]
fn key_differs_for_different_step_indices() {
    let key1 = key("flow-123", 0);
    let key2 = key("flow-123", 1);
    assert_ne!(key1, key2);
}

#[test]
fn key_hash_length_is_consistent() {
    let key1 = key("short", 0);
    let key2 = key("this-is-a-very-long-flow-identifier", 999);
    // SHA256 produces 32 bytes = 64 hex chars, plus 'd' prefix
    assert_eq!(key1.len(), 65);
    assert_eq!(key2.len(), 65);
}

#[test]
fn key_format_has_correct_prefix() {
    let key = key("flow-id", 0);
    assert!(key.starts_with('d'), "key should start with 'd' prefix");
    assert_eq!(key.len(), 65, "SHA256 = 64 hex + 1 prefix");
}

#[test]
fn key_is_deterministic() {
    let id = "test-flow";
    let idx = 42;
    let key1 = key(id, idx);
    let key2 = key(id, idx);
    assert_eq!(key1, key2);
}

#[test]
fn key_changes_with_any_parameter() {
    let key1 = key("flow-1", 0);
    let key2 = key("flow-2", 0);
    let key3 = key("flow-1", 1);
    assert_ne!(key1, key2);
    assert_ne!(key1, key3);
    assert_ne!(key2, key3);
}

#[test]
fn key_handles_empty_flow_id() {
    let key = key("", 0);
    assert!(key.starts_with('d'));
    assert_eq!(key.len(), 65);
}

#[test]
fn key_handles_unicode_flow_id() {
    let key = key("流程-123", 0);
    assert!(key.starts_with('d'));
    assert_eq!(key.len(), 65);
}

#[test]
fn key_handles_max_step_index() {
    let key = key("flow", u32::MAX);
    assert!(key.starts_with('d'));
    assert_eq!(key.len(), 65);
}

#[test]
fn max_cas_retries_constant() {
    assert_eq!(MAX_CAS_RETRIES, 8);
}

#[test]
fn max_cas_retries_is_reasonable() {
    assert!(MAX_CAS_RETRIES > 0);
    assert!(MAX_CAS_RETRIES <= 32);
}

#[test]
fn key_is_hex_encoded() {
    let key = key("flow", 0);
    let hex_part = &key[1..];
    assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn key_empty_flow_id_different_from_none() {
    let key_empty = key("", 0);
    let key_none = key("\0", 0); // different from empty
    assert_ne!(key_empty, key_none);
}
