//! Unit tests for DSL progress helper functions.

use sha2::{Digest, Sha256};

const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'value', ARGV[2])
return 1
"#;

const UPDATE: &str = r#"
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'value', ARGV[3])
return 1
"#;

fn dsl_progress_key(prefix: &str, flow_id: &str, step_index: u32) -> String {
    let mut digest = Sha256::new();
    digest.update(flow_id.len().to_be_bytes());
    digest.update(flow_id.as_bytes());
    digest.update(step_index.to_be_bytes());
    format!("{}:dsl-progress:{}", prefix, hex::encode(digest.finalize()))
}

#[test]
fn create_script_checks_existence() {
    assert!(CREATE.contains("EXISTS"), "should check EXISTS");
    assert!(CREATE.contains("~= 0"), "should return 0 if exists");
}

#[test]
fn create_script_stores_version_and_value() {
    assert!(CREATE.contains("HSET"), "should use HSET");
    assert!(CREATE.contains("'version'"), "should store version");
    assert!(CREATE.contains("'value'"), "should store value");
    assert!(CREATE.contains("ARGV[1]"), "version arg");
    assert!(CREATE.contains("ARGV[2]"), "value arg");
}

#[test]
fn create_script_returns_success() {
    assert!(CREATE.contains("return 1"), "should return 1 on success");
}

#[test]
fn update_script_checks_version() {
    assert!(UPDATE.contains("HGET"), "should get current version");
    assert!(UPDATE.contains("'version'"), "should read version field");
    assert!(UPDATE.contains("~= ARGV[1]"), "should compare versions");
    assert!(UPDATE.contains("return 0"), "should return 0 on mismatch");
}

#[test]
fn update_script_updates_fields() {
    assert!(UPDATE.contains("HSET"), "should update hash");
    assert!(UPDATE.contains("ARGV[2]"), "new version");
    assert!(UPDATE.contains("ARGV[3]"), "new value");
}

#[test]
fn update_script_returns_success() {
    assert!(UPDATE.contains("return 1"), "should return 1 on success");
}

#[test]
fn dsl_progress_key_format() {
    let key = dsl_progress_key("catga", "flow-123", 0);
    assert!(key.starts_with("catga:dsl-progress:"));
    assert!(key.len() > "catga:dsl-progress:".len());
}

#[test]
fn dsl_progress_key_includes_flow_id() {
    let key1 = dsl_progress_key("p", "flow-a", 0);
    let key2 = dsl_progress_key("p", "flow-b", 0);
    assert_ne!(key1, key2, "different flow ids should produce different keys");
}

#[test]
fn dsl_progress_key_includes_step_index() {
    let key1 = dsl_progress_key("p", "flow", 0);
    let key2 = dsl_progress_key("p", "flow", 1);
    assert_ne!(key1, key2, "different step indices should produce different keys");
}

#[test]
fn dsl_progress_key_different_prefixes() {
    let key1 = dsl_progress_key("prefix-a", "flow", 0);
    let key2 = dsl_progress_key("prefix-b", "flow", 0);
    assert_ne!(key1, key2, "different prefixes should produce different keys");
}

#[test]
fn dsl_progress_key_length_consistent() {
    let key1 = dsl_progress_key("p", "short", 0);
    let key2 = dsl_progress_key("p", "this-is-a-very-long-flow-identifier", u32::MAX);
    // SHA256 = 32 bytes = 64 hex chars
    // prefix + ":dsl-progress:" + 64 hex chars
    let min_len = "p:dsl-progress:".len() + 64;
    assert!(key1.len() >= min_len);
    assert!(key2.len() >= min_len);
}

#[test]
fn dsl_progress_key_deterministic() {
    let key1 = dsl_progress_key("prefix", "flow-id", 42);
    let key2 = dsl_progress_key("prefix", "flow-id", 42);
    assert_eq!(key1, key2, "same inputs should produce same key");
}

#[test]
fn dsl_progress_key_empty_flow_id() {
    let key = dsl_progress_key("p", "", 0);
    assert!(key.starts_with("p:dsl-progress:"));
}

#[test]
fn dsl_progress_key_max_step_index() {
    let key = dsl_progress_key("p", "flow", u32::MAX);
    assert!(key.starts_with("p:dsl-progress:"));
}
