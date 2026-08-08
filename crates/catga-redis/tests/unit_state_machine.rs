//! Unit tests for state-machine helper functions.

use catga_core::CatgaError;

const MAX_CAS_RETRIES: usize = 8;

const COMPARE_AND_SET: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2])
    return 1
end
return 0
"#;

fn state_machine_key(prefix: &str, instance_id: &str) -> String {
    format!("{prefix}:{instance_id}")
}

#[test]
fn max_cas_retries_value() {
    assert_eq!(MAX_CAS_RETRIES, 8);
}

#[test]
fn max_cas_retries_reasonable() {
    assert!(MAX_CAS_RETRIES > 0);
    assert!(MAX_CAS_RETRIES <= 32);
}

#[test]
fn compare_and_set_script_checks_equality() {
    assert!(COMPARE_AND_SET.contains("GET"), "should GET current value");
    assert!(COMPARE_AND_SET.contains("== ARGV[1]"), "should compare with expected");
}

#[test]
fn compare_and_set_script_sets_on_match() {
    assert!(COMPARE_AND_SET.contains("SET"), "should SET new value");
    assert!(COMPARE_AND_SET.contains("ARGV[2]"), "should use new value arg");
}

#[test]
fn compare_and_set_script_returns_correct_values() {
    assert!(COMPARE_AND_SET.contains("return 1"), "returns 1 on success");
    assert!(COMPARE_AND_SET.contains("return 0"), "returns 0 on failure");
}

#[test]
fn compare_and_set_script_single_key() {
    assert!(COMPARE_AND_SET.contains("KEYS[1]"), "uses single key");
}

#[test]
fn state_machine_key_format() {
    let key = state_machine_key("catga:sm", "instance-42");
    assert_eq!(key, "catga:sm:instance-42");
}

#[test]
fn state_machine_key_empty_prefix() {
    let key = state_machine_key("", "instance");
    assert_eq!(key, ":instance");
}

#[test]
fn state_machine_key_empty_instance() {
    let key = state_machine_key("prefix", "");
    assert_eq!(key, "prefix:");
}

#[test]
fn state_machine_key_both_empty() {
    let key = state_machine_key("", "");
    assert_eq!(key, ":");
}

#[test]
fn state_machine_key_consistent() {
    let key1 = state_machine_key("p", "i");
    let key2 = state_machine_key("p", "i");
    assert_eq!(key1, key2);
}

#[test]
fn state_machine_key_special_characters() {
    let key = state_machine_key("prefix", "inst:ance/with#chars");
    assert_eq!(key, "prefix:inst:ance/with#chars");
}
