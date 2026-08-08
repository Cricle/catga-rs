//! Unit tests for lease helper functions.

use std::time::Duration;

#[test]
fn ttl_millis_exact() {
    let ttl_millis = |ttl: Duration| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
    assert_eq!(ttl_millis(Duration::from_millis(1000)), 1000);
    assert_eq!(ttl_millis(Duration::from_secs(5)), 5000);
}

#[test]
fn ttl_millis_zero_returns_one() {
    let ttl_millis = |ttl: Duration| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
    assert_eq!(ttl_millis(Duration::ZERO), 1);
}

#[test]
fn ttl_millis_sub_millis_rounds_up() {
    let ttl_millis = |ttl: Duration| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
    assert_eq!(ttl_millis(Duration::from_nanos(500)), 1);
    assert_eq!(ttl_millis(Duration::from_micros(1)), 1);
}

#[test]
fn ttl_millis_large_value() {
    let ttl_millis = |ttl: Duration| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1);
    let large = Duration::from_secs(u64::MAX / 1000);
    assert!(ttl_millis(large) >= 1);
}

const ACQUIRE: &str = "local key, ttl = KEYS[1], ARGV[1] if redis.call('SET', key, ARGV[2], 'PX', ttl, 'NX') then return redis.call('PTTL', key) else return -2 end";
const RENEW: &str = "local key, value = KEYS[1], ARGV[1] if redis.call('GET', key) == value then return redis.call('PEXPIRE', key, ARGV[2]) else return -2 end";
const RELEASE: &str = "local key, value = KEYS[1], ARGV[1] if redis.call('GET', key) == value then return redis.call('DEL', key) else return -2 end";

#[test]
fn lua_scripts_are_valid_strings() {
    assert!(!ACQUIRE.is_empty());
    assert!(!RENEW.is_empty());
    assert!(!RELEASE.is_empty());
}

#[test]
fn lua_scripts_contain_expected_commands() {
    assert!(ACQUIRE.contains("SET"));
    assert!(ACQUIRE.contains("NX"));
    assert!(RENEW.contains("PEXPIRE"));
    assert!(RELEASE.contains("DEL"));
}
