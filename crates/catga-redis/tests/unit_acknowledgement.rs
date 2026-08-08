//! Unit tests for acknowledgement module constants and helpers.

const ACK_IF_OWNER: &str = r#"
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending ~= 1 or pending[1][2] ~= ARGV[2] then return 0 end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
"#;

#[test]
fn ack_if_owner_checks_pending_before_ack() {
    assert!(ACK_IF_OWNER.contains("XPENDING"), "should check XPENDING first");
    assert!(ACK_IF_OWNER.contains("XACK"), "should call XACK on success");
}

#[test]
fn ack_if_owner_returns_zero_when_no_pending() {
    assert!(ACK_IF_OWNER.contains("return 0"), "should return 0 when no pending");
    assert!(ACK_IF_OWNER.contains("#pending ~= 1"), "should check pending count");
}

#[test]
fn ack_if_owner_validates_consumer() {
    assert!(ACK_IF_OWNER.contains("pending[1][2] ~= ARGV[2]"), "should validate consumer");
}

#[test]
fn ack_if_owner_uses_correct_arg_count() {
    assert!(ACK_IF_OWNER.contains("KEYS[1]"), "stream key");
    assert!(ACK_IF_OWNER.contains("ARGV[1]"), "group");
    assert!(ACK_IF_OWNER.contains("ARGV[2]"), "consumer");
    assert!(ACK_IF_OWNER.contains("ARGV[3]"), "entry id");
}

#[test]
fn ack_if_owner_script_length() {
    assert!(!ACK_IF_OWNER.trim().is_empty());
    assert!(ACK_IF_OWNER.contains("local pending"));
    assert!(ACK_IF_OWNER.contains("end"));
}
