//! Unit tests for pubsub module helper functions.

use sha2::{Digest, Sha256};

fn deduplication_key_internal(
    channel: &str,
    message_id: u64,
    scope: &[u8],
    receiver_id: Option<&str>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(scope);
    digest.update(channel.len().to_be_bytes());
    digest.update(channel.as_bytes());
    digest.update(message_id.to_be_bytes());
    if scope == b"receive" {
        if let Some(rid) = receiver_id {
            digest.update(rid.as_bytes());
        }
    }
    format!("catga:pubsub:dedup:{}", hex::encode(digest.finalize()))
}

#[test]
fn deduplication_key_format() {
    let key = deduplication_key_internal("test-channel", 42, b"publish", None);
    assert!(key.starts_with("catga:pubsub:dedup:"));
    assert_eq!(key.len(), "catga:pubsub:dedup:".len() + 64);
}

#[test]
fn deduplication_key_is_hex_encoded() {
    let key = deduplication_key_internal("test-channel", 42, b"publish", None);
    let hex_part = &key["catga:pubsub:dedup:".len()..];
    assert_eq!(hex_part.len(), 64);
    assert!(hex_part.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn deduplication_key_different_message_ids() {
    let key1 = deduplication_key_internal("channel", 1, b"publish", None);
    let key2 = deduplication_key_internal("channel", 2, b"publish", None);
    assert_ne!(key1, key2);
}

#[test]
fn deduplication_key_different_channels() {
    let key1 = deduplication_key_internal("channel-a", 42, b"publish", None);
    let key2 = deduplication_key_internal("channel-b", 42, b"publish", None);
    assert_ne!(key1, key2);
}

#[test]
fn deduplication_key_different_scopes() {
    let key1 = deduplication_key_internal("channel", 42, b"publish", None);
    let key2 = deduplication_key_internal("channel", 42, b"receive", None);
    assert_ne!(key1, key2);
}

#[test]
fn deduplication_key_receive_scope_considers_receiver_id() {
    let key1 = deduplication_key_internal("channel", 42, b"receive", Some("recv-1"));
    let key2 = deduplication_key_internal("channel", 42, b"receive", Some("recv-2"));
    assert_ne!(key1, key2);
}

#[test]
fn deduplication_key_publish_scope_ignores_receiver_id() {
    let key1 = deduplication_key_internal("channel", 42, b"publish", Some("recv-1"));
    let key2 = deduplication_key_internal("channel", 42, b"publish", Some("recv-2"));
    assert_eq!(key1, key2);
}

#[test]
fn deduplication_key_empty_channel() {
    let key = deduplication_key_internal("", 42, b"publish", None);
    assert!(key.starts_with("catga:pubsub:dedup:"));
}

#[test]
fn deduplication_key_special_chars_in_channel() {
    let key = deduplication_key_internal("channel:with:colons", 42, b"publish", None);
    assert!(key.starts_with("catga:pubsub:dedup:"));
}

#[test]
fn deduplication_key_zero_message_id() {
    let key = deduplication_key_internal("channel", 0, b"publish", None);
    assert!(key.starts_with("catga:pubsub:dedup:"));
}

#[test]
fn deduplication_key_large_message_id() {
    let key = deduplication_key_internal("channel", u64::MAX, b"publish", None);
    assert!(key.starts_with("catga:pubsub:dedup:"));
}

#[test]
fn deduplication_key_consistency() {
    let key1 = deduplication_key_internal("channel", 42, b"publish", None);
    let key2 = deduplication_key_internal("channel", 42, b"publish", None);
    assert_eq!(key1, key2);
}
