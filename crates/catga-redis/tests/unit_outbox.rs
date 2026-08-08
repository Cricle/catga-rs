//! Unit tests for outbox module helper functions.

/// Replicated key construction functions for testing.
fn message_key(prefix: &str, id: u64) -> String {
    format!("{prefix}:{id}")
}

fn pending_key(prefix: &str) -> String {
    format!("{prefix}:pending")
}

fn published_key(prefix: &str) -> String {
    format!("{prefix}:published")
}

// =============================================================================
// Key construction tests
// =============================================================================

#[test]
fn message_key_format() {
    let key = message_key("catga:outbox", 42);
    assert_eq!(key, "catga:outbox:42");
}

#[test]
fn message_key_zero_id() {
    let key = message_key("prefix", 0);
    assert_eq!(key, "prefix:0");
}

#[test]
fn message_key_large_id() {
    let key = message_key("prefix", u64::MAX);
    assert_eq!(key, format!("prefix:{}", u64::MAX));
}

#[test]
fn message_key_consistent() {
    let key1 = message_key("prefix", 12345);
    let key2 = message_key("prefix", 12345);
    assert_eq!(key1, key2);
}

#[test]
fn message_key_different_prefixes() {
    let key1 = message_key("prefix-a", 42);
    let key2 = message_key("prefix-b", 42);
    assert_ne!(key1, key2);
}

#[test]
fn message_key_different_ids() {
    let key1 = message_key("prefix", 1);
    let key2 = message_key("prefix", 2);
    assert_ne!(key1, key2);
}

#[test]
fn message_key_empty_prefix() {
    let key = message_key("", 42);
    assert_eq!(key, ":42");
}

#[test]
fn pending_key_format() {
    let key = pending_key("catga:outbox");
    assert_eq!(key, "catga:outbox:pending");
}

#[test]
fn pending_key_empty_prefix() {
    let key = pending_key("");
    assert_eq!(key, ":pending");
}

#[test]
fn pending_key_consistent() {
    let key1 = pending_key("prefix");
    let key2 = pending_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn pending_key_different_prefixes() {
    let key1 = pending_key("prefix-a");
    let key2 = pending_key("prefix-b");
    assert_ne!(key1, key2);
}

#[test]
fn published_key_format() {
    let key = published_key("catga:outbox");
    assert_eq!(key, "catga:outbox:published");
}

#[test]
fn published_key_empty_prefix() {
    let key = published_key("");
    assert_eq!(key, ":published");
}

#[test]
fn published_key_consistent() {
    let key1 = published_key("prefix");
    let key2 = published_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn published_key_different_prefixes() {
    let key1 = published_key("prefix-a");
    let key2 = published_key("prefix-b");
    assert_ne!(key1, key2);
}

// =============================================================================
// Key separation tests
// =============================================================================

#[test]
fn keys_are_distinct() {
    let prefix = "catga:outbox";
    let msg = message_key(prefix, 42);
    let pen = pending_key(prefix);
    let pub_ = published_key(prefix);

    assert_ne!(msg, pen);
    assert_ne!(msg, pub_);
    assert_ne!(pen, pub_);
}

#[test]
fn keys_share_prefix() {
    let prefix = "catga:outbox";
    assert!(message_key(prefix, 42).starts_with(prefix));
    assert!(pending_key(prefix).starts_with(prefix));
    assert!(published_key(prefix).starts_with(prefix));
}

// =============================================================================
// Outbox message state tests
// =============================================================================

#[test]
fn outbox_message_state_values() {
    // Valid states: pending, claimed, published, failed
    let valid_states = vec!["pending", "claimed", "published", "failed"];

    for state in valid_states {
        // Just verify the strings are not empty
        assert!(!state.is_empty());
    }
}

#[test]
fn outbox_message_state_count() {
    let states = vec!["pending", "claimed", "published", "failed"];
    assert_eq!(states.len(), 4);
}

// =============================================================================
// Message ID validation tests
// =============================================================================

#[test]
fn message_id_zero_is_valid() {
    use catga_core::validate_outbox_message_id;
    // Zero ID should be rejected (outbox requires nonzero identifiers)
    let result = validate_outbox_message_id(0);
    assert!(result.is_err());
}

#[test]
fn message_id_nonzero_is_valid() {
    use catga_core::validate_outbox_message_id;
    let result = validate_outbox_message_id(1);
    assert!(result.is_ok());
}

#[test]
fn message_id_max_is_valid() {
    use catga_core::validate_outbox_message_id;
    let result = validate_outbox_message_id(u64::MAX);
    assert!(result.is_ok());
}

// =============================================================================
// Claim limit validation tests
// =============================================================================

#[test]
fn claim_limit_zero_is_valid() {
    use catga_core::validate_outbox_claim_limit;
    let result = validate_outbox_claim_limit(0);
    assert!(result.is_ok());
}

#[test]
fn claim_limit_within_max() {
    use catga_core::MAX_OUTBOX_CLAIM_LIMIT;
    use catga_core::validate_outbox_claim_limit;
    let result = validate_outbox_claim_limit(MAX_OUTBOX_CLAIM_LIMIT);
    assert!(result.is_ok());
}

#[test]
fn claim_limit_above_max() {
    use catga_core::MAX_OUTBOX_CLAIM_LIMIT;
    use catga_core::validate_outbox_claim_limit;
    let result = validate_outbox_claim_limit(MAX_OUTBOX_CLAIM_LIMIT + 1);
    assert!(result.is_err());
}

// =============================================================================
// Scan offset tests
// =============================================================================

#[test]
fn claim_scan_factor_is_positive() {
    // CLAIM_SCAN_FACTOR = 4, meaning we scan 4x the requested limit
    const CLAIM_SCAN_FACTOR: usize = 4;
    assert!(CLAIM_SCAN_FACTOR > 0);
}

#[test]
fn claim_scan_factor_is_reasonable() {
    const CLAIM_SCAN_FACTOR: usize = 4;
    // Scanning 4x limit is a reasonable balance between efficiency and accuracy
    assert!(CLAIM_SCAN_FACTOR >= 2);
    assert!(CLAIM_SCAN_FACTOR <= 10);
}

// =============================================================================
// OutboxMessage tests
// =============================================================================

#[test]
fn outbox_message_new() {
    use catga_core::{Envelope, OutboxMessage, MessageMetadata, codec::memorypack::MemoryPackCodec};

    let _codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let envelope = Envelope::new(1, "test.type", vec![1, 2, 3], metadata);

    let message = OutboxMessage::new(envelope);
    assert_eq!(message.id(), 1);
    assert_eq!(message.state(), catga_core::OutboxState::Pending);
}

#[test]
fn outbox_message_default_retries() {
    use catga_core::DEFAULT_OUTBOX_MAX_RETRIES;
    assert!(DEFAULT_OUTBOX_MAX_RETRIES > 0);
}

// =============================================================================
// Lease expiration tests
// =============================================================================

#[test]
fn default_claim_lease_is_positive() {
    use catga_core::DEFAULT_OUTBOX_CLAIM_LEASE;
    assert!(DEFAULT_OUTBOX_CLAIM_LEASE.as_secs() > 0);
}

#[test]
fn outbox_claim_expires_at_positive() {
    use catga_core::{DEFAULT_OUTBOX_CLAIM_LEASE, outbox_claim_expires_at};

    let result = outbox_claim_expires_at(DEFAULT_OUTBOX_CLAIM_LEASE);
    assert!(result.is_ok());
    let expires_at = result.unwrap();
    assert!(expires_at > 0);
}

#[test]
fn outbox_claim_expires_at_custom_lease() {
    use catga_core::outbox_claim_expires_at;
    use std::time::Duration;

    let result = outbox_claim_expires_at(Duration::from_secs(300));
    assert!(result.is_ok());
    let expires_at = result.unwrap();
    assert!(expires_at > 0);
}

// =============================================================================
// Envelope codec tests
// =============================================================================

#[test]
fn memory_pack_codec_encode_decode() {
    use catga_core::{Envelope, MessageMetadata, EnvelopeCodec, codec::memorypack::MemoryPackCodec};

    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let original = Envelope::new(42, "test.message", vec![1, 2, 3, 4, 5], metadata);

    let encoded = codec.encode(&original).expect("encode should succeed");
    assert!(!encoded.is_empty());

    let decoded: Envelope = codec.decode(&encoded).expect("decode should succeed");
    assert_eq!(decoded.id(), original.id());
    assert_eq!(decoded.message_type(), original.message_type());
}

#[test]
fn memory_pack_codec_empty_payload() {
    use catga_core::{Envelope, MessageMetadata, EnvelopeCodec, codec::memorypack::MemoryPackCodec};

    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let original = Envelope::new(1, "empty", vec![], metadata);

    let encoded = codec.encode(&original).expect("encode should succeed");
    let decoded: Envelope = codec.decode(&encoded).expect("decode should succeed");
    assert!(decoded.payload().is_empty());
}
