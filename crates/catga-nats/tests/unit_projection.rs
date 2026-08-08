use catga_nats::projection::{
    cas_error, definition_key, lease_key, map_error, encode, decode, LeaseRecord,
    StoredSubscription, DEFAULT_LEASE_TTL, MAX_CAS_RETRIES,
};
use catga_core::{PersistentSubscription, SubscriptionCheckpoint, ErrorCode};
use std::time::Duration;

#[test]
fn stored_subscriptions_round_trip_definitions_and_upsert_checkpoints() {
    let definition =
        PersistentSubscription::new("orders", "orders-*").with_event_types(["created", "paid"]);
    let mut stored = StoredSubscription::from(definition.clone());
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-a", 3));
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-a", 4));
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-b", 1));
    assert_eq!(stored.checkpoints.len(), 2);
    assert_eq!(stored.checkpoints[0].version, 4);

    let encoded = encode(&stored).expect("encode subscription");
    let decoded: StoredSubscription = decode(&encoded).expect("decode subscription");
    let restored = PersistentSubscription::from(decoded);
    assert_eq!(restored.name(), definition.name());
    assert_eq!(restored.stream_pattern(), definition.stream_pattern());
    assert_eq!(restored.event_types(), definition.event_types());
    assert!(decode::<StoredSubscription>(b"invalid subscription").is_err());
}

#[test]
fn lease_records_expire_after_positive_ttl_and_hash_keys_stably() {
    let lease = LeaseRecord::new("consumer-a", Duration::from_secs(1));
    assert_eq!(lease.owner.as_ref(), "consumer-a");
    assert!(!lease.is_expired());
    let expired = LeaseRecord {
        owner: "consumer-b".into(),
        expires_at_unix_ms: 0,
    };
    assert!(expired.is_expired());
    assert!(LeaseRecord::new("consumer-c", Duration::ZERO).expires_at_unix_ms > catga_nats::projection::now_millis());

    assert!(definition_key("orders").starts_with('d'));
    assert!(lease_key("orders").starts_with('l'));
    assert_ne!(definition_key("orders"), definition_key("payments"));
    assert_ne!(lease_key("orders"), lease_key("payments"));
    assert_eq!(map_error("NATS unavailable").code(), ErrorCode::Transient);
    assert!(
        cas_error("release lease")
            .message()
            .contains("release lease")
    );
}

#[test]
fn default_lease_ttl_constant() {
    assert_eq!(DEFAULT_LEASE_TTL, Duration::from_secs(30));
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
fn definition_key_format() {
    let key = definition_key("my-subscription");
    assert!(key.starts_with('d'));
    assert!(key.len() > 1);
}

#[test]
fn lease_key_format() {
    let key = lease_key("my-subscription");
    assert!(key.starts_with('l'));
    assert!(key.len() > 1);
}

#[test]
fn definition_key_hash_length_consistent() {
    // SHA256 produces 32 bytes = 64 hex chars + 'd' prefix = 65
    assert_eq!(definition_key("short").len(), 65);
    assert_eq!(definition_key("this-is-a-very-long-subscription-name").len(), 65);
}

#[test]
fn lease_key_hash_length_consistent() {
    // SHA256 produces 32 bytes = 64 hex chars + 'l' prefix = 65
    assert_eq!(lease_key("short").len(), 65);
    assert_eq!(lease_key("this-is-a-very-long-subscription-name").len(), 65);
}

#[test]
fn definition_key_deterministic() {
    let key1 = definition_key("orders");
    let key2 = definition_key("orders");
    assert_eq!(key1, key2);
}

#[test]
fn lease_key_deterministic() {
    let key1 = lease_key("orders");
    let key2 = lease_key("orders");
    assert_eq!(key1, key2);
}

#[test]
fn lease_record_new_sets_correct_expiry() {
    let lease = LeaseRecord::new("test-owner", Duration::from_secs(60));
    let expected_min = catga_nats::projection::now_millis() + 60_000;
    let expected_max = expected_min + 10_000;
    assert!(
        lease.expires_at_unix_ms >= expected_min
            && lease.expires_at_unix_ms <= expected_max,
        "lease expiry should be within expected range"
    );
}

#[test]
fn lease_record_is_expired_edge_cases() {
    // Not expired
    let future = LeaseRecord {
        owner: "owner".into(),
        expires_at_unix_ms: u64::MAX,
    };
    assert!(!future.is_expired());

    // Expired
    let past = LeaseRecord {
        owner: "owner".into(),
        expires_at_unix_ms: 0,
    };
    assert!(past.is_expired());
}

#[test]
fn stored_subscription_checkpoint_upsert_logic() {
    let definition = PersistentSubscription::new("orders", "orders-*");
    let mut stored = StoredSubscription::from(definition);

    // First checkpoint for stream-a
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-a", 5));
    assert_eq!(stored.checkpoints.len(), 1);

    // Second checkpoint for same stream should update (not add)
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-a", 10));
    assert_eq!(stored.checkpoints.len(), 1);
    assert_eq!(stored.checkpoints[0].version, 10);

    // New stream should add
    stored.save_checkpoint(SubscriptionCheckpoint::new("orders", "stream-b", 3));
    assert_eq!(stored.checkpoints.len(), 2);
}

#[test]
fn map_error_creates_transient_error() {
    let err = map_error("connection timeout");
    assert_eq!(err.code(), ErrorCode::Transient);
    assert!(err.to_string().contains("connection timeout"));
}

#[test]
fn map_error_handles_empty_string() {
    let err = map_error("");
    assert_eq!(err.code(), ErrorCode::Transient);
}

#[test]
fn cas_error_contains_operation() {
    let err = cas_error("test_op");
    assert!(err.to_string().contains("test_op"));
    assert_eq!(err.code(), ErrorCode::Transient);
}
