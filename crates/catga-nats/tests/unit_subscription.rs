use catga_nats::subscription::{
    cas_error, map_error, unix_millis, encode, decode, projection_key,
};
use catga_nats::subscription::{StoredCheckpoints, StoredCheckpoint};
use catga_core::{ProjectionCheckpoint, ErrorCode};
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn stored_checkpoints_upsert_remove_and_restore_durable_timestamps() {
    let first = ProjectionCheckpoint::from_persisted(
        "order-totals",
        "order-1",
        3,
        UNIX_EPOCH + Duration::from_millis(10),
    );
    let replacement = ProjectionCheckpoint::from_persisted(
        "order-totals",
        "order-1",
        4,
        UNIX_EPOCH + Duration::from_millis(20),
    );
    let second = ProjectionCheckpoint::from_persisted(
        "order-totals",
        "order-2",
        1,
        UNIX_EPOCH + Duration::from_millis(30),
    );
    let mut stored = StoredCheckpoints::with(first);

    stored.save(replacement);
    stored.save(second);
    assert_eq!(stored.checkpoints.len(), 2);
    assert_eq!(stored.checkpoints[0].version, 4);
    assert_eq!(stored.checkpoints[1].stream_id.as_ref(), "order-2");

    let restored = StoredCheckpoint {
        stream_id: stored.checkpoints[0].stream_id.clone(),
        version: stored.checkpoints[0].version,
        updated_at_unix_ms: stored.checkpoints[0].updated_at_unix_ms,
    }
    .into_checkpoint("order-totals");
    assert_eq!(restored.projection_name(), "order-totals");
    assert_eq!(restored.stream_id(), "order-1");
    assert_eq!(restored.version(), 4);
    assert_eq!(
        restored.updated_at(),
        UNIX_EPOCH + Duration::from_millis(20)
    );

    assert!(!stored.remove("missing"));
    assert!(stored.remove("order-1"));
    assert_eq!(stored.checkpoints.len(), 1);
    assert!(stored.remove("order-2"));
    assert!(stored.checkpoints.is_empty());
}

#[test]
fn checkpoint_payload_and_key_encoding_are_stable_and_reject_invalid_payloads() {
    let stored = StoredCheckpoints::with(ProjectionCheckpoint::from_persisted(
        "inventory",
        "sku-42",
        8,
        UNIX_EPOCH + Duration::from_secs(1),
    ));
    let encoded = encode(&stored).expect("encode stored checkpoints");
    let decoded: StoredCheckpoints = decode(&encoded).expect("decode stored checkpoints");
    assert_eq!(decoded.checkpoints.len(), 1);
    assert_eq!(decoded.checkpoints[0].stream_id.as_ref(), "sku-42");
    assert_eq!(decoded.checkpoints[0].version, 8);
    assert_eq!(projection_key("inventory"), projection_key("inventory"));
    assert_ne!(projection_key("inventory"), projection_key("orders"));
    assert!(projection_key("inventory").starts_with('p'));
    assert!(decode::<StoredCheckpoints>(b"not memorypack").is_err());
}

#[test]
fn projection_key_hash_length_consistent() {
    // SHA256 = 64 hex chars + 'p' prefix = 65
    assert_eq!(projection_key("short").len(), 65);
    assert_eq!(projection_key("this-is-a-very-long-projection-name").len(), 65);
}

#[test]
fn projection_key_handles_empty_name() {
    let key = projection_key("");
    assert!(key.starts_with('p'));
    assert_eq!(key.len(), 65);
}

#[test]
fn projection_key_handles_unicode_name() {
    let key = projection_key("投影-123");
    assert!(key.starts_with('p'));
    assert_eq!(key.len(), 65);
}

#[test]
fn unix_millis_handles_before_epoch() {
    // Before epoch returns 0, not negative
    assert_eq!(unix_millis(UNIX_EPOCH - Duration::from_secs(100)), 0);
}

#[test]
fn unix_millis_handles_max_u64() {
    // Very large time should not overflow u64
    let far_future = UNIX_EPOCH + Duration::from_secs(u64::MAX / 1000);
    let millis = unix_millis(far_future);
    assert!(millis > 0);
}

#[test]
fn timestamp_and_error_helpers_keep_failure_information() {
    assert_eq!(unix_millis(UNIX_EPOCH - Duration::from_secs(1)), 0);
    assert_eq!(unix_millis(UNIX_EPOCH + Duration::from_millis(123)), 123);
    assert_eq!(cas_error("save").code(), ErrorCode::Transient);
    assert!(cas_error("delete").message().contains("delete"));
    assert_eq!(map_error("NATS unavailable").code(), ErrorCode::Transient);
    assert_eq!(map_error("NATS unavailable").message(), "NATS unavailable");
}
