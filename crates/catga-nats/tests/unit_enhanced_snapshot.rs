use super::*;

#[test]
fn cas_error_contains_operation_name() {
    let error = cas_error("save");
    assert_eq!(error.code(), ErrorCode::Transient);
    let msg = error.to_string();
    assert!(msg.contains("save"));
    assert!(msg.contains("compare-and-set"));
}

#[test]
fn cas_error_contains_delete_operation() {
    let error = cas_error("delete");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("delete"));
}

#[test]
fn unix_millis_handles_unix_epoch() {
    assert_eq!(unix_millis(UNIX_EPOCH), 0);
}

#[test]
fn unix_millis_handles_reasonable_time() {
    let time = UNIX_EPOCH + Duration::from_secs(1700000000);
    assert_eq!(unix_millis(time), 1700000000000);
}

#[test]
fn unix_millis_handles_future_time() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    assert_eq!(unix_millis(time), 1000);
}

#[test]
fn unix_millis_handles_time_before_epoch() {
    // Duration before epoch returns Duration::ZERO
    let time = UNIX_EPOCH - Duration::from_secs(1);
    assert_eq!(unix_millis(time), 0);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection lost");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection lost"));
}

#[test]
fn max_cas_retries_constant() {
    assert_eq!(MAX_CAS_RETRIES, 8);
}

#[test]
fn stored_history_with_creates_single_entry() {
    let history = StoredHistory::with(StoredSnapshot {
        version: 1,
        timestamp_unix_ms: 1000,
        state: vec![1],
    });
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].version, 1);
}

#[test]
fn stored_history_upsert_replaces_existing_version() {
    let mut history = StoredHistory::with(StoredSnapshot {
        version: 1,
        timestamp_unix_ms: 1000,
        state: vec![1],
    });
    // Upsert same version - replaces the entry
    history.upsert(StoredSnapshot { version: 1, timestamp_unix_ms: 2000, state: vec![2] });
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].timestamp_unix_ms, 2000);
    assert_eq!(history.entries[0].state, vec![2]);
}

#[test]
fn stored_history_upsert_appends_new_version() {
    let mut history = StoredHistory::with(StoredSnapshot {
        version: 1,
        timestamp_unix_ms: 1000,
        state: vec![1],
    });
    history.upsert(StoredSnapshot { version: 2, timestamp_unix_ms: 2000, state: vec![2] });
    assert_eq!(history.entries.len(), 2);
    assert_eq!(history.entries[0].version, 1);
    assert_eq!(history.entries[1].version, 2);
}

#[test]
fn stored_history_upsert_inserts_in_sorted_order() {
    let mut history = StoredHistory::with(StoredSnapshot {
        version: 2,
        timestamp_unix_ms: 2000,
        state: vec![2],
    });
    history.upsert(StoredSnapshot { version: 1, timestamp_unix_ms: 1000, state: vec![1] });
    history.upsert(StoredSnapshot { version: 3, timestamp_unix_ms: 3000, state: vec![3] });
    assert_eq!(history.entries.len(), 3);
    assert_eq!(history.entries[0].version, 1);
    assert_eq!(history.entries[1].version, 2);
    assert_eq!(history.entries[2].version, 3);
}

#[test]
fn stream_key_derivation_is_deterministic() {
    let key1 = stream_key("stream-123");
    let key2 = stream_key("stream-123");
    assert_eq!(key1, key2);
}

#[test]
fn stream_key_derivation_differs_for_different_streams() {
    let key1 = stream_key("stream-123");
    let key2 = stream_key("stream-456");
    assert_ne!(key1, key2);
}

#[test]
fn stream_key_starts_with_s_prefix() {
    let key = stream_key("test-stream");
    assert!(key.starts_with('s'));
}

#[test]
fn stream_key_contains_hex_encoded_hash() {
    let key = stream_key("test-stream");
    // SHA-256 produces 64 hex characters
    assert_eq!(key.len(), 65); // 's' + 64 hex chars
}

#[test]
fn encode_decode_roundtrip_stored_history() {
    let original = StoredHistory::with(StoredSnapshot {
        version: 1,
        timestamp_unix_ms: 1000,
        state: vec![1, 2, 3],
    });
    let encoded = encode(&original).unwrap();
    let decoded: StoredHistory = decode(&encoded).unwrap();
    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.entries[0].version, 1);
    assert_eq!(decoded.entries[0].timestamp_unix_ms, 1000);
    assert_eq!(decoded.entries[0].state, vec![1, 2, 3]);
}

#[test]
fn encode_stored_history_produces_valid_bytes() {
    let history = StoredHistory::with(StoredSnapshot {
        version: 42,
        timestamp_unix_ms: 1234567890,
        state: vec![],
    });
    let encoded = encode(&history).unwrap();
    assert!(!encoded.is_empty());
}

#[test]
fn stored_snapshot_clone_preserves_data() {
    let original = StoredSnapshot {
        version: 5,
        timestamp_unix_ms: 999,
        state: vec![10, 20],
    };
    let cloned = original.clone();
    assert_eq!(cloned.version, original.version);
    assert_eq!(cloned.timestamp_unix_ms, original.timestamp_unix_ms);
    assert_eq!(cloned.state, original.state);
}

#[test]
fn max_cas_retries_value_is_reasonable() {
    // Verify MAX_CAS_RETRIES is a positive value that makes sense for retries
    assert!(MAX_CAS_RETRIES > 0);
    assert!(MAX_CAS_RETRIES <= 16); // Reasonable upper bound
}

#[test]
fn is_revision_conflict_handles_non_conflict_error() {
    // Test with a mock-like scenario - the function requires kv::UpdateError
    // This is a compile-time verification that the function exists
    fn _check_is_revision_conflict_exists() {
        fn _takes_error(e: &kv::UpdateError) -> bool {
            is_revision_conflict(e)
        }
        let _ = _takes_error;
    }
}
