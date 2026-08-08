use super::*;

#[test]
fn max_index_page_entries_constant() {
    assert_eq!(MAX_INDEX_PAGE_ENTRIES, 32);
}

#[test]
fn max_index_page_entries_is_reasonable() {
    assert!(MAX_INDEX_PAGE_ENTRIES > 0);
    assert!(MAX_INDEX_PAGE_ENTRIES <= 256);
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
fn type_index_page_key_format() {
    let key = type_page_key("orders", 5);
    assert!(key.starts_with('p'));
    assert!(key.contains('.'));
}

#[test]
fn type_metadata_key_format() {
    let key = type_metadata_key("orders");
    assert!(key.starts_with('m'));
    assert!(key.len() > 1);
}

#[test]
fn type_marker_key_format() {
    let key = type_marker_key("orders", "flow-123");
    assert!(key.starts_with('i'));
    assert!(key.contains('.'));
}

#[test]
fn flow_key_hash_length() {
    assert_eq!(flow_key("test").len(), 65);
}

#[test]
fn next_index_cursor_wraps_at_tail_page() {
    let metadata = TypeIndex {
        tail_page: 3,
        scan_page: 3,
        scan_offset: 0,
    };
    let result = next_index_cursor(&metadata, false).expect("should wrap");
    assert_eq!(result.scan_page, 0);
    assert_eq!(result.scan_offset, 0);
}

#[test]
fn next_index_cursor_offset_increments() {
    let metadata = TypeIndex {
        tail_page: 3,
        scan_page: 1,
        scan_offset: 10,
    };
    let result = next_index_cursor(&metadata, true).expect("should increment");
    assert_eq!(result.scan_page, 1);
    assert_eq!(result.scan_offset, 11);
}

#[test]
fn next_index_cursor_page_transition() {
    let metadata = TypeIndex {
        tail_page: 5,
        scan_page: 3,
        scan_offset: 31,
    };
    let result = next_index_cursor(&metadata, false).expect("should advance page");
    assert_eq!(result.scan_page, 4);
    assert_eq!(result.scan_offset, 0);
}

#[test]
fn is_stale_with_zero_duration() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = SystemTime::UNIX_EPOCH;
    assert!(is_stale(heartbeat, now, Duration::ZERO));
}

#[test]
fn is_stale_with_future_time() {
    let heartbeat = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2000);
    assert!(is_stale(heartbeat, now, Duration::from_secs(1)));
}

#[test]
fn cas_error_contains_operation_name() {
    let err = cas_error("test_operation");
    let msg = err.to_string();
    assert!(msg.contains("test_operation"));
    assert_eq!(err.code(), ErrorCode::Transient);
}

#[test]
fn indexed_flow_variants() {
    let candidate = IndexedFlow::Candidate(Box::from("flow-1"));
    let advanced = IndexedFlow::Advanced;
    let absent = IndexedFlow::Absent;
    assert!(matches!(candidate, IndexedFlow::Candidate(_)));
    assert!(matches!(advanced, IndexedFlow::Advanced));
    assert!(matches!(absent, IndexedFlow::Absent));
}

#[test]
fn next_index_cursor_offset_overflow() {
    let metadata = TypeIndex {
        tail_page: 5,
        scan_page: 1,
        scan_offset: u32::MAX,
    };
    let result = next_index_cursor(&metadata, true);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("offset overflowed"));
}

#[test]
fn next_index_cursor_page_overflow_unreachable() {
    let metadata = TypeIndex {
        tail_page: 10,
        scan_page: u64::MAX,
        scan_offset: 5,
    };
    let result = next_index_cursor(&metadata, false);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().scan_page, 0);
}

#[test]
fn next_index_cursor_preserves_tail_page() {
    let metadata = TypeIndex {
        tail_page: 7,
        scan_page: 3,
        scan_offset: 15,
    };
    let result = next_index_cursor(&metadata, false).expect("should succeed");
    assert_eq!(result.tail_page, 7);
    assert_eq!(result.scan_page, 4);
    assert_eq!(result.scan_offset, 0);

    let result = next_index_cursor(&result, true).expect("should succeed");
    assert_eq!(result.tail_page, 7);
    assert_eq!(result.scan_page, 4);
    assert_eq!(result.scan_offset, 1);
}

#[test]
fn validate_index_page_rejects_overflow() {
    let oversized: Vec<Box<str>> = (0..=MAX_INDEX_PAGE_ENTRIES)
        .map(|i| Box::from(format!("id-{}", i)))
        .collect();
    let result = validate_index_page(&oversized);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("exceeds"));
}

#[test]
fn validate_index_page_accepts_exact_limit() {
    let exact_limit: Vec<Box<str>> = (0..MAX_INDEX_PAGE_ENTRIES)
        .map(|i| Box::from(format!("id-{}", i)))
        .collect();
    assert!(validate_index_page(&exact_limit).is_ok());
}

#[test]
fn validate_index_page_accepts_empty() {
    let empty: Vec<Box<str>> = Vec::new();
    assert!(validate_index_page(&empty).is_ok());
}

#[test]
fn type_page_key_with_different_pages() {
    let key0 = type_page_key("orders", 0);
    let key1 = type_page_key("orders", 1);
    let key_large = type_page_key("orders", u64::MAX);

    assert_ne!(key0, key1);
    assert_ne!(key0, key_large);
    assert!(key0.contains(".0"));
    assert!(key1.contains(".1"));
    assert!(key_large.contains(".18446744073709551615"));
}

#[test]
fn type_metadata_key_is_unique() {
    let key1 = type_metadata_key("type-a");
    let key2 = type_metadata_key("type-b");
    assert_ne!(key1, key2);
}

#[test]
fn type_marker_key_with_different_ids() {
    let key1 = type_marker_key("orders", "id-1");
    let key2 = type_marker_key("orders", "id-2");
    assert_ne!(key1, key2);
    assert!(key1.starts_with('i'));
    assert!(key2.starts_with('i'));
}

#[test]
fn flow_key_deterministic() {
    let key1 = flow_key("my-flow");
    let key2 = flow_key("my-flow");
    assert_eq!(key1, key2);
}

#[test]
fn flow_key_empty_string() {
    let key = flow_key("");
    assert_eq!(key.len(), 65);
    assert!(key.starts_with('f'));
}

#[test]
fn flow_key_unicode() {
    let key = flow_key("日本語");
    assert!(key.starts_with('f'));
    assert_eq!(key.len(), 65);
}

#[test]
fn type_index_default() {
    let index = TypeIndex::default();
    assert_eq!(index.tail_page, 0);
    assert_eq!(index.scan_page, 0);
    assert_eq!(index.scan_offset, 0);
}

#[test]
fn index_marker_new() {
    let marker = IndexMarker { page: 42 };
    assert_eq!(marker.page, 42);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection lost");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection lost"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().is_empty());
}

#[test]
fn map_error_includes_nats_details() {
    let error = map_error("jetstream error: stream not found");
    assert!(error.to_string().contains("jetstream"));
}

#[test]
fn is_stale_with_elapsed_time() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = heartbeat + Duration::from_secs(5);
    assert!(is_stale(heartbeat, now, Duration::from_secs(5)));
    assert!(!is_stale(heartbeat, now, Duration::from_secs(10)));
}

#[test]
fn is_stale_with_very_old_heartbeat() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = heartbeat + Duration::from_secs(1_000_000);
    assert!(is_stale(heartbeat, now, Duration::from_secs(1)));
}

#[test]
fn is_stale_with_immediate_duration() {
    let heartbeat = SystemTime::now();
    let now = heartbeat + Duration::from_nanos(1);
    assert!(is_stale(heartbeat, now, Duration::ZERO));
}

#[test]
fn cas_error_format() {
    let err = cas_error("update_flow");
    let msg = err.to_string();
    assert!(msg.contains("update_flow"));
    assert!(msg.contains("compare-and-set"));
    assert_eq!(err.code(), ErrorCode::Transient);
}

#[test]
fn cas_error_with_long_operation() {
    let err = cas_error("prune_index_with_cleanup");
    assert!(err.to_string().contains("prune_index_with_cleanup"));
}
