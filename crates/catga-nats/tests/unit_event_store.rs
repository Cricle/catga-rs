use super::*;

#[test]
fn version_constant() {
    assert_eq!(VERSION, "Catga-Version");
}

#[test]
fn timestamp_constant() {
    assert_eq!(TIMESTAMP, "Catga-Timestamp");
}

#[test]
fn batch_count_constant() {
    assert_eq!(BATCH_COUNT, "Catga-Batch-Count");
}

#[test]
fn max_unconditional_append_retries_constant() {
    assert_eq!(MAX_UNCONDITIONAL_APPEND_RETRIES, 64);
}

#[test]
fn max_event_store_history_scan_equals_page_size() {
    assert_eq!(MAX_EVENT_STORE_HISTORY_SCAN, MAX_EVENT_STORE_PAGE_SIZE);
}

#[test]
fn validate_subject_prefix_accepts_valid_prefix() {
    assert!(validate_subject_prefix("events").is_ok());
    assert!(validate_subject_prefix("events.orders").is_ok());
    assert!(validate_subject_prefix("catga.events.orders").is_ok());
    assert!(validate_subject_prefix("A").is_ok());
    assert!(validate_subject_prefix("foo.bar.baz").is_ok());
}

#[test]
fn validate_subject_prefix_rejects_empty() {
    let result = validate_subject_prefix("");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("literal NATS subject tokens"));
}

#[test]
fn validate_subject_prefix_rejects_whitespace_only() {
    let result = validate_subject_prefix("   ");
    assert!(result.is_err());
}

#[test]
fn validate_subject_prefix_rejects_empty_token() {
    let result = validate_subject_prefix("events..orders");
    assert!(result.is_err());
}

#[test]
fn validate_subject_prefix_rejects_wildcard_star() {
    let result = validate_subject_prefix("events.*.orders");
    assert!(result.is_err());
}

#[test]
fn validate_subject_prefix_rejects_wildcard_gt() {
    let result = validate_subject_prefix("events.>");
    assert!(result.is_err());
}

#[test]
fn stream_subjects_cover_prefix_returns_true_for_matching() {
    let stream_subjects = vec!["events.>".to_string()];
    assert!(stream_subjects_cover_prefix(&stream_subjects, "events"));
}

#[test]
fn stream_subjects_cover_prefix_returns_true_for_nested_prefix() {
    let stream_subjects = vec!["events.orders.>".to_string()];
    assert!(stream_subjects_cover_prefix(&stream_subjects, "events.orders"));
}

#[test]
fn stream_subjects_cover_prefix_returns_false_for_non_covering() {
    let stream_subjects = vec!["orders.>".to_string()];
    assert!(!stream_subjects_cover_prefix(&stream_subjects, "events"));
}

#[test]
fn stream_subjects_cover_prefix_returns_false_for_empty_subjects() {
    let stream_subjects: Vec<String> = vec![];
    assert!(!stream_subjects_cover_prefix(&stream_subjects, "events"));
}

#[test]
fn subject_filter_covers_prefix_returns_true_for_exact_match() {
    assert!(subject_filter_covers_prefix("events.>", "events"));
}

#[test]
fn subject_filter_covers_prefix_returns_true_for_wildcard_prefix() {
    assert!(subject_filter_covers_prefix("*.events.>", "foo.events"));
}

#[test]
fn subject_filter_covers_prefix_returns_false_without_trailing_wildcard() {
    assert!(!subject_filter_covers_prefix("events.orders", "events"));
}

#[test]
fn subject_filter_covers_prefix_returns_true_when_prefix_longer_than_filter() {
    assert!(subject_filter_covers_prefix("events.orders.>", "events.orders.extra"));
    assert!(subject_filter_covers_prefix("events.orders.>", "events.orders.extra.more"));
}

#[test]
fn next_direct_sequence_returns_incremented_value() {
    assert_eq!(next_direct_sequence(0), Some(1));
    assert_eq!(next_direct_sequence(5), Some(6));
    assert_eq!(next_direct_sequence(u64::MAX - 1), Some(u64::MAX));
}

#[test]
fn next_direct_sequence_returns_none_for_max() {
    assert_eq!(next_direct_sequence(u64::MAX), None);
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
fn unix_millis_handles_duration_zero() {
    assert_eq!(unix_millis(UNIX_EPOCH + Duration::ZERO), 0);
}

#[test]
fn unix_millis_handles_time_before_epoch() {
    let time = UNIX_EPOCH - Duration::from_secs(1);
    assert_eq!(unix_millis(time), 0);
}

#[test]
fn from_unix_millis_converts_correctly() {
    let millis = 1700000000000u64;
    let time = from_unix_millis(millis);
    let expected = UNIX_EPOCH + Duration::from_millis(millis);
    assert_eq!(time, expected);
}

#[test]
fn from_unix_millis_handles_zero() {
    assert_eq!(from_unix_millis(0), UNIX_EPOCH);
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
}

#[test]
fn map_append_error_creates_conflict_for_wrong_sequence() {
    let error = map_append_error("expected last subject sequence mismatch");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(error.to_string().contains("version conflict"));
}

#[test]
fn map_append_error_creates_conflict_for_wrong_last_sequence() {
    let error = map_append_error("wrong last sequence error");
    assert_eq!(error.code(), ErrorCode::Conflict);
}

#[test]
fn map_append_error_creates_transient_for_other_errors() {
    let error = map_append_error("connection timeout");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection timeout"));
}

#[test]
fn direct_get_next_request_serializes_correctly() {
    let request = DirectGetNextRequest {
        subject: "events.orders",
        sequence: Some(42),
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"next_by_subj\""));
    assert!(json.contains("\"events.orders\""));
    assert!(json.contains("\"seq\":42"));
}

#[test]
fn direct_get_next_request_serializes_without_optional_sequence() {
    let request = DirectGetNextRequest {
        subject: "events.orders",
        sequence: None,
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(!json.contains("seq"));
}

#[test]
fn append_message_id_is_stable_when_a_retry_observes_a_newer_version() {
    assert_eq!(
        append_message_id("catga.events.orders", 0, 0, b"payload"),
        append_message_id("catga.events.orders", 1, 1, b"payload"),
    );
}

#[test]
fn append_message_id_differs_for_different_subjects() {
    let id1 = append_message_id("events.orders", 0, 0, b"payload");
    let id2 = append_message_id("events.products", 0, 0, b"payload");
    assert_ne!(id1, id2);
}

#[test]
fn append_message_id_differs_for_different_payloads() {
    let id1 = append_message_id("events", 0, 0, b"payload1");
    let id2 = append_message_id("events", 0, 0, b"payload2");
    assert_ne!(id1, id2);
}

#[test]
fn append_message_id_starts_with_catga_prefix() {
    let id = append_message_id("events", 0, 0, b"payload");
    assert!(id.starts_with("catga-event:"));
}

#[test]
fn stream_id_reconciliation_only_runs_for_the_first_page() {
    assert!(stream_id_reconciliation_needed(None));
    assert!(!stream_id_reconciliation_needed(Some("orders-1000")));
}

#[test]
fn stream_id_reconciliation_runs_for_empty_after() {
    assert!(stream_id_reconciliation_needed(None));
}

#[test]
fn validate_subject_prefix_rejects_single_dot() {
    let result = validate_subject_prefix("events..orders");
    assert!(result.is_err());
}

#[test]
fn validate_subject_prefix_rejects_leading_dot() {
    let result = validate_subject_prefix(".events");
    assert!(result.is_err());
}

#[test]
fn validate_subject_prefix_rejects_trailing_dot() {
    let result = validate_subject_prefix("events.");
    assert!(result.is_err());
}

#[test]
fn stream_subjects_cover_prefix_returns_true_for_wildcard_in_middle() {
    let stream_subjects = vec!["orders.events.>".to_string()];
    assert!(stream_subjects_cover_prefix(&stream_subjects, "orders.events"));
    assert!(stream_subjects_cover_prefix(&stream_subjects, "orders.events.orders"));
}

#[test]
fn stream_subjects_cover_prefix_returns_false_for_wrong_prefix() {
    let stream_subjects = vec!["events.>".to_string()];
    assert!(!stream_subjects_cover_prefix(&stream_subjects, "orders"));
}

#[test]
fn stream_subjects_cover_prefix_handles_multiple_subjects() {
    let stream_subjects = vec![
        "other.>".to_string(),
        "events.>".to_string(),
    ];
    assert!(stream_subjects_cover_prefix(&stream_subjects, "events"));
    assert!(!stream_subjects_cover_prefix(&stream_subjects, "missing"));
}

#[test]
fn subject_filter_covers_prefix_handles_deeply_nested() {
    assert!(subject_filter_covers_prefix("*.foo.bar.>", "x.foo.bar"));
    assert!(subject_filter_covers_prefix("*.foo.bar.>", "x.foo.bar.baz"));
}

#[test]
fn append_message_id_contains_subject_and_digest() {
    let id = append_message_id("events.orders", 1, 5, b"test payload");
    assert!(id.contains("events.orders"));
    assert!(id.contains("catga-event:"));
    let parts: Vec<&str> = id.split(':').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[2].len(), 64);
}

#[test]
fn append_message_id_is_stable_across_versions() {
    let id1 = append_message_id("events", 1, 10, b"same-payload");
    let id2 = append_message_id("events", 5, 20, b"same-payload");
    assert_eq!(id1, id2, "same payload should produce same message ID");
}

#[test]
fn unix_millis_handles_max_u64_time() {
    let max_time = UNIX_EPOCH + Duration::from_millis(u64::MAX);
    let millis = unix_millis(max_time);
    assert_eq!(millis, u64::MAX);
}

#[test]
fn unix_millis_handles_large_duration() {
    let time = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
    let millis = unix_millis(time);
    assert!(millis > 0);
}

#[test]
fn map_append_error_creates_conflict_for_exact_match() {
    let error = map_append_error("NATS: expected last subject sequence mismatch");
    assert_eq!(error.code(), ErrorCode::Conflict);
}

#[test]
fn map_append_error_creates_transient_for_timeout() {
    let error = map_append_error("request timeout: no responders");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn direct_get_next_request_serializes_empty_subject() {
    let request = DirectGetNextRequest {
        subject: "",
        sequence: None,
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("next_by_subj"));
    assert!(json.contains("\"\""));
}
