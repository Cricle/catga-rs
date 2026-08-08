//! Unit tests for store types, envelopes, and outbox messages.

use std::{
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use catga_core::{
    Envelope, EnvelopeHeaders, ErrorCode, MessageMetadata, OutboxMessage,
    OutboxState, validate_outbox_claim_lease, validate_outbox_claim_limit,
    validate_outbox_message_id, MAX_OUTBOX_CLAIM_LEASE, MAX_OUTBOX_CLAIM_LIMIT,
    MAX_OUTBOX_FAILURE_ERROR_BYTES, DEFAULT_OUTBOX_MAX_RETRIES,
};

fn make_envelope() -> Envelope {
    Envelope::new(
        1,
        "test.message",
        vec![1, 2, 3],
        MessageMetadata::new(1, None),
    )
}

#[test]
fn envelope_header_basic() {
    let headers = EnvelopeHeaders::try_new([("key", "value")]).expect("valid headers");
    let header = headers.iter().next().expect("header exists");
    assert_eq!(header.0, "key");
    assert_eq!(header.1, "value");
}

#[test]
fn envelope_headers_empty() {
    let headers = EnvelopeHeaders::try_new::<[(&str, &str); 0], _, _>([]).expect("valid");
    assert!(headers.is_empty());
    assert_eq!(headers.len(), 0);
    assert!(headers.get("key").is_none());
}

#[test]
fn envelope_headers_with_values() {
    let headers = EnvelopeHeaders::try_new([("tenant", "acme"), ("region", "us-east")])
        .expect("valid headers");
    assert!(!headers.is_empty());
    assert_eq!(headers.len(), 2);
    assert_eq!(headers.get("tenant"), Some("acme"));
    assert_eq!(headers.get("region"), Some("us-east"));
    assert!(headers.get("missing").is_none());
}

#[test]
fn envelope_headers_rejects_duplicate_keys() {
    let result = EnvelopeHeaders::try_new([("key", "value1"), ("key", "value2")]);
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[test]
fn envelope_headers_rejects_empty_key() {
    let result = EnvelopeHeaders::try_new([("", "value")]);
    assert!(result.is_err());
}

#[test]
fn envelope_headers_rejects_whitespace_key() {
    let result = EnvelopeHeaders::try_new([("   ", "value")]);
    assert!(result.is_err());
}

#[test]
fn envelope_headers_enforces_max_count() {
    let headers: Vec<(String, &str)> = (0..100).map(|i| (format!("key{}", i), "v")).collect();
    let result = EnvelopeHeaders::try_new(headers);
    assert!(result.is_err());
}

#[test]
fn envelope_headers_enforces_max_bytes() {
    let long_key = "k".repeat(MAX_OUTBOX_FAILURE_ERROR_BYTES * 8);
    let result = EnvelopeHeaders::try_new([(long_key.as_str(), "v")]);
    assert!(result.is_err());
}

#[test]
fn envelope_headers_iter() {
    let headers = EnvelopeHeaders::try_new([("a", "1"), ("b", "2")]).expect("valid");
    let items: Vec<_> = headers.iter().collect();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], ("a", "1"));
    assert_eq!(items[1], ("b", "2"));
}

#[test]
fn envelope_headers_merge_overrides_empty_base() {
    let base = EnvelopeHeaders::try_new::<[(&str, &str); 0], _, _>([]).expect("valid");
    let overrides = EnvelopeHeaders::try_new([("key", "value")]).expect("valid");
    let merged = base.merge_overrides(&overrides).expect("valid merge");
    assert_eq!(merged.get("key"), Some("value"));
}

#[test]
fn envelope_headers_merge_overrides_empty_overrides() {
    let base = EnvelopeHeaders::try_new([("key", "value")]).expect("valid");
    let overrides = EnvelopeHeaders::try_new::<[(&str, &str); 0], _, _>([]).expect("valid");
    let merged = base.merge_overrides(&overrides).expect("valid merge");
    assert_eq!(merged.get("key"), Some("value"));
}

#[test]
fn envelope_headers_merge_overrides_replaces_existing() {
    let base = EnvelopeHeaders::try_new([("key", "old")]).expect("valid");
    let overrides = EnvelopeHeaders::try_new([("key", "new")]).expect("valid");
    let merged = base.merge_overrides(&overrides).expect("valid merge");
    assert_eq!(merged.get("key"), Some("new"));
}

#[test]
fn envelope_headers_merge_overrides_appends_new() {
    let base = EnvelopeHeaders::try_new([("existing", "value")]).expect("valid");
    let overrides = EnvelopeHeaders::try_new([("new", "value2")]).expect("valid");
    let merged = base.merge_overrides(&overrides).expect("valid merge");
    assert_eq!(merged.get("existing"), Some("value"));
    assert_eq!(merged.get("new"), Some("value2"));
}

#[test]
fn envelope_new() {
    let envelope = make_envelope();
    assert_eq!(envelope.id(), 1);
    assert_eq!(envelope.message_type(), "test.message");
    assert_eq!(envelope.payload(), &[1, 2, 3]);
    assert_eq!(envelope.schema_version(), 1);
    assert!(envelope.reply_to().is_none());
    assert!(envelope.headers().next().is_none());
}

#[test]
fn envelope_with_reply_to() {
    let envelope = make_envelope().with_reply_to("reply.to.queue");
    assert_eq!(envelope.reply_to(), Some("reply.to.queue"));
}

#[test]
fn envelope_with_headers() {
    let headers = EnvelopeHeaders::try_new([("key", "value")]).expect("valid");
    let envelope = make_envelope().with_headers(headers);
    assert_eq!(envelope.header("key"), Some("value"));
}

#[test]
fn envelope_with_sent_at() {
    let envelope = make_envelope()
        .with_sent_at(SystemTime::now())
        .expect("valid time");
    assert!(envelope.sent_at().is_some());
}

#[test]
fn envelope_with_sent_at_rejects_before_epoch() {
    let before_epoch = UNIX_EPOCH - Duration::from_secs(1);
    let result = make_envelope().with_sent_at(before_epoch);
    assert!(result.is_err());
}

#[test]
fn envelope_with_sent_at_unix_ms() {
    let envelope = make_envelope().with_sent_at_unix_ms(Some(1000));
    assert_eq!(envelope.sent_at_unix_ms(), Some(1000));
}

#[test]
fn envelope_headers_iterator() {
    let headers = EnvelopeHeaders::try_new([("a", "1"), ("b", "2")]).expect("valid");
    let envelope = make_envelope().with_headers(headers);
    let items: Vec<_> = envelope.headers().collect();
    assert_eq!(items.len(), 2);
}

#[test]
fn validate_outbox_message_id_accepts_positive() {
    assert!(validate_outbox_message_id(1).is_ok());
    assert!(validate_outbox_message_id(u64::MAX).is_ok());
}

#[test]
fn validate_outbox_message_id_rejects_zero() {
    let result = validate_outbox_message_id(0);
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[test]
fn validate_outbox_claim_limit_accepts_valid() {
    assert!(validate_outbox_claim_limit(0).is_ok());
    assert!(validate_outbox_claim_limit(MAX_OUTBOX_CLAIM_LIMIT).is_ok());
}

#[test]
fn validate_outbox_claim_limit_rejects_excessive() {
    let result = validate_outbox_claim_limit(MAX_OUTBOX_CLAIM_LIMIT + 1);
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[test]
fn validate_outbox_claim_lease_accepts_valid() {
    assert!(validate_outbox_claim_lease(Duration::from_millis(1)).is_ok());
    assert!(validate_outbox_claim_lease(MAX_OUTBOX_CLAIM_LEASE).is_ok());
}

#[test]
fn validate_outbox_claim_lease_rejects_zero() {
    let result = validate_outbox_claim_lease(Duration::ZERO);
    assert!(result.is_err());
}

#[test]
fn validate_outbox_claim_lease_rejects_excessive() {
    let result = validate_outbox_claim_lease(MAX_OUTBOX_CLAIM_LEASE + Duration::from_secs(1));
    assert!(result.is_err());
}

#[test]
fn outbox_message_new() {
    let message = OutboxMessage::new(make_envelope());
    assert_eq!(message.state(), OutboxState::Pending);
    assert!(message.owner().is_none());
    assert_eq!(message.retry_count(), 0);
    assert_eq!(message.max_retries(), DEFAULT_OUTBOX_MAX_RETRIES);
    assert!(message.last_error().is_none());
}

#[test]
fn outbox_message_with_max_retries() {
    let message = OutboxMessage::new(make_envelope())
        .with_max_retries(5)
        .expect("valid");
    assert_eq!(message.max_retries(), 5);
}

#[test]
fn outbox_message_with_max_retries_rejects_zero() {
    let result = OutboxMessage::new(make_envelope()).with_max_retries(0);
    assert!(result.is_err());
}

#[test]
fn outbox_message_with_retry_history() {
    let message = OutboxMessage::new(make_envelope()).with_retry_history(3, Some("last error"));
    assert_eq!(message.retry_count(), 3);
    assert_eq!(message.last_error(), Some("last error"));
}

#[test]
fn outbox_message_claim() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim("worker-1");
    assert_eq!(message.state(), OutboxState::Claimed);
    assert_eq!(message.owner(), Some("worker-1"));
}

#[test]
fn outbox_message_claim_ignores_terminal_states() {
    let mut failed = OutboxMessage::new(make_envelope())
        .with_max_retries(1)
        .expect("valid");
    failed.record_failure("error");
    assert_eq!(failed.state(), OutboxState::Failed);
    failed.claim("worker");
    assert_eq!(failed.state(), OutboxState::Failed);

    let mut published = OutboxMessage::new(make_envelope());
    published.mark_published(1000);
    assert_eq!(published.state(), OutboxState::Pending);
    published.claim("worker");
    assert_eq!(published.state(), OutboxState::Claimed);
}

#[test]
fn outbox_message_mark_published_from_claimed() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim("worker");
    assert_eq!(message.state(), OutboxState::Claimed);

    message.mark_published(1000);
    assert_eq!(message.state(), OutboxState::Published);
}

#[test]
fn outbox_message_claim_after_first_failure() {
    let mut message = OutboxMessage::new(make_envelope());
    assert_eq!(message.state(), OutboxState::Pending);

    message.record_failure("error");
    assert_eq!(message.state(), OutboxState::Pending);
    assert_eq!(message.retry_count(), 1);

    message.claim("worker");
    assert_eq!(message.state(), OutboxState::Claimed);
}

#[test]
fn outbox_message_claim_until() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim_until("worker", 1000);
    assert_eq!(message.state(), OutboxState::Claimed);
    assert_eq!(message.owner(), Some("worker"));
    assert_eq!(message.claimed_until_unix_ms(), Some(1000));
}

#[test]
fn outbox_message_claim_until_with_token() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim_until_with_token("worker", "token-123", 1000);
    assert_eq!(message.state(), OutboxState::Claimed);
    assert_eq!(message.owner(), Some("worker"));
    assert_eq!(message.claim_token(), Some("token-123"));
    assert_eq!(message.claimed_until_unix_ms(), Some(1000));
}

#[test]
fn outbox_message_mark_published() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim("worker");
    message.mark_published(1000);
    assert_eq!(message.state(), OutboxState::Published);
    assert!(message.owner().is_none());
    assert_eq!(message.published_at_unix_ms(), Some(1000));
}

#[test]
fn outbox_message_mark_published_ignores_non_claimed() {
    let mut message = OutboxMessage::new(make_envelope());
    message.mark_published(1000);
    assert_eq!(message.state(), OutboxState::Pending);
}

#[test]
fn outbox_message_release() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim_until("worker", 1000);
    message.release();
    assert_eq!(message.state(), OutboxState::Pending);
    assert!(message.owner().is_none());
}

#[test]
fn outbox_message_record_failure_below_max() {
    let mut message = OutboxMessage::new(make_envelope())
        .with_max_retries(3)
        .expect("valid");
    message.record_failure("error 1");
    assert_eq!(message.state(), OutboxState::Pending);
    assert_eq!(message.retry_count(), 1);
    assert_eq!(message.last_error(), Some("error 1"));
}

#[test]
fn outbox_message_record_failure_at_max() {
    let mut message = OutboxMessage::new(make_envelope())
        .with_max_retries(2)
        .expect("valid");
    message.record_failure("error 1");
    message.record_failure("error 2");
    assert_eq!(message.state(), OutboxState::Failed);
    assert_eq!(message.retry_count(), 2);
}

#[test]
fn outbox_message_is_claimable_at_pending() {
    let message = OutboxMessage::new(make_envelope());
    assert!(message.is_claimable_at(0));
    assert!(message.is_claimable_at(u64::MAX));
}

#[test]
fn outbox_message_is_claimable_at_claimed_expired() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim_until("worker", 1000);
    assert!(message.is_claimable_at(1001));
}

#[test]
fn outbox_message_is_claimable_at_claimed_not_expired() {
    let mut message = OutboxMessage::new(make_envelope());
    message.claim_until("worker", 1000);
    assert!(!message.is_claimable_at(999));
}

#[test]
fn outbox_message_bounded_failure_reason_within_limit() {
    let short = "short error";
    let result = OutboxMessage::bounded_failure_reason(short);
    assert_eq!(result, short.into());
}

#[test]
fn outbox_message_bounded_failure_reason_truncates() {
    let long = "x".repeat(MAX_OUTBOX_FAILURE_ERROR_BYTES * 2);
    let result = OutboxMessage::bounded_failure_reason(&long);
    assert_eq!(result.len(), MAX_OUTBOX_FAILURE_ERROR_BYTES);
}

#[test]
fn outbox_message_envelope_returns_inner() {
    let inner = make_envelope();
    let message = OutboxMessage::new(inner.clone());
    assert_eq!(message.envelope().id(), inner.id());
}
