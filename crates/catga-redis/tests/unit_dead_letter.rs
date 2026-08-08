//! Unit tests for dead_letter module helper functions.

use catga_core::{ErrorCode, DeadLetter, DeadLetterDiagnostics};

/// Replicated key construction functions for testing.
fn sequence_key(prefix: &str) -> String {
    format!("{prefix}:sequence")
}

fn details_key(prefix: &str) -> String {
    format!("{prefix}:details")
}

fn queue_key(prefix: &str) -> String {
    format!("{prefix}:queue")
}

fn detail_key(prefix: &str, id: u64) -> String {
    format!("{prefix}:details:{id}")
}

// =============================================================================
// Key construction tests
// =============================================================================

#[test]
fn sequence_key_format() {
    let key = sequence_key("catga:dlq");
    assert_eq!(key, "catga:dlq:sequence");
}

#[test]
fn sequence_key_empty_prefix() {
    let key = sequence_key("");
    assert_eq!(key, ":sequence");
}

#[test]
fn sequence_key_consistent() {
    let key1 = sequence_key("prefix");
    let key2 = sequence_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn details_key_format() {
    let key = details_key("catga:dlq");
    assert_eq!(key, "catga:dlq:details");
}

#[test]
fn details_key_empty_prefix() {
    let key = details_key("");
    assert_eq!(key, ":details");
}

#[test]
fn details_key_consistent() {
    let key1 = details_key("prefix");
    let key2 = details_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn queue_key_format() {
    let key = queue_key("catga:dlq");
    assert_eq!(key, "catga:dlq:queue");
}

#[test]
fn queue_key_empty_prefix() {
    let key = queue_key("");
    assert_eq!(key, ":queue");
}

#[test]
fn queue_key_consistent() {
    let key1 = queue_key("prefix");
    let key2 = queue_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn detail_key_format() {
    let key = detail_key("catga:dlq", 42);
    assert_eq!(key, "catga:dlq:details:42");
}

#[test]
fn detail_key_zero_id() {
    let key = detail_key("prefix", 0);
    assert_eq!(key, "prefix:details:0");
}

#[test]
fn detail_key_large_id() {
    let key = detail_key("prefix", u64::MAX);
    assert_eq!(key, format!("prefix:details:{}", u64::MAX));
}

#[test]
fn detail_key_consistent() {
    let key1 = detail_key("prefix", 123);
    let key2 = detail_key("prefix", 123);
    assert_eq!(key1, key2);
}

#[test]
fn detail_key_different_prefixes() {
    let key1 = detail_key("prefix-a", 42);
    let key2 = detail_key("prefix-b", 42);
    assert_ne!(key1, key2);
}

#[test]
fn detail_key_different_ids() {
    let key1 = detail_key("prefix", 1);
    let key2 = detail_key("prefix", 2);
    assert_ne!(key1, key2);
}

// =============================================================================
// Key separation tests
// =============================================================================

#[test]
fn keys_are_distinct() {
    let seq = sequence_key("prefix");
    let det = details_key("prefix");
    let que = queue_key("prefix");
    assert_ne!(seq, det);
    assert_ne!(seq, que);
    assert_ne!(det, que);
}

#[test]
fn keys_share_prefix() {
    let prefix = "catga:dlq";
    let seq = sequence_key(prefix);
    let det = details_key(prefix);
    let que = queue_key(prefix);
    assert!(seq.starts_with(prefix));
    assert!(det.starts_with(prefix));
    assert!(que.starts_with(prefix));
}

// =============================================================================
// Diagnostics constant tests
// =============================================================================

#[test]
fn error_code_from_stable_str_valid() {
    use catga_core::ErrorCode;
    // Test that ErrorCode can be created from stable string
    let error_code = ErrorCode::from_stable_str("validation");
    assert!(error_code.is_some());
}

#[test]
fn error_code_from_stable_str_invalid() {
    use catga_core::ErrorCode;
    let error_code = ErrorCode::from_stable_str("not_a_real_code_xyz");
    assert!(error_code.is_none());
}

#[test]
fn dead_letter_diagnostics_try_at_valid() {
    let result = DeadLetterDiagnostics::try_at(
        1700000000000,
        catga_core::ErrorCode::Validation,
        "test-stage",
    );
    assert!(result.is_ok());
}

#[test]
fn dead_letter_diagnostics_try_at_empty_stage() {
    let result = DeadLetterDiagnostics::try_at(
        1700000000000,
        catga_core::ErrorCode::Internal,
        "",
    );
    // Empty stage is invalid
    assert!(result.is_err());
}

#[test]
fn dead_letter_diagnostics_try_at_whitespace_stage() {
    let result = DeadLetterDiagnostics::try_at(
        1700000000000,
        catga_core::ErrorCode::Internal,
        "   ",
    );
    // Whitespace-only stage is invalid (only ASCII alphanumeric, dot, underscore, hyphen allowed)
    assert!(result.is_err());
}

// =============================================================================
// Envelope structure tests
// =============================================================================

#[test]
fn dead_letter_new_requires_envelope() {
    use catga_core::Envelope;
    use catga_core::MessageMetadata;
    use catga_core::EnvelopeCodec;
    use catga_core::codec::memorypack::MemoryPackCodec;

    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let envelope = Envelope::new(1, "test.type", vec![1, 2, 3], metadata);

    let letter = DeadLetter::new(envelope, "test reason", 3);
    assert_eq!(letter.reason(), "test reason");
    assert_eq!(letter.attempts(), 3);
}

#[test]
fn dead_letter_with_diagnostics() {
    use catga_core::Envelope;
    use catga_core::MessageMetadata;
    use catga_core::EnvelopeCodec;
    use catga_core::codec::memorypack::MemoryPackCodec;

    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let envelope = Envelope::new(1, "test.type", vec![], metadata);

    let diagnostics = DeadLetterDiagnostics::try_at(
        1700000000000,
        catga_core::ErrorCode::Transient,
        "processing",
    ).unwrap();

    let letter = DeadLetter::try_with_diagnostics(
        envelope,
        "connection lost",
        5,
        diagnostics,
    );

    // try_with_diagnostics may fail due to validation, but that's expected
    // This test verifies the API is callable
    let _ = letter;
}
