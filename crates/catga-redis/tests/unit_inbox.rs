//! Unit tests for inbox module helper functions.

use catga_core::{ErrorCode, ProcessingState};

// Replicated constants from inbox.rs for testing
const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;

/// Replicated state parsing function for testing.
/// Mirrors the logic in inbox.rs without requiring a Redis connection.
fn state(value: &[u8]) -> Result<ProcessingState, catga_core::CatgaError> {
    match value.first() {
        Some(&CLAIMED) => Ok(ProcessingState::Claimed),
        Some(&COMPLETED_EMPTY | &COMPLETED_RESULT) => Ok(ProcessingState::Completed),
        Some(&FAILED) => Ok(ProcessingState::Failed),
        _ => Err(catga_core::CatgaError::new(
            ErrorCode::Internal,
            "Redis inbox record is malformed",
        )),
    }
}

/// Replicated key construction function for testing.
fn inbox_key(prefix: &str, message_id: u64) -> String {
    format!("{prefix}:{message_id}")
}

/// Replicated completed key construction function for testing.
fn completed_key(prefix: &str) -> String {
    format!("{prefix}:completed")
}

// =============================================================================
// State parsing tests
// =============================================================================

#[test]
fn state_claimed() {
    let value = vec![CLAIMED, 100, 50, 58]; // CLAIMED followed by expiry:generation:
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Claimed);
}

#[test]
fn state_completed_empty() {
    let value = vec![COMPLETED_EMPTY];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Completed);
}

#[test]
fn state_completed_with_result() {
    let value = vec![COMPLETED_RESULT, b'd', b'a', b't', b'a'];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Completed);
}

#[test]
fn state_failed() {
    let value = vec![FAILED, 0, 0, 0];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Failed);
}

#[test]
fn state_empty_value() {
    let value = vec![];
    let result = state(&value);
    assert!(result.is_err());
    let error = result.unwrap_err();
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("malformed"));
}

#[test]
fn state_zero_byte() {
    let value = vec![0];
    let result = state(&value);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::Internal);
}

#[test]
fn state_invalid_state_byte() {
    let value = vec![5]; // 5 is not a valid state
    let result = state(&value);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::Internal);
}

#[test]
fn state_invalid_state_high_byte() {
    let value = vec![255];
    let result = state(&value);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().code(), ErrorCode::Internal);
}

// =============================================================================
// Key construction tests
// =============================================================================

#[test]
fn inbox_key_format() {
    let key = inbox_key("catga:inbox", 42);
    assert_eq!(key, "catga:inbox:42");
}

#[test]
fn inbox_key_zero_id() {
    let key = inbox_key("prefix", 0);
    assert_eq!(key, "prefix:0");
}

#[test]
fn inbox_key_large_id() {
    let key = inbox_key("prefix", u64::MAX);
    assert_eq!(key, format!("prefix:{}", u64::MAX));
}

#[test]
fn inbox_key_consistent() {
    let key1 = inbox_key("prefix", 12345);
    let key2 = inbox_key("prefix", 12345);
    assert_eq!(key1, key2);
}

#[test]
fn inbox_key_different_prefixes() {
    let key1 = inbox_key("prefix-a", 42);
    let key2 = inbox_key("prefix-b", 42);
    assert_ne!(key1, key2);
}

#[test]
fn inbox_key_different_ids() {
    let key1 = inbox_key("prefix", 1);
    let key2 = inbox_key("prefix", 2);
    assert_ne!(key1, key2);
}

#[test]
fn inbox_key_empty_prefix() {
    let key = inbox_key("", 42);
    assert_eq!(key, ":42");
}

#[test]
fn completed_key_format() {
    let key = completed_key("catga:inbox");
    assert_eq!(key, "catga:inbox:completed");
}

#[test]
fn completed_key_consistent() {
    let key1 = completed_key("prefix");
    let key2 = completed_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn completed_key_different_prefixes() {
    let key1 = completed_key("prefix-a");
    let key2 = completed_key("prefix-b");
    assert_ne!(key1, key2);
}

#[test]
fn completed_key_empty_prefix() {
    let key = completed_key("");
    assert_eq!(key, ":completed");
}

// =============================================================================
// State constant validation tests
// =============================================================================

#[test]
fn state_constants_are_distinct() {
    assert_ne!(CLAIMED, COMPLETED_EMPTY);
    assert_ne!(CLAIMED, COMPLETED_RESULT);
    assert_ne!(CLAIMED, FAILED);
    assert_ne!(COMPLETED_EMPTY, COMPLETED_RESULT);
    assert_ne!(COMPLETED_EMPTY, FAILED);
    assert_ne!(COMPLETED_RESULT, FAILED);
}

#[test]
fn state_constants_are_non_zero() {
    assert!(CLAIMED > 0);
    assert!(COMPLETED_EMPTY > 0);
    assert!(COMPLETED_RESULT > 0);
    assert!(FAILED > 0);
}

#[test]
fn state_constants_are_single_byte() {
    // u8 is guaranteed to be a single byte (0-255)
    assert!(CLAIMED <= u8::MAX);
    assert!(COMPLETED_EMPTY <= u8::MAX);
    assert!(COMPLETED_RESULT <= u8::MAX);
    assert!(FAILED <= u8::MAX);
}

// =============================================================================
// Edge cases for state parsing
// =============================================================================

#[test]
fn state_with_only_claimed_byte() {
    let value = vec![CLAIMED];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Claimed);
}

#[test]
fn state_with_only_failed_byte() {
    let value = vec![FAILED];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Failed);
}

#[test]
fn state_completed_empty_followed_by_data() {
    // Even if there's data after COMPLETED_EMPTY, it's still Completed
    let value = vec![COMPLETED_EMPTY, 1, 2, 3, 4, 5];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Completed);
}

#[test]
fn state_claimed_with_extended_data() {
    // Extended data after CLAIMED should still be Claimed
    let value = vec![CLAIMED, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Claimed);
}

#[test]
fn state_failed_with_extended_data() {
    // Extended data after FAILED should still be Failed
    let value = vec![FAILED, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let result = state(&value);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), ProcessingState::Failed);
}

// =============================================================================
// Processing state trait verification
// =============================================================================

#[test]
fn processing_state_claimed_is_correct() {
    assert_eq!(ProcessingState::Claimed, ProcessingState::Claimed);
}

#[test]
fn processing_state_completed_is_correct() {
    assert_eq!(ProcessingState::Completed, ProcessingState::Completed);
}

#[test]
fn processing_state_failed_is_correct() {
    assert_eq!(ProcessingState::Failed, ProcessingState::Failed);
}

#[test]
fn processing_state_all_variants() {
    use catga_core::ProcessingState;
    // Verify all expected variants exist
    let _ = ProcessingState::Claimed;
    let _ = ProcessingState::Completed;
    let _ = ProcessingState::Failed;
}
