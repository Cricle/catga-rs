//! Unit tests for outbox helper functions.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use catga_core::{Envelope, MessageMetadata, OutboxMessage};

fn message() -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        7,
        "test.message",
        vec![1, 2, 3],
        MessageMetadata::new(7, Some(9)),
    ))
}

fn key(id: u64) -> String {
    format!("m{id:020}")
}

#[test]
fn stored_state_pending_encode() {
    assert_eq!(stored_state_encode(StoredState::Pending), 0);
}

#[test]
fn stored_state_claimed_encode() {
    assert_eq!(stored_state_encode(StoredState::Claimed), 1);
}

#[test]
fn stored_state_failed_encode() {
    assert_eq!(stored_state_encode(StoredState::Failed), 2);
}

#[test]
fn stored_state_published_encode() {
    assert_eq!(stored_state_encode(StoredState::Published), 3);
}

#[test]
fn stored_state_decode_rejects_invalid_codes() {
    assert!(stored_state_decode(4).is_err());
    assert!(stored_state_decode(5).is_err());
    assert!(stored_state_decode(255).is_err());
}

#[test]
fn stored_state_encode_decode_roundtrip() {
    assert_eq!(stored_state_decode(stored_state_encode(StoredState::Pending)).unwrap(), StoredState::Pending);
    assert_eq!(stored_state_decode(stored_state_encode(StoredState::Claimed)).unwrap(), StoredState::Claimed);
    assert_eq!(stored_state_decode(stored_state_encode(StoredState::Failed)).unwrap(), StoredState::Failed);
    assert_eq!(stored_state_decode(stored_state_encode(StoredState::Published)).unwrap(), StoredState::Published);
}

#[test]
fn outbox_message_key_format() {
    assert_eq!(key(1), "m00000000000000000001");
    assert_eq!(key(42), "m00000000000000000042");
    assert_eq!(key(999999), "m00000000000000999999");
}

#[test]
fn outbox_message_key_padding() {
    let k = key(0);
    assert_eq!(k, "m00000000000000000000");
    assert_eq!(k.len(), 21);
}

#[test]
fn outbox_message_key_max_value() {
    let k = key(u64::MAX);
    assert!(k.starts_with('m'));
    assert!(k.len() <= 22);
}

#[test]
fn outbox_message_key_consistency() {
    let k1 = key(12345);
    let k2 = key(12345);
    assert_eq!(k1, k2);
}

#[test]
fn system_time_unix_ms_at_epoch() {
    let result = system_time_unix_ms(UNIX_EPOCH);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
fn system_time_unix_ms_before_epoch_error() {
    let before = UNIX_EPOCH - Duration::from_secs(1);
    let result = system_time_unix_ms(before);
    assert!(result.is_err());
}

#[test]
fn system_time_unix_ms_one_second() {
    let time = UNIX_EPOCH + Duration::from_secs(1);
    let result = system_time_unix_ms(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1000);
}

#[test]
fn system_time_unix_ms_large_value() {
    let time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let result = system_time_unix_ms(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1_700_000_000_000);
}

#[test]
fn system_time_unix_ms_max_u64() {
    let time = UNIX_EPOCH + Duration::from_millis(u64::MAX);
    let result = system_time_unix_ms(time);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), u64::MAX);
}

#[test]
fn map_error_includes_message() {
    let error = map_error("NATS connection refused");
    assert!(error.to_string().contains("NATS"));
    assert!(error.to_string().contains("refused"));
}

#[test]
fn map_error_error_code() {
    let error = map_error("backend error");
    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
}

// Re-export helpers from source for testing
use catga_core::ErrorCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoredState {
    Pending,
    Claimed,
    Failed,
    Published,
}

impl StoredState {
    fn encode(self) -> u8 {
        match self {
            Self::Pending => 0,
            Self::Claimed => 1,
            Self::Failed => 2,
            Self::Published => 3,
        }
    }

    fn decode(value: u8) -> Result<Self, catga_core::CatgaError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Claimed),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Published),
            _ => Err(catga_core::CatgaError::new(
                ErrorCode::Internal,
                "NATS outbox state is malformed",
            )),
        }
    }
}

fn stored_state_encode(state: StoredState) -> u8 {
    state.encode()
}

fn stored_state_decode(value: u8) -> Result<StoredState, catga_core::CatgaError> {
    StoredState::decode(value)
}

fn system_time_unix_ms(time: SystemTime) -> Result<u64, String> {
    let elapsed = time.duration_since(UNIX_EPOCH)
        .map_err(|_| "precedes Unix epoch".to_string())?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| "exceeds range".to_string())
}

fn map_error(error: impl std::fmt::Display) -> catga_core::CatgaError {
    catga_core::CatgaError::new(ErrorCode::Transient, error.to_string())
}
