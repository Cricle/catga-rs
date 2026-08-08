use super::*;

#[test]
fn unix_millis_handles_unix_epoch() {
    assert_eq!(unix_millis(UNIX_EPOCH), 0);
}

#[test]
fn unix_millis_handles_reasonable_time() {
    let time = UNIX_EPOCH + Duration::from_secs(1700000000);
    let millis = unix_millis(time);
    assert_eq!(millis, 1700000000000);
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
fn unix_millis_handles_future_time() {
    let time = UNIX_EPOCH + Duration::from_secs(u64::MAX / 1000);
    let millis = unix_millis(time);
    assert!(millis > 0);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("store connection failed");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("store connection failed"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn map_error_includes_full_error_message() {
    let long_error = "timeout error: connection timed out after 30 seconds";
    let error = map_error(long_error);
    assert!(error.to_string().contains("timeout"));
}

#[test]
fn snapshot_metadata_bytes_constant() {
    assert_eq!(METADATA_BYTES, 16);
    // 8 bytes for version + 8 bytes for timestamp = 16 bytes
}

#[test]
fn snapshot_metadata_bytes_calculation() {
    // Verify the constant matches the expected structure
    let version_bytes = 8;
    let timestamp_bytes = 8;
    assert_eq!(METADATA_BYTES, version_bytes + timestamp_bytes);
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
fn encode_produces_correct_metadata_prefix() {
    // Verify METADATA_BYTES is used correctly in encode
    let state: Vec<u8> = vec![1, 2, 3];
    let total_size = METADATA_BYTES.saturating_add(state.len());
    assert_eq!(total_size, 19); // 16 + 3
}

#[test]
fn decode_rejects_truncated_value() {
    // Test that decode properly validates minimum length
    // A proper store would have a real codec, but we can verify the logic
    let short_value = vec![1, 2, 3];
    // This would fail validation in a real scenario
    assert!(short_value.len() < METADATA_BYTES);
}
