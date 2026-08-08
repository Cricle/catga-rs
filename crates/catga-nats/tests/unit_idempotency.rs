use super::*;

#[test]
fn state_claimed() {
    let value = vec![CLAIMED];
    assert_eq!(
        state(&value).expect("state should be Claimed"),
        ProcessingState::Claimed
    );
}

#[test]
fn state_completed_empty() {
    let value = vec![COMPLETED_EMPTY];
    assert_eq!(
        state(&value).expect("state should be Completed"),
        ProcessingState::Completed
    );
}

#[test]
fn state_completed_result() {
    let mut value = vec![COMPLETED_RESULT];
    value.extend_from_slice(b"result data");
    assert_eq!(
        state(&value).expect("state should be Completed"),
        ProcessingState::Completed
    );
}

#[test]
fn state_failed() {
    let value = vec![FAILED];
    assert_eq!(
        state(&value).expect("state should be Failed"),
        ProcessingState::Failed
    );
}

#[test]
fn state_rejects_unknown_first_byte() {
    let value = vec![99];
    assert!(state(&value).is_err());
    let err = state(&value).expect_err("state should return an error for unknown byte");
    assert_eq!(err.code(), ErrorCode::Internal);
    assert!(err.to_string().contains("malformed"));
}

#[test]
fn state_rejects_empty() {
    let value = vec![];
    assert!(state(&value).is_err());
}

#[test]
fn state_handles_large_payload() {
    let mut value = vec![COMPLETED_RESULT];
    value.extend(vec![0u8; 1000]);
    assert_eq!(
        state(&value).expect("state should be Completed"),
        ProcessingState::Completed
    );
}

#[test]
fn claimed_with_expiry_format() {
    let expires_at = 1_700_000_000_000u64;
    let value = claimed_with_expiry(expires_at);
    assert_eq!(value[0], CLAIMED);
    let decoded = u64::from_be_bytes(value[1..9].try_into().expect("9 bytes for u64"));
    assert_eq!(decoded, expires_at);
}

#[test]
fn claimed_with_expiry_zero() {
    let value = claimed_with_expiry(0);
    assert_eq!(value[0], CLAIMED);
    assert_eq!(value.len(), 9);
}

#[test]
fn claimed_with_expiry_max_value() {
    let value = claimed_with_expiry(u64::MAX);
    assert_eq!(value[0], CLAIMED);
    let decoded = u64::from_be_bytes(value[1..9].try_into().expect("9 bytes for u64"));
    assert_eq!(decoded, u64::MAX);
}

#[test]
fn claim_expired_when_expired() {
    let expires_at = 1_000_000_000_000u64;
    let value = claimed_with_expiry(expires_at);
    let now = expires_at + 1;
    assert!(claim_expired(&value, now));
}

#[test]
fn claim_expired_when_not_expired() {
    let expires_at = 9_000_000_000_000u64;
    let value = claimed_with_expiry(expires_at);
    let now = 1_000_000_000_000u64;
    assert!(!claim_expired(&value, now));
}

#[test]
fn claim_expired_at_exact_boundary() {
    let expires_at = 1_000_000_000u64;
    let value = claimed_with_expiry(expires_at);
    assert!(claim_expired(&value, expires_at));
    assert!(!claim_expired(&value, expires_at - 1));
}

#[test]
fn claim_expired_rejects_short_payload() {
    let value = vec![CLAIMED];
    let now = u64::MAX;
    assert!(claim_expired(&value, now));
}

#[test]
fn claim_expired_rejects_empty() {
    let value = vec![];
    let now = u64::MAX;
    assert!(claim_expired(&value, now));
}

#[test]
fn claim_expired_rejects_partial_u64() {
    let value = vec![CLAIMED, 0x12, 0x34];
    let now = u64::MAX;
    assert!(claim_expired(&value, now));
}

#[test]
fn kv_key_prefix() {
    let result = kv_key("");
    assert!(
        result.starts_with('k'),
        "kv_key should start with 'k': {result}"
    );
}

#[test]
fn kv_key_empty() {
    let result = kv_key("");
    assert_eq!(result, "k");
}

#[test]
fn kv_key_simple() {
    let result = kv_key("abc");
    assert_eq!(result, "k616263");
}

#[test]
fn kv_key_digits() {
    let result = kv_key("123");
    assert_eq!(result, "k313233");
}

#[test]
fn kv_key_hex_encoding() {
    let result = kv_key("\x00\x0f");
    assert_eq!(result, "k000f");
}

#[test]
fn kv_key_special_chars() {
    let result = kv_key("test:key-1.val");
    assert_eq!(result, "k746573743a6b65792d312e76616c");
}

#[test]
fn kv_key_unicode() {
    let result = kv_key("你好");
    assert_eq!(result, "ke4bda0e5a5bd");
}

#[test]
fn kv_key_idempotent() {
    let key = "order-12345";
    let result1 = kv_key(key);
    let result2 = kv_key(key);
    assert_eq!(result1, result2, "kv_key should be deterministic");
}

#[test]
fn kv_key_unique_for_different_inputs() {
    let result_a = kv_key("a");
    let result_b = kv_key("b");
    assert_ne!(
        result_a, result_b,
        "different keys should produce different results"
    );
}

#[test]
fn kv_key_encoding_correctness() {
    let result = kv_key("AB");
    assert_eq!(result, "k4142");

    let result = kv_key("\x00\x0f");
    assert_eq!(result, "k000f");
}

#[test]
fn kv_key_consistent_length() {
    let test_cases: &[(&str, usize)] = &[
        ("", 1),
        ("a", 3),
        ("ab", 5),
        ("abc", 7),
        ("hello", 11),
    ];
    for (input, expected_len) in test_cases {
        let result = kv_key(input);
        assert_eq!(
            result.len(),
            *expected_len,
            "kv_key({:?}) length should be {}",
            input,
            expected_len
        );
    }

    for i in 0..10 {
        let input = "x".repeat(i);
        let result = kv_key(&input);
        assert_eq!(
            result.len(),
            1 + 2 * i,
            "length for {} chars should be {}",
            i,
            1 + 2 * i
        );
    }
}

#[test]
fn now_millis_returns_reasonable_timestamp() {
    let now = now_millis();
    assert!(now > 1_577_836_800_000u64);
    assert!(now < 4_104_451_840_000u64);
}

#[test]
fn now_millis_is_increasing() {
    let now1 = now_millis();
    let now2 = now_millis();
    assert!(now2 >= now1);
}

#[test]
fn map_error_creates_transient_error() {
    let err = map_error("test error message");
    assert_eq!(err.code(), ErrorCode::Transient);
    assert!(err.to_string().contains("test error message"));
}

#[test]
fn map_error_handles_empty_string() {
    let err = map_error("");
    assert_eq!(err.code(), ErrorCode::Transient);
}

#[test]
fn map_error_handles_unicode_error() {
    let err = map_error("错误消息");
    assert_eq!(err.code(), ErrorCode::Transient);
}

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
fn state_constants_are_single_bytes() {}

#[test]
fn retries_constant() {
    assert_eq!(RETRIES, 8);
}
