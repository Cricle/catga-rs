//! Unit tests for unit_message_signing.

use catga_core::{ErrorCode, HmacMessageSigner, MessageSigner};

#[test]
fn hmac_message_signer_creation_requires_non_empty_key() {
    let result = HmacMessageSigner::new(b"");
    match result {
        Err(e) => assert_eq!(e.code(), ErrorCode::Validation),
        Ok(_) => panic!("expected error"),
    }
}

#[test]
fn hmac_message_signer_creation_accepts_valid_key() {
    let signer = HmacMessageSigner::new(b"my-secret-key");
    assert!(signer.is_ok());
}

#[test]
fn hmac_message_signer_sign_produces_base64_output() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let signature = signer.sign(b"test payload");

    assert!(!signature.is_empty());
    assert!(
        signature
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/' || c == '=')
    );
}

#[test]
fn hmac_message_signer_sign_is_deterministic() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let sig1 = signer.sign(b"test");
    let sig2 = signer.sign(b"test");
    assert_eq!(sig1, sig2);
}

#[test]
fn hmac_message_signer_sign_different_payloads_different_signatures() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let sig1 = signer.sign(b"payload1");
    let sig2 = signer.sign(b"payload2");
    assert_ne!(sig1, sig2);
}

#[test]
fn hmac_message_signer_sign_different_keys_different_signatures() {
    let signer1 = HmacMessageSigner::new(b"key1").expect("valid key");
    let signer2 = HmacMessageSigner::new(b"key2").expect("valid key");
    let sig1 = signer1.sign(b"same payload");
    let sig2 = signer2.sign(b"same payload");
    assert_ne!(sig1, sig2);
}

#[test]
fn hmac_message_signer_verify_accepts_valid_signature() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let signature = signer.sign(b"test payload");
    assert!(signer.verify(b"test payload", &signature));
}

#[test]
fn hmac_message_signer_verify_rejects_invalid_signature() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    assert!(!signer.verify(b"test payload", "invalid_signature"));
}

#[test]
fn hmac_message_signer_verify_rejects_tampered_payload() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let signature = signer.sign(b"original payload");
    assert!(!signer.verify(b"tampered payload", &signature));
}

#[test]
fn hmac_message_signer_verify_handles_malformed_base64() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    assert!(!signer.verify(b"payload", "not-valid-base64!!!"));
    assert!(!signer.verify(b"payload", ""));
}

#[test]
fn hmac_message_signer_clone_preserves_behavior() {
    let signer1 = HmacMessageSigner::new(b"secret").expect("valid key");
    let signer2 = signer1.clone();
    let sig1 = signer1.sign(b"test");
    let sig2 = signer2.sign(b"test");
    assert_eq!(sig1, sig2);
}

#[test]
fn hmac_message_signer_empty_payload() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let signature = signer.sign(b"");
    assert!(signer.verify(b"", &signature));
    assert!(!signer.verify(b"not empty", &signature));
}

#[test]
fn hmac_message_signer_unicode_payload() {
    let signer = HmacMessageSigner::new(b"secret").expect("valid key");
    let payload = "Hello, 世界! 🌍";
    let signature = signer.sign(payload.as_bytes());
    assert!(signer.verify(payload.as_bytes(), &signature));
}
