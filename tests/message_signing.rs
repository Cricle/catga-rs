//! HMAC message-signing contract tests.

use catga_core::{HmacMessageSigner, MessageSigner};

#[test]
fn hmac_signer_accepts_original_payload_and_rejects_tampering() {
    let signer = HmacMessageSigner::new(b"shared-secret").unwrap();
    let signature = signer.sign(b"order:42");
    assert!(signer.verify(b"order:42", &signature));
    assert!(!signer.verify(b"order:43", &signature));
    assert!(!signer.verify(b"order:42", "not-base64"));
}

#[test]
fn hmac_signer_rejects_an_empty_secret() {
    assert!(HmacMessageSigner::new([]).is_err());
}
