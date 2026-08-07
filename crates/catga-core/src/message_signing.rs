//! HMAC signing for serialized message payloads.

use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::{CatgaError, CatgaResult, ErrorCode};

type HmacSha256 = Hmac<Sha256>;

/// Signs and verifies serialized message payloads.
pub trait MessageSigner: Send + Sync {
    /// Returns a portable signature for the payload.
    fn sign(&self, payload: &[u8]) -> String;

    /// Verifies one signature without exposing comparison timing.
    fn verify(&self, payload: &[u8], signature: &str) -> bool;
}

/// A shared-secret HMAC-SHA256 message signer using Base64 signatures.
#[derive(Clone)]
pub struct HmacMessageSigner {
    mac: HmacSha256,
}

impl HmacMessageSigner {
    /// Creates a signer from non-empty shared-secret bytes.
    ///
    /// Keep the key in a secret manager and rotate it by accepting both the
    /// active and previous key at the application boundary while messages are
    /// in flight. The signer itself is cheap to clone for shared transport
    /// configuration.
    ///
    /// ```
    /// use catga_core::{HmacMessageSigner, MessageSigner};
    ///
    /// let signer = HmacMessageSigner::new(b"development-secret")?;
    /// let signature = signer.sign(b"order:42");
    ///
    /// assert!(signer.verify(b"order:42", &signature));
    /// assert!(!signer.verify(b"order:43", &signature));
    /// # Ok::<(), catga_core::CatgaError>(())
    /// ```
    pub fn new(key: impl AsRef<[u8]>) -> CatgaResult<Self> {
        let key = key.as_ref();
        if key.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message signing key must not be empty",
            ));
        }
        let mac = HmacSha256::new_from_slice(key).map_err(|error| {
            CatgaError::new(
                ErrorCode::Validation,
                format!("invalid message signing key: {error}"),
            )
        })?;
        Ok(Self { mac })
    }

    fn mac(&self, payload: &[u8]) -> HmacSha256 {
        let mut mac = self.mac.clone();
        mac.update(payload);
        mac
    }
}

impl MessageSigner for HmacMessageSigner {
    fn sign(&self, payload: &[u8]) -> String {
        STANDARD.encode(self.mac(payload).finalize().into_bytes())
    }

    fn verify(&self, payload: &[u8], signature: &str) -> bool {
        let Ok(signature) = STANDARD.decode(signature) else {
            return false;
        };
        self.mac(payload).verify_slice(&signature).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Base64 encoded string
        assert!(!signature.is_empty());
        // Base64 characters only (no padding issues)
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
}
