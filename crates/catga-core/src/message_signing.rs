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
