//! Internal record envelope used to confirm ambiguous NATS creates.

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use uuid::Uuid;

const PREFIX: &[u8; 4] = b"CNR1";
const HEADER_BYTES: usize = PREFIX.len() + 16;

pub(crate) struct CreatedRecord {
    token: [u8; 16],
    value: Vec<u8>,
}

impl CreatedRecord {
    pub(crate) fn value(&self) -> &[u8] {
        &self.value
    }

    pub(crate) fn matches(&self, record: &NatsRecord<'_>) -> bool {
        record.token == Some(self.token)
    }
}

pub(crate) struct NatsRecord<'a> {
    token: Option<[u8; 16]>,
    payload: &'a [u8],
}

impl<'a> NatsRecord<'a> {
    pub(crate) fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub(crate) fn with_payload(&self, payload: &[u8]) -> Vec<u8> {
        self.token
            .map_or_else(|| payload.to_vec(), |token| encode_record(token, payload))
    }
}

pub(crate) fn create_record(payload: &[u8]) -> CreatedRecord {
    let token = Uuid::new_v4().into_bytes();
    CreatedRecord {
        token,
        value: encode_record(token, payload),
    }
}

pub(crate) fn decode_record(value: &[u8]) -> CatgaResult<NatsRecord<'_>> {
    if !value.starts_with(PREFIX) {
        return Ok(NatsRecord {
            token: None,
            payload: value,
        });
    }
    if value.len() < HEADER_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Internal,
            "NATS record envelope is incomplete",
        ));
    }
    let token = value[PREFIX.len()..HEADER_BYTES].try_into().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "NATS record envelope token is malformed",
        )
    })?;
    Ok(NatsRecord {
        token: Some(token),
        payload: &value[HEADER_BYTES..],
    })
}

fn encode_record(token: [u8; 16], payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(HEADER_BYTES.saturating_add(payload.len()));
    value.extend_from_slice(PREFIX);
    value.extend_from_slice(&token);
    value.extend_from_slice(payload);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_payloads_and_preserve_ambiguity_tokens() {
        let created = create_record(b"payload");
        let decoded = decode_record(created.value()).expect("decode created record");
        assert_eq!(decoded.payload(), b"payload");
        assert!(created.matches(&decoded));
        assert_ne!(created.value(), b"payload");
        assert_eq!(
            &decoded.with_payload(b"updated")[HEADER_BYTES..],
            b"updated"
        );

        let raw = decode_record(b"plain payload").expect("decode legacy raw value");
        assert_eq!(raw.payload(), b"plain payload");
        assert_eq!(raw.with_payload(b"replacement"), b"replacement");
        assert!(!created.matches(&raw));
    }

    #[test]
    fn records_reject_truncated_envelopes_but_preserve_non_magic_bytes() {
        let mut truncated = PREFIX.to_vec();
        truncated.extend_from_slice(&[0; 8]);
        assert_eq!(
            match decode_record(&truncated) {
                Err(error) => error.code(),
                Ok(_) => panic!("truncated record unexpectedly decoded"),
            },
            ErrorCode::Internal
        );
        let mut token = [0; 16];
        token[0] = 7;
        let encoded = encode_record(token, b"payload");
        assert_eq!(
            decode_record(&encoded).expect("encoded record").payload(),
            b"payload"
        );
    }
}
