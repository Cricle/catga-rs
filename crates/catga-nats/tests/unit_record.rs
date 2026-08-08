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
