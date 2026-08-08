use super::*;
use catga_core::{Envelope, MessageMetadata};

fn envelope() -> Envelope {
    Envelope::new(
        7,
        "orders.failed",
        vec![1, 2, 3],
        MessageMetadata::new(7, None),
    )
}

#[test]
fn structured_dead_letters_round_trip_and_retain_diagnostics() {
    let codec = MemoryPackCodec::default();
    let diagnostics = DeadLetterDiagnostics::try_at(1234, ErrorCode::Conflict, "worker.retry")
        .expect("valid diagnostics");
    let letter =
        DeadLetter::try_with_diagnostics(envelope(), "version conflict", 4, diagnostics)
            .expect("valid dead letter");
    let encoded = encode(&codec, &letter).expect("encode dead letter");
    let decoded = decode(&codec, &encoded).expect("decode dead letter");

    assert_eq!(decoded, letter);
    assert_eq!(decoded.attempts(), 4);
    assert_eq!(decoded.diagnostics().failed_at_unix_ms(), 1234);
    assert_eq!(decoded.diagnostics().error_code(), ErrorCode::Conflict);
    assert_eq!(decoded.diagnostics().stage(), "worker.retry");
}

#[test]
fn legacy_dead_letters_decode_without_diagnostics() {
    let codec = MemoryPackCodec::default();
    let letter = DeadLetter::new(envelope(), "legacy failure", 2);
    let encoded = encode(&codec, &letter).expect("encode dead letter");
    let decoded = decode(&codec, &encoded).expect("decode dead letter");

    assert_eq!(decoded.reason(), "legacy failure");
    assert_eq!(decoded.attempts(), 2);
    assert_eq!(decoded.diagnostics().stage(), "legacy");
    assert_eq!(decoded.diagnostics().error_code(), ErrorCode::Internal);
}

#[test]
fn malformed_dead_letter_records_are_rejected() {
    let codec = MemoryPackCodec::default();
    for value in [vec![], vec![0; 11], {
        let mut value = vec![0; 12];
        value[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
        value
    }] {
        assert_eq!(
            decode(&codec, &value).expect_err("malformed record").code(),
            ErrorCode::Internal
        );
    }

    let letter = DeadLetter::new(envelope(), "reason", 1);
    let mut encoded = encode(&codec, &letter).expect("encode dead letter");
    encoded.extend_from_slice(b"BAD!");
    assert_eq!(
        decode(&codec, &encoded)
            .expect_err("bad diagnostics")
            .code(),
        ErrorCode::Internal
    );
}
