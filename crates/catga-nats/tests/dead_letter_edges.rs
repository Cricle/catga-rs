//! Dead-letter edge contracts: corrupt broker records surface as internal errors.
//!
//! The store treats the stream as untrusted input: length prefixes, the diagnostics
//! magic, UTF-8 boundaries, and the stable error-code registry are all validated on
//! read, so a hand-corrupted record fails the whole listing loudly instead of
//! decoding garbage.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;

use async_nats::jetstream;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{CatgaResult, DeadLetterStore, EnvelopeCodec, ErrorCode, QualityOfService};
use catga_nats::NatsDeadLetters;
use envelopes::envelope;
use names::unique;
use nats_server::{server_url, test_error};

/// Builds a structurally valid dead-letter head: attempts, lengths, reason, envelope.
fn letter_head(id: u64) -> Vec<u8> {
    let envelope = MemoryPackCodec::default()
        .encode(&envelope(id, QualityOfService::AtLeastOnce))
        .expect("envelope must encode");
    let reason = b"handler poisoned";
    let mut value = Vec::new();
    value.extend_from_slice(&1u32.to_be_bytes());
    value.extend_from_slice(&(reason.len() as u32).to_be_bytes());
    value.extend_from_slice(&(envelope.len() as u32).to_be_bytes());
    value.extend_from_slice(reason);
    value.extend_from_slice(&envelope);
    value
}

async fn publish_raw(subject: &str, payload: Vec<u8>) -> CatgaResult<()> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw letter publisher", error))?;
    jetstream::new(client)
        .publish(subject.to_owned(), payload.into())
        .await
        .map_err(|error| test_error("publish raw letter", error))?
        .await
        .map_err(|error| test_error("confirm raw letter", error))?;
    Ok(())
}

async fn connect_pair() -> CatgaResult<(NatsDeadLetters, String)> {
    let subject = unique("catga.dlq.edge");
    let store =
        NatsDeadLetters::connect(&server_url(), unique("CATGA_DLQ_EDGE"), subject.as_str()).await?;
    Ok((store, subject))
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_letter_lengths_are_internal_errors() -> CatgaResult<()> {
    let (store, subject) = connect_pair().await?;
    // The reason length outruns the record: 12 + u32::MAX cannot be a real letter.
    let mut corrupt = Vec::new();
    corrupt.extend_from_slice(&1u32.to_be_bytes());
    corrupt.extend_from_slice(&u32::MAX.to_be_bytes());
    corrupt.extend_from_slice(&0u32.to_be_bytes());
    corrupt.extend_from_slice(&[0; 8]);
    publish_raw(&format!("{subject}.1"), corrupt).await?;
    assert!(matches!(
        store.list(8).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_diagnostics_trailers_are_internal_errors() -> CatgaResult<()> {
    // Each corrupt trailer gets its own stream so one failure cannot mask the next.
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

    // A trailer without the diagnostics magic.
    let mut bad_magic = letter_head(11);
    bad_magic.extend_from_slice(b"GARBAGEGARBAGE!");
    cases.push(("magic", bad_magic));

    // Declared diagnostics lengths that disagree with the trailer size.
    let mut mismatched = letter_head(12);
    mismatched.extend_from_slice(b"DLQ2");
    mismatched.extend_from_slice(&1_735_689_600_000u64.to_be_bytes());
    mismatched.push(3);
    mismatched.push(3);
    mismatched.extend_from_slice(b"four");
    cases.push(("lengths", mismatched));

    // An error code that is not UTF-8.
    let mut bad_utf8 = letter_head(13);
    bad_utf8.extend_from_slice(b"DLQ2");
    bad_utf8.extend_from_slice(&1_735_689_600_000u64.to_be_bytes());
    bad_utf8.push(1);
    bad_utf8.push(0);
    bad_utf8.push(0xFF);
    cases.push(("utf8", bad_utf8));

    // An error code outside the stable registry.
    let mut unknown = letter_head(14);
    unknown.extend_from_slice(b"DLQ2");
    unknown.extend_from_slice(&1_735_689_600_000u64.to_be_bytes());
    unknown.push(5);
    unknown.push(0);
    unknown.extend_from_slice(b"bogus");
    cases.push(("unknown", unknown));

    // A stage beyond the durable diagnostics budget.
    let mut long_stage = letter_head(15);
    long_stage.extend_from_slice(b"DLQ2");
    long_stage.extend_from_slice(&1_735_689_600_000u64.to_be_bytes());
    long_stage.push(8);
    long_stage.push(100);
    long_stage.extend_from_slice(b"internal");
    long_stage.extend_from_slice(&[b's'; 100]);
    cases.push(("stage", long_stage));

    for (name, record) in cases {
        let (store, subject) = connect_pair().await?;
        publish_raw(&format!("{subject}.1"), record).await?;
        assert!(
            matches!(
                store.list(8).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name} must surface an internal decode error"
        );
    }
    Ok(())
}
