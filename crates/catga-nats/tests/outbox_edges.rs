//! Outbox edge contracts: legacy wire layouts, corrupt broker records, claim-update
//! broker failures, and retry/cleanup state-machine bounds.
//!
//! The store reads whatever the bucket holds, so superseded record layouts must still
//! decode, malformed records must surface as internal errors, and broker-level write
//! failures must not be mistaken for revision conflicts.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/raw_capped_kv.rs"]
mod raw_capped_kv;
#[path = "support/raw_kv.rs"]
mod raw_kv;

use std::time::Duration;

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, EnvelopeCodec, ErrorCode, OutboxMessage, OutboxState, OutboxStore,
    QualityOfService,
};
use catga_nats::NatsOutbox;
use envelopes::envelope;
use names::unique;
use nats_server::server_url;
use raw_capped_kv::raw_kv_with_byte_cap;
use raw_kv::raw_kv;

fn key(id: u64) -> String {
    format!("m{id:020}")
}

fn test_error(context: &'static str, error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, context).with_details(error.to_string())
}

/// Encodes the envelope payload every record layout embeds.
fn payload(id: u64) -> CatgaResult<Vec<u8>> {
    MemoryPackCodec::default().encode(&envelope(id, QualityOfService::AtLeastOnce))
}

/// Builds a versioned record in one of the superseded layouts.
///
/// Field order matches the wire contract: state, owner length, retry and max-retry
/// counters, error length, then the optional published/claimed timestamps the layout
/// version carries, followed by owner, error, and payload bytes.
fn versioned_record(
    magic: &[u8; 5],
    state: u8,
    owner: &str,
    published_at: Option<u64>,
    id: u64,
) -> Vec<u8> {
    let includes_published_at = magic != b"CGOB\x01";
    let includes_claimed_until = magic == b"CGOB\x04" || magic == b"CGOB\x03";
    let includes_claim_token = magic == b"CGOB\x04";
    let mut value = Vec::new();
    value.extend_from_slice(magic);
    value.push(state);
    value.extend_from_slice(&(owner.len() as u16).to_be_bytes());
    if includes_claim_token {
        value.extend_from_slice(&0u16.to_be_bytes());
    }
    value.extend_from_slice(&0u32.to_be_bytes());
    value.extend_from_slice(&3u32.to_be_bytes());
    value.extend_from_slice(&0u16.to_be_bytes());
    if includes_published_at {
        value.extend_from_slice(&published_at.unwrap_or(u64::MAX).to_be_bytes());
    }
    if includes_claimed_until {
        value.extend_from_slice(&u64::MAX.to_be_bytes());
    }
    value.extend_from_slice(owner.as_bytes());
    value.extend_from_slice(&payload(id).expect("payload must encode"));
    value
}

/// Builds a pre-magic legacy record: owner length, owner, payload.
fn legacy_record(owner: &str, id: u64) -> Vec<u8> {
    let mut value = Vec::new();
    value.extend_from_slice(&(owner.len() as u16).to_be_bytes());
    value.extend_from_slice(owner.as_bytes());
    value.extend_from_slice(&payload(id).expect("payload must encode"));
    value
}

async fn inject(bucket: &str, record_key: &str, value: Vec<u8>) -> CatgaResult<()> {
    raw_kv(bucket)
        .await?
        .put(record_key, value.into())
        .await
        .map_err(|error| test_error("inject raw outbox record", error))?;
    Ok(())
}

async fn connect(bucket: &str) -> CatgaResult<NatsOutbox> {
    NatsOutbox::connect(&server_url(), bucket).await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn missing_and_delete_marked_messages_are_no_ops() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_NOOP");
    let outbox = connect(&bucket).await?;

    // Every owner-scoped transition on an unknown identifier is a silent miss.
    outbox.ack("owner", 404, "token").await?;
    outbox.release("owner", 404, "token").await?;
    outbox.record_failure("owner", 404, "token", "gone").await?;
    assert!(!outbox.cancel(404).await?);

    // A delete-marked record behaves the same way.
    outbox
        .enqueue(OutboxMessage::new(envelope(
            5,
            QualityOfService::AtLeastOnce,
        )))
        .await?;
    raw_kv(&bucket)
        .await?
        .delete(key(5))
        .await
        .map_err(|error| test_error("delete outbox record", error))?;
    outbox.ack("owner", 5, "token").await?;
    assert!(!outbox.cancel(5).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claim_validates_its_inputs_before_record_io() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_CLAIM");
    let outbox = connect(&bucket).await?;

    // Trait-level bounds are fenced before any record I/O.
    assert!(matches!(
        outbox
            .enqueue(OutboxMessage::new(envelope(0, QualityOfService::AtLeastOnce)))
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(outbox.claim("owner", 0).await?.is_empty());
    assert!(matches!(
        outbox.claim("owner", 1025).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        outbox.claim_for("owner", 8, Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        outbox.list_published(1025).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    // A duplicate enqueue conflicts.
    outbox
        .enqueue(OutboxMessage::new(envelope(
            7,
            QualityOfService::AtLeastOnce,
        )))
        .await?;
    assert!(matches!(
        outbox
            .enqueue(OutboxMessage::new(envelope(7, QualityOfService::AtLeastOnce)))
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));

    // An owner that cannot fit the record header is a validation error.
    assert!(matches!(
        outbox.claim(&"o".repeat(70_000), 8).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn claim_reports_broker_write_failures_as_transient() -> CatgaResult<()> {
    // The bucket budget admits the enqueue but not the claimed rewrite: discarding the
    // old revision happens after the byte check, so the compare-and-set publish fails
    // with a storage error rather than a revision conflict.
    let bucket = unique("CATGA_OUTBOX_CAPPED");
    raw_kv_with_byte_cap(&bucket, 2_500).await?;
    let outbox = connect(&bucket).await?;
    outbox
        .enqueue(OutboxMessage::new(catga_core::Envelope::new(
            11,
            "nats.coverage",
            vec![0xCD; 1_400],
            catga_core::MessageMetadata::new(11, None)
                .with_quality_of_service(QualityOfService::AtLeastOnce),
        )))
        .await?;

    for attempt in 0..2 {
        assert!(
            matches!(
                outbox.claim("owner", 8).await,
                Err(error) if error.code() == ErrorCode::Transient
            ),
            "attempt {attempt} must surface the broker storage error"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn published_listing_is_bounded_sorted_and_restores_legacy_timestamps() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_PUB");
    let outbox = connect(&bucket).await?;
    for id in [11, 12, 13] {
        outbox
            .enqueue(OutboxMessage::new(envelope(
                id,
                QualityOfService::AtLeastOnce,
            )))
            .await?;
    }
    for message in outbox.claim("owner", 8).await? {
        outbox
            .ack(
                message.owner().unwrap_or_default(),
                message.id(),
                message.claim_token().unwrap_or_default(),
            )
            .await?;
    }

    // A legacy record without its published timestamp is restored from broker metadata.
    inject(
        &bucket,
        &key(99),
        versioned_record(b"CGOB\x03", 3, "", None, 99),
    )
    .await?;

    let published = outbox.list_published(8).await?;
    assert_eq!(published.len(), 4);
    assert!(
        published
            .iter()
            .all(|message| message.state() == OutboxState::Published)
    );
    let ids: Vec<u64> = published.iter().map(|message| message.id()).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable_by_key(|id| {
        (
            published
                .iter()
                .find(|message| &message.id() == id)
                .and_then(|message| message.published_at_unix_ms())
                .unwrap_or(u64::MAX),
            *id,
        )
    });
    assert_eq!(ids, sorted);

    // The listing is bounded by its limit.
    assert_eq!(outbox.list_published(1).await?.len(), 1);
    assert!(outbox.list_published(0).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn superseded_record_layouts_decode_and_remain_claimable() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_LEGACY");
    let outbox = connect(&bucket).await?;

    // CGOB\x02 carries no claim lease; CGOB\x01 carries neither timestamp; the pre-magic
    // layout carries only an owner. Claimed-without-deadline records are treated as
    // expired so an upgrade recovers them.
    inject(
        &bucket,
        &key(21),
        versioned_record(b"CGOB\x02", 0, "", None, 21),
    )
    .await?;
    inject(
        &bucket,
        &key(22),
        versioned_record(b"CGOB\x01", 1, "legacy-owner", None, 22),
    )
    .await?;
    inject(&bucket, &key(23), legacy_record("ancient-owner", 23)).await?;
    inject(&bucket, &key(24), legacy_record("", 24)).await?;

    let claimed = outbox.claim("owner", 8).await?;
    let mut ids: Vec<u64> = claimed.iter().map(|message| message.id()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![21, 22, 23, 24]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn malformed_records_fail_with_internal_errors() -> CatgaResult<()> {
    // Each corrupt record gets its own bucket so one failure cannot mask the next.
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();

    // An unknown state byte.
    cases.push(("state", versioned_record(b"CGOB\x04", 9, "", None, 31)));
    // A truncated versioned prefix.
    cases.push(("truncated", b"CGOB\x04\x00\x00\x00".to_vec()));
    // A versioned record whose owner length outruns its bytes.
    let mut long_owner = Vec::new();
    long_owner.extend_from_slice(b"CGOB\x04");
    long_owner.push(0);
    long_owner.extend_from_slice(&u16::MAX.to_be_bytes());
    long_owner.extend_from_slice(&[0; 28]);
    cases.push(("lengths", long_owner));
    // A versioned record whose owner is not UTF-8.
    let mut bad_utf8 = Vec::new();
    bad_utf8.extend_from_slice(b"CGOB\x04");
    bad_utf8.push(1);
    bad_utf8.extend_from_slice(&1u16.to_be_bytes());
    bad_utf8.extend_from_slice(&0u16.to_be_bytes());
    bad_utf8.extend_from_slice(&0u32.to_be_bytes());
    bad_utf8.extend_from_slice(&3u32.to_be_bytes());
    bad_utf8.extend_from_slice(&0u16.to_be_bytes());
    bad_utf8.extend_from_slice(&u64::MAX.to_be_bytes());
    bad_utf8.extend_from_slice(&u64::MAX.to_be_bytes());
    bad_utf8.push(0xFF);
    cases.push(("utf8", bad_utf8));
    // A legacy record shorter than its length prefix.
    cases.push(("short-legacy", vec![0x00]));
    // A legacy record whose owner length outruns its bytes.
    cases.push(("legacy-lengths", vec![0x00, 0x64, b'a']));

    for (name, value) in cases {
        let bucket = unique("CATGA_OUTBOX_CORRUPT");
        let outbox = connect(&bucket).await?;
        inject(&bucket, &key(31), value).await?;
        assert!(
            matches!(
                outbox.claim("owner", 8).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "case {name} must surface an internal decode error"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn failures_retry_until_the_message_is_terminal() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_RETRY");
    let outbox = connect(&bucket).await?;
    outbox
        .enqueue(
            OutboxMessage::new(envelope(41, QualityOfService::AtLeastOnce)).with_max_retries(2)?,
        )
        .await?;

    // The first failure returns the message to pending with its retry recorded.
    let first = outbox.claim("owner", 1).await?.remove(0);
    outbox
        .record_failure(
            first.owner().unwrap_or_default(),
            first.id(),
            first.claim_token().unwrap_or_default(),
            "transient handler error",
        )
        .await?;
    let retried = outbox.claim("owner", 1).await?;
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].retry_count(), 1);
    assert_eq!(
        retried[0].last_error().unwrap_or_default(),
        "transient handler error"
    );

    // A wrong owner or token cannot record a failure.
    outbox
        .record_failure(
            "impostor",
            retried[0].id(),
            retried[0].claim_token().unwrap_or_default(),
            "nope",
        )
        .await?;
    outbox
        .record_failure("owner", retried[0].id(), "wrong-token", "nope")
        .await?;
    // The live claim still fences re-claims.
    assert!(outbox.claim("owner", 1).await?.is_empty());

    // The second failure exhausts the retry budget: the message leaves the claim set.
    outbox
        .record_failure(
            retried[0].owner().unwrap_or_default(),
            retried[0].id(),
            retried[0].claim_token().unwrap_or_default(),
            "fatal",
        )
        .await?;
    assert!(outbox.claim("owner", 8).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_exhausted_message_stays_terminal() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_DEAD");
    let outbox = connect(&bucket).await?;
    outbox
        .enqueue(
            OutboxMessage::new(envelope(43, QualityOfService::AtLeastOnce)).with_max_retries(1)?,
        )
        .await?;
    let claimed = outbox.claim("owner", 1).await?.remove(0);
    outbox
        .record_failure(
            claimed.owner().unwrap_or_default(),
            claimed.id(),
            claimed.claim_token().unwrap_or_default(),
            "fatal",
        )
        .await?;
    assert!(outbox.claim("owner", 8).await?.is_empty());
    // A terminal message rejects cancellation, which is reserved for pending records.
    assert!(!outbox.cancel(43).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn cleanup_removes_only_published_records_older_than_retention() -> CatgaResult<()> {
    let bucket = unique("CATGA_OUTBOX_SWEEP");
    let outbox = connect(&bucket).await?;
    for id in [51, 52] {
        outbox
            .enqueue(OutboxMessage::new(envelope(
                id,
                QualityOfService::AtLeastOnce,
            )))
            .await?;
    }
    for message in outbox.claim("owner", 8).await? {
        outbox
            .ack(
                message.owner().unwrap_or_default(),
                message.id(),
                message.claim_token().unwrap_or_default(),
            )
            .await?;
    }
    // A still-pending record is never swept.
    outbox
        .enqueue(OutboxMessage::new(envelope(
            53,
            QualityOfService::AtLeastOnce,
        )))
        .await?;

    assert_eq!(outbox.cleanup_published(Duration::ZERO, 0).await?, 0);
    assert_eq!(
        outbox
            .cleanup_published(Duration::from_secs(3600), 8)
            .await?,
        0
    );
    assert!(matches!(
        outbox.cleanup_published(Duration::MAX, 8).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        outbox.cleanup_published(Duration::ZERO, usize::MAX).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    assert_eq!(outbox.cleanup_published(Duration::ZERO, 8).await?, 2);
    assert!(outbox.list_published(8).await?.is_empty());
    Ok(())
}
