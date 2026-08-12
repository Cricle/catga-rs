//! Event-store edge contracts: provisioning validation, corrupt broker records, and paging.
//!
//! The store treats the JetStream stream as untrusted input: corrupt headers or batch frames
//! must surface as internal errors, and stream-id pages must stay bounded and resumable.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::{self, stream};
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaResult, EnvelopeCodec, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE, PayloadEncoder,
    QualityOfService,
};
use catga_nats::NatsEventStore;
use envelopes::envelope;
use names::unique;
use nats_server::{server_url, test_error};

fn connect_pair() -> (String, String) {
    (unique("CATGA_EVENTS_EDGE"), unique("catga.edge"))
}

async fn publish_raw(subject: &str, headers: &[(&str, &str)], payload: Vec<u8>) -> CatgaResult<()> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw event publisher", error))?;
    let context = jetstream::new(client);
    let mut message = jetstream::message::PublishMessage::build().payload(payload.into());
    for (name, value) in headers {
        message = message.header(*name, *value);
    }
    context
        .send_publish(subject.to_owned(), message)
        .await
        .map_err(|error| test_error("send raw event publish", error))?
        .await
        .map_err(|error| test_error("confirm raw event publish", error))?;
    Ok(())
}

async fn publish_headerless(subject: &str, payload: Vec<u8>) -> CatgaResult<()> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect headerless publisher", error))?;
    let context = jetstream::new(client);
    context
        .publish(subject.to_owned(), payload.into())
        .await
        .map_err(|error| test_error("publish headerless event", error))?
        .await
        .map_err(|error| test_error("confirm headerless event", error))?;
    Ok(())
}

const TIMESTAMP: &str = "1735689600000";

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn connecting_against_an_incompatible_stream_is_a_validation_error() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect stream fixture client", error))?;
    let context = jetstream::new(client);

    // A pre-existing stream that does not cover the requested prefix is rejected.
    context
        .create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("{prefix}.unrelated.>")],
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create incompatible stream", error))?;
    assert!(matches!(
        NatsEventStore::connect(&server_url(), stream_name.as_str(), format!("{prefix}.events"))
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    // A covering wildcard subject without direct reads is upgraded during connect.
    let (direct_stream, direct_prefix) = connect_pair();
    context
        .create_stream(stream::Config {
            name: direct_stream.clone(),
            subjects: vec![format!("{direct_prefix}.>")],
            allow_direct: false,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create indirect stream", error))?;
    let store = NatsEventStore::connect(
        &server_url(),
        direct_stream.as_str(),
        direct_prefix.as_str(),
    )
    .await?;
    let stream_id = unique("order");
    let version = store
        .append(
            &stream_id,
            vec![envelope(1, QualityOfService::AtLeastOnce)],
            None,
        )
        .await?;
    assert_eq!(version, 0);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn stream_id_validation_rejects_subjects_that_break_the_wire_layout() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    for invalid in ["", "a..b", "a.*", "a.>", ".", "ends."] {
        assert!(matches!(
            store
                .append(invalid, vec![envelope(1, QualityOfService::AtLeastOnce)], None)
                .await,
            Err(error) if error.code() == ErrorCode::Validation
        ));
        assert!(matches!(
            store.read_page(invalid, 0, 1).await,
            Err(error) if error.code() == ErrorCode::Validation
        ));
    }
    // Page sizes are bounded at the trait level.
    assert!(matches!(
        store.read_page("order-1", 0, 0).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        store
            .read_page("order-1", 0, MAX_EVENT_STORE_PAGE_SIZE + 1)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn appending_zero_events_reports_the_current_version_without_writes() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let stream_id = unique("order");
    assert_eq!(store.append(&stream_id, Vec::new(), None).await?, -1);
    let version = store
        .append(
            &stream_id,
            vec![envelope(7, QualityOfService::AtLeastOnce)],
            None,
        )
        .await?;
    assert_eq!(version, 0);
    assert_eq!(store.append(&stream_id, Vec::new(), None).await?, 0);
    assert_eq!(store.version(&stream_id).await?, 0);
    assert_eq!(store.version(&unique("missing")).await?, -1);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn corrupt_stored_events_surface_as_internal_errors() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let codec = MemoryPackCodec::default();
    let payload = codec.encode(&envelope(9, QualityOfService::AtLeastOnce))?;

    // (stream id, header set, payload) cases that each violate one decode invariant.
    type CorruptCase = (String, Vec<(&'static str, &'static str)>, Vec<u8>);
    let mut cases: Vec<CorruptCase> = vec![
        (
            unique("corrupt.version"),
            event_headers("not-a-number", TIMESTAMP, None),
            payload.clone(),
        ),
        (
            unique("corrupt.timestamp"),
            event_headers("0", "not-a-number", None),
            payload.clone(),
        ),
        (
            unique("corrupt.count"),
            event_headers("0", TIMESTAMP, Some("not-a-number")),
            payload.clone(),
        ),
        (
            unique("corrupt.empty"),
            event_headers("0", TIMESTAMP, Some("0")),
            payload.clone(),
        ),
        (
            unique("corrupt.huge"),
            event_headers("0", TIMESTAMP, Some("9223372036854775808")),
            payload.clone(),
        ),
        (
            unique("corrupt.overflow"),
            event_headers("-9223372036854775808", TIMESTAMP, Some("1")),
            payload.clone(),
        ),
        (
            unique("corrupt.mismatch"),
            event_headers("1", TIMESTAMP, Some("2")),
            codec.encode_payload(&vec![payload.clone()])?,
        ),
    ];

    for (stream_id, headers, payload) in cases.drain(..) {
        publish_raw(&format!("{prefix}.{stream_id}"), &headers, payload).await?;
        assert!(
            matches!(
                store.read_page(&stream_id, 0, 8).await,
                Err(error) if error.code() == ErrorCode::Internal
            ),
            "stream {stream_id} must surface an internal decode error"
        );
    }

    // A message without any headers fails the same contract.
    let headerless = unique("corrupt.none");
    publish_headerless(&format!("{prefix}.{headerless}"), payload).await?;
    assert!(matches!(
        store.read_page(&headerless, 0, 8).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

fn event_headers<'a>(
    version: &'a str,
    timestamp: &'a str,
    batch_count: Option<&'a str>,
) -> Vec<(&'static str, &'a str)> {
    let mut headers: Vec<(&'static str, &str)> =
        vec![("Catga-Version", version), ("Catga-Timestamp", timestamp)];
    if let Some(batch_count) = batch_count {
        headers.push(("Catga-Batch-Count", batch_count));
    }
    headers
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn unbatched_legacy_records_still_decode_as_single_events() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let codec = MemoryPackCodec::default();
    let stream_id = unique("order.legacy");
    let payload = codec.encode(&envelope(11, QualityOfService::AtLeastOnce))?;
    publish_raw(
        &format!("{prefix}.{stream_id}"),
        &event_headers("4", TIMESTAMP, None),
        payload,
    )
    .await?;

    // The legacy single-event frame reports its recorded version.
    assert_eq!(store.version(&stream_id).await?, 4);
    let page = store.read_page(&stream_id, 0, 8).await?;
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(page.stream().events()[0].version(), 4);
    assert_eq!(page.stream().events()[0].envelope().id(), 11);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_corrupt_latest_record_fails_version_reads() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let codec = MemoryPackCodec::default();
    let stream_id = unique("order.corrupt.latest");
    store
        .append(
            &stream_id,
            vec![envelope(21, QualityOfService::AtLeastOnce)],
            None,
        )
        .await?;
    publish_raw(
        &format!("{prefix}.{stream_id}"),
        &event_headers("not-a-number", TIMESTAMP, None),
        codec.encode(&envelope(22, QualityOfService::AtLeastOnce))?,
    )
    .await?;
    assert!(matches!(
        store.version(&stream_id).await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn stream_id_pages_resume_from_their_cursor() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    for name in ["alpha", "beta", "gamma"] {
        store
            .append(
                &format!("paged.{name}"),
                vec![envelope(31, QualityOfService::AtLeastOnce)],
                None,
            )
            .await?;
    }

    let first = store.stream_ids_page(None, 1).await?;
    assert_eq!(first.ids().len(), 1);
    let cursor = first
        .next_stream_id()
        .map(str::to_owned)
        .expect("first page must resume");
    let second = store.stream_ids_page(Some(cursor.as_str()), 1).await?;
    assert_eq!(second.ids().len(), 1);
    assert_ne!(first.ids(), second.ids());
    let third = store.stream_ids_page(second.next_stream_id(), 8).await?;
    assert_eq!(third.ids().len(), 1);
    assert!(third.next_stream_id().is_none());

    // Page-size validation also guards the identifier scan.
    assert!(matches!(
        store.stream_ids_page(None, 0).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_full_stream_rejects_appends_with_a_transient_error() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect capped stream client", error))?;
    let context = jetstream::new(client);
    // A discard-new stream capped at one message rejects the next append at the broker.
    context
        .create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("{prefix}.>")],
            max_messages: 1,
            discard: stream::DiscardPolicy::New,
            allow_direct: true,
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create capped stream", error))?;
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let stream_id = unique("order.capped");
    store
        .append(
            &stream_id,
            vec![envelope(41, QualityOfService::AtLeastOnce)],
            None,
        )
        .await?;
    assert!(matches!(
        store
            .append(&stream_id, vec![envelope(42, QualityOfService::AtLeastOnce)], None)
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn read_pages_bound_versions_timestamps_and_history() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let stream_id = unique("order.history");
    let events: Vec<_> = (0..4)
        .map(|id| envelope(id, QualityOfService::AtLeastOnce))
        .collect();
    let version = store.append(&stream_id, events, None).await?;
    assert_eq!(version, 3);

    // A version bound pages up to and including it; the cursor resumes after the page.
    let bounded = store.read_to_version_page(&stream_id, 0, 3, 2).await?;
    assert_eq!(bounded.stream().events().len(), 2);
    assert_eq!(bounded.next_version(), Some(2));
    let rest = store.read_to_version_page(&stream_id, 2, 3, 8).await?;
    assert_eq!(rest.stream().events().len(), 2);
    assert_eq!(rest.next_version(), None);

    // A time bound excludes later events entirely.
    let timed = store
        .read_to_time_page(&stream_id, 0, SystemTime::now() + Duration::from_secs(1), 8)
        .await?;
    assert_eq!(timed.stream().events().len(), 4);
    let epoch = store
        .read_to_time_page(&stream_id, 0, UNIX_EPOCH, 8)
        .await?;
    assert!(epoch.stream().events().is_empty());

    // Version history pages carry the same cursor contract.
    let history = store.version_history_page(&stream_id, 0, 2).await?;
    assert_eq!(history.entries().len(), 2);
    assert_eq!(history.next_version(), Some(2));
    let rest = store.version_history_page(&stream_id, 2, 8).await?;
    assert_eq!(rest.entries().len(), 2);
    assert_eq!(rest.next_version(), None);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn an_exhausted_stream_version_rejects_further_appends() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let codec = MemoryPackCodec::default();
    let stream_id = unique("order.maxed");
    publish_raw(
        &format!("{prefix}.{stream_id}"),
        &event_headers("9223372036854775807", TIMESTAMP, None),
        codec.encode(&envelope(51, QualityOfService::AtLeastOnce))?,
    )
    .await?;
    assert!(matches!(
        store
            .append(&stream_id, vec![envelope(52, QualityOfService::AtLeastOnce)], None)
            .await,
        Err(error) if error.code() == ErrorCode::Internal
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_literal_stream_subject_does_not_cover_the_prefix() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect literal stream client", error))?;
    jetstream::new(client)
        .create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![format!("{prefix}.events.literal")],
            ..Default::default()
        })
        .await
        .map_err(|error| test_error("create literal stream", error))?;
    assert!(matches!(
        NatsEventStore::connect(&server_url(), stream_name.as_str(), format!("{prefix}.events"))
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn a_history_scan_beyond_the_message_bound_reports_unavailable() -> CatgaResult<()> {
    let (stream_name, prefix) = connect_pair();
    let store =
        NatsEventStore::connect(&server_url(), stream_name.as_str(), prefix.as_str()).await?;
    let codec = MemoryPackCodec::default();
    let stream_id = unique("order.deep");
    let payload = codec.encode(&envelope(61, QualityOfService::AtLeastOnce))?;
    // One more message than the bounded scan inspects, all below the requested version.
    for version in 0..=1024_i64 {
        publish_raw(
            &format!("{prefix}.{stream_id}"),
            &event_headers(&version.to_string(), TIMESTAMP, None),
            payload.clone(),
        )
        .await?;
    }
    assert!(matches!(
        store.read_page(&stream_id, 10_000, 1).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    Ok(())
}
