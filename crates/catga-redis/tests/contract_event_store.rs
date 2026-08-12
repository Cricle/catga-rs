//! Service-backed contract coverage for [`RedisEventStore`].

use std::time::{Duration, UNIX_EPOCH};

use catga_core::{CatgaResult, ErrorCode, EventStore};
use catga_redis::RedisEventStore;
use redis::AsyncCommands;

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/ids.rs"]
mod ids;
#[path = "support/raw.rs"]
mod raw;
#[path = "support/redis_err.rs"]
mod redis_err;
#[path = "support/service_url.rs"]
mod service_url;

use envelopes::envelope;
use ids::unique_prefix;
use raw::raw_connection;
use redis_err::map_redis_error;

#[tokio::test]
async fn append_read_and_version_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;

    // A missing stream reports version -1, with or without an empty append probe.
    assert_eq!(store.version("stream-a").await?, -1);
    assert_eq!(store.append("stream-a", Vec::new(), None).await?, -1);

    // Appending without an expected version assigns sequential versions.
    let events = vec![
        envelope(11, "catga.test.created"),
        envelope(12, "catga.test.paid"),
        envelope(13, "catga.test.shipped"),
    ];
    assert_eq!(store.append("stream-a", events, None).await?, 2);
    assert_eq!(store.version("stream-a").await?, 2);

    // An empty append returns the current version without writing.
    assert_eq!(store.append("stream-a", Vec::new(), None).await?, 2);

    // A matching expected version passes; a stale one conflicts.
    assert_eq!(
        store
            .append("stream-a", vec![envelope(14, "catga.test.closed")], Some(2))
            .await?,
        3
    );
    let conflict = store
        .append(
            "stream-a",
            vec![envelope(15, "catga.test.reopened")],
            Some(2),
        )
        .await;
    assert!(matches!(conflict, Err(error) if error.code() == ErrorCode::Conflict));

    // A mismatched expectation on a missing stream also conflicts.
    let missing_conflict = store
        .append(
            "stream-b",
            vec![envelope(16, "catga.test.created")],
            Some(5),
        )
        .await;
    assert!(matches!(missing_conflict, Err(error) if error.code() == ErrorCode::Conflict));

    Ok(())
}

#[tokio::test]
async fn append_rejects_an_empty_stream_id() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;

    let result = store
        .append("", vec![envelope(1, "catga.test.event")], None)
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    // The identifier check runs before the empty-batch fast path.
    let empty_batch = store.append("", Vec::new(), None).await;
    assert!(matches!(empty_batch, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn read_page_paginates_with_next_version() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;
    let events: Vec<_> = (1..=5).map(|id| envelope(id, "catga.test.event")).collect();
    assert_eq!(store.append("paged", events, None).await?, 4);

    let first = store.read_page("paged", 0, 2).await?;
    assert_eq!(first.stream().stream_id(), "paged");
    assert_eq!(first.stream().version(), 4);
    assert_eq!(first.stream().events().len(), 2);
    assert_eq!(first.stream().events()[0].version(), 0);
    assert_eq!(first.next_version(), Some(2));

    let second = store
        .read_page(
            "paged",
            first.next_version().expect("page must continue"),
            2,
        )
        .await?;
    assert_eq!(second.stream().events().len(), 2);
    assert_eq!(second.next_version(), Some(4));

    let last = store.read_page("paged", 4, 2).await?;
    assert_eq!(last.stream().events().len(), 1);
    assert_eq!(last.next_version(), None);

    let missing = store.read_page("missing", 0, 10).await?;
    assert_eq!(missing.stream().version(), -1);
    assert!(missing.stream().events().is_empty());
    assert_eq!(missing.next_version(), None);

    Ok(())
}

#[tokio::test]
async fn read_pages_validate_their_size_and_entry_range() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;

    for result in [
        store.read_page("s", 0, 0).await.map(|_| ()),
        store.read_page("s", 0, 1_025).await.map(|_| ()),
        store.read_to_version_page("s", 0, 1, 0).await.map(|_| ()),
        store
            .read_to_time_page("s", 0, std::time::SystemTime::now(), 0)
            .await
            .map(|_| ()),
        store.version_history_page("s", 0, 0).await.map(|_| ()),
        store.stream_ids_page(None, 0).await.map(|_| ()),
    ] {
        assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    }

    // from_version at the Redis entry-id ceiling is a validation error, not a panic.
    let overflow = store.read_page("s", u64::MAX, 1).await;
    assert!(matches!(overflow, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn read_to_version_page_bounds_results() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;
    let events: Vec<_> = (1..=4).map(|id| envelope(id, "catga.test.event")).collect();
    assert_eq!(store.append("bounded", events, None).await?, 3);

    // A negative upper bound yields an empty page without touching Redis ranges.
    let empty = store.read_to_version_page("bounded", 0, -1, 10).await?;
    assert!(empty.stream().events().is_empty());
    assert_eq!(empty.stream().version(), -1);
    assert_eq!(empty.next_version(), None);

    let partial = store.read_to_version_page("bounded", 0, 1, 10).await?;
    assert_eq!(partial.stream().events().len(), 2);
    assert_eq!(partial.stream().version(), 1);
    assert_eq!(partial.next_version(), None);

    // A page boundary inside the requested range keeps a continuation cursor.
    let windowed = store.read_to_version_page("bounded", 0, 3, 2).await?;
    assert_eq!(windowed.stream().events().len(), 2);
    assert_eq!(windowed.next_version(), Some(2));

    Ok(())
}

#[tokio::test]
async fn read_to_time_page_filters_by_the_upper_bound() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;
    let events: Vec<_> = (1..=3).map(|id| envelope(id, "catga.test.event")).collect();
    assert_eq!(store.append("timed", events, None).await?, 2);

    let everything = store
        .read_to_time_page("timed", 0, std::time::SystemTime::now(), 10)
        .await?;
    assert_eq!(everything.stream().events().len(), 3);
    assert_eq!(everything.stream().version(), 2);
    assert_eq!(everything.next_version(), None);

    // Every stored timestamp follows the Unix epoch, so nothing qualifies.
    let nothing = store.read_to_time_page("timed", 0, UNIX_EPOCH, 10).await?;
    assert!(nothing.stream().events().is_empty());
    assert_eq!(nothing.stream().version(), -1);
    assert_eq!(nothing.next_version(), None);

    Ok(())
}

#[tokio::test]
async fn version_history_page_lists_event_metadata() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;
    let events = vec![
        envelope(21, "catga.test.first"),
        envelope(22, "catga.test.second"),
    ];
    assert_eq!(store.append("history", events, None).await?, 1);

    let page = store.version_history_page("history", 0, 10).await?;
    assert_eq!(page.entries().len(), 2);
    assert_eq!(page.entries()[0].version(), 0);
    assert_eq!(page.entries()[0].event_type(), "catga.test.first");
    assert_eq!(page.entries()[1].event_type(), "catga.test.second");
    assert_eq!(page.next_version(), None);

    let windowed = store.version_history_page("history", 0, 1).await?;
    assert_eq!(windowed.entries().len(), 1);
    assert_eq!(windowed.next_version(), Some(1));

    Ok(())
}

#[tokio::test]
async fn stream_ids_page_scans_in_sorted_order_with_a_cursor() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect(&url, unique_prefix("events")).await?;
    for (index, stream) in ["stream-a", "stream-b", "stream-c"].iter().enumerate() {
        let id = (index + 1) as u64;
        assert_eq!(
            store
                .append(stream, vec![envelope(id, "catga.test.event")], None)
                .await?,
            0
        );
    }

    let first = store.stream_ids_page(None, 2).await?;
    assert_eq!(
        first.ids(),
        &["stream-a".to_string(), "stream-b".to_string()]
    );
    let cursor = first
        .next_stream_id()
        .expect("a third stream must keep the page open")
        .to_string();

    let second = store.stream_ids_page(Some(&cursor), 2).await?;
    assert_eq!(second.ids(), &["stream-c".to_string()]);
    assert_eq!(second.next_stream_id(), None);

    Ok(())
}

#[tokio::test]
async fn stream_ids_page_scans_large_sets_across_cursors() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("events");
    let store = RedisEventStore::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // Two hundred ids exceed Redis's listpack set threshold, so SSCAN walks a
    // hash table in bounded COUNT batches instead of returning one snapshot.
    let ids: Vec<String> = (0..200).map(|index| format!("stream-{index:04}")).collect();
    for chunk in ids.chunks(40) {
        let _: usize = raw
            .sadd(format!("{prefix}:ids"), chunk)
            .await
            .map_err(map_redis_error)?;
    }

    // The first page keeps the ten smallest ids no matter how the scan ordered them.
    let first = store.stream_ids_page(None, 10).await?;
    assert_eq!(first.ids(), &ids[..10]);
    let cursor = first
        .next_stream_id()
        .expect("a two-hundred-id set must keep paging")
        .to_string();

    let rest = store.stream_ids_page(Some(&cursor), 200).await?;
    assert_eq!(rest.ids(), &ids[10..]);
    assert_eq!(rest.next_stream_id(), None);

    Ok(())
}

#[tokio::test]
async fn malformed_stream_entries_surface_internal_errors() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("events");
    let store = RedisEventStore::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    let stream_key = |stream: &str| format!("{prefix}:stream:{stream}");

    // Missing version field.
    let _: String = raw
        .xadd(
            stream_key("broken-version"),
            "*",
            &[("payload", b"x".to_vec()), ("timestamp", b"1".to_vec())],
        )
        .await
        .map_err(map_redis_error)?;
    let result = store.read_page("broken-version", 0, 10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // Missing payload field.
    let _: String = raw
        .xadd(
            stream_key("broken-payload"),
            "*",
            &[("version", b"0".to_vec()), ("timestamp", b"1".to_vec())],
        )
        .await
        .map_err(map_redis_error)?;
    let result = store.read_page("broken-payload", 0, 10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // Missing timestamp field.
    let _: String = raw
        .xadd(
            stream_key("broken-timestamp"),
            "*",
            &[("version", b"0".to_vec()), ("payload", b"x".to_vec())],
        )
        .await
        .map_err(map_redis_error)?;
    let result = store.read_page("broken-timestamp", 0, 10).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // Payload bytes that are not a valid envelope frame.
    let _: String = raw
        .xadd(
            stream_key("broken-codec"),
            "*",
            &[
                ("version", b"0".to_vec()),
                ("payload", b"not-a-frame".to_vec()),
                ("timestamp", b"1".to_vec()),
            ],
        )
        .await
        .map_err(map_redis_error)?;
    assert!(store.read_page("broken-codec", 0, 10).await.is_err());

    Ok(())
}

#[tokio::test]
async fn connect_with_options_applies_the_command_timeout() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let store = RedisEventStore::connect_with_options(
        &url,
        unique_prefix("events"),
        catga_redis::RedisCommandOptions::new(Duration::from_secs(2))?,
    )
    .await?;
    assert_eq!(store.version("anything").await?, -1);
    Ok(())
}

#[tokio::test]
async fn append_maps_version_exhaustion_to_an_internal_error() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("events");
    let store = RedisEventStore::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A stream version at the i64 ceiling cannot advance inside the append script.
    let _: () = raw
        .set(format!("{prefix}:version:exhausted"), "9223372036854775807")
        .await
        .map_err(map_redis_error)?;
    let result = store
        .append(
            "exhausted",
            vec![envelope(61, "catga.test.event")],
            Some(i64::MAX),
        )
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));
    Ok(())
}

#[tokio::test]
async fn append_maps_unexpected_script_failures_to_transient_errors() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = unique_prefix("events");
    let store = RedisEventStore::connect(&url, prefix.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // A string value where the stream-id set belongs breaks SADD with WRONGTYPE.
    let _: () = raw
        .set(format!("{prefix}:ids"), "not-a-set")
        .await
        .map_err(map_redis_error)?;
    let result = store
        .append("stream-x", vec![envelope(62, "catga.test.event")], None)
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}
