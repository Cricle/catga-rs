//! Event store contract tests.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use catga_core::{Envelope, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE, MessageMetadata};
use catga_memory::MemoryEventStore;

fn event(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[tokio::test]
async fn event_store_pages_are_bounded_validated_and_cursor_resumable() {
    let store = MemoryEventStore::default();
    for id in 0..5 {
        store.append("paged", vec![event(id)], None).await.unwrap();
    }
    store
        .append("z-stream", vec![event(10)], None)
        .await
        .unwrap();
    store
        .append("a-stream", vec![event(11)], None)
        .await
        .unwrap();

    assert_eq!(
        store.read_page("paged", 0, 0).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(
        store
            .read_page("paged", 0, MAX_EVENT_STORE_PAGE_SIZE + 1)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    let first = store.read_page("paged", 0, 2).await.unwrap();
    assert_eq!(
        first
            .stream()
            .events()
            .iter()
            .map(|event| event.version())
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let second = store
        .read_page("paged", first.next_version().unwrap(), 2)
        .await
        .unwrap();
    assert_eq!(
        second
            .stream()
            .events()
            .iter()
            .map(|event| event.version())
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let third = store
        .read_page("paged", second.next_version().unwrap(), 2)
        .await
        .unwrap();
    assert_eq!(
        third
            .stream()
            .events()
            .iter()
            .map(|event| event.version())
            .collect::<Vec<_>>(),
        [4]
    );
    assert_eq!(third.next_version(), None);

    let ids = store.stream_ids_page(None, 2).await.unwrap();
    assert_eq!(ids.ids(), ["a-stream", "paged"]);
    let final_ids = store
        .stream_ids_page(ids.next_stream_id(), 2)
        .await
        .unwrap();
    assert_eq!(final_ids.ids(), ["z-stream"]);
    assert_eq!(final_ids.next_stream_id(), None);
}

#[tokio::test]
async fn event_store_appends_with_optimistic_concurrency_and_reads_immutable_snapshots() {
    let store = MemoryEventStore::default();
    assert_eq!(
        store.append("order-1", vec![event(1)], None).await.unwrap(),
        0
    );
    assert_eq!(
        store
            .append("order-1", vec![event(2)], Some(0))
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .append("order-1", vec![event(3)], Some(0))
            .await
            .is_err()
    );

    let page = store.read_page("order-1", 1, 1).await.unwrap();
    let stream = page.stream();
    assert_eq!(stream.version(), 1);
    assert_eq!(stream.events().len(), 1);
    assert_eq!(stream.events()[0].version(), 1);
    assert_eq!(stream.events()[0].envelope().id(), 2);
    assert_eq!(
        store.stream_ids_page(None, 1).await.unwrap().ids(),
        ["order-1"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_publish_every_immutable_event_snapshot() {
    let store = Arc::new(MemoryEventStore::default());
    let mut writes = tokio::task::JoinSet::new();
    for id in 0..64 {
        let store = Arc::clone(&store);
        writes.spawn(async move { store.append("concurrent", vec![event(id)], None).await });
    }
    while let Some(write) = writes.join_next().await {
        write.unwrap().unwrap();
    }
    assert_eq!(
        store
            .read_page("concurrent", 0, 64)
            .await
            .unwrap()
            .stream()
            .events()
            .len(),
        64
    );
}

#[tokio::test]
async fn event_store_supports_time_travel_and_lightweight_version_history() {
    let store = MemoryEventStore::default();
    store.append("history", vec![event(1)], None).await.unwrap();
    store
        .append("history", vec![event(2)], Some(0))
        .await
        .unwrap();

    let at_zero = store
        .read_to_version_page("history", 0, 0, 2)
        .await
        .unwrap();
    let at_zero = at_zero.stream();
    assert_eq!(at_zero.version(), 0);
    assert_eq!(at_zero.events().len(), 1);
    assert_eq!(
        store
            .read_to_time_page("history", 0, SystemTime::now() + Duration::from_secs(1), 2)
            .await
            .unwrap()
            .stream()
            .events()
            .len(),
        2
    );
    let history = store.version_history_page("history", 0, 2).await.unwrap();
    assert_eq!(history.entries().len(), 2);
    assert_eq!(history.entries()[1].version(), 1);
    assert_eq!(history.entries()[1].event_type(), "order.created");
}
