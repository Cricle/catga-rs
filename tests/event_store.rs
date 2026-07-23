//! Event store contract tests.

use std::sync::Arc;

use catga_core::{Envelope, EventStore, MessageMetadata};
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

    let stream = store.read("order-1", 1, 1).await.unwrap();
    assert_eq!(stream.version(), 1);
    assert_eq!(stream.events().len(), 1);
    assert_eq!(stream.events()[0].version(), 1);
    assert_eq!(stream.events()[0].envelope().id(), 2);
    assert_eq!(store.stream_ids().await.unwrap(), vec!["order-1"]);
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
            .read("concurrent", 0, 64)
            .await
            .unwrap()
            .events()
            .len(),
        64
    );
}
