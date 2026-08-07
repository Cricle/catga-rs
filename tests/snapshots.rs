//! Snapshot-store contract tests.

use catga_core::memory::MemorySnapshots;
use catga_core::{ErrorCode, Snapshot, SnapshotStore};

#[derive(Debug, Eq, PartialEq)]
struct OrderState {
    total: u64,
}

#[tokio::test]
async fn snapshots_are_immutable_typed_and_monotonically_versioned_per_stream() {
    let store = MemorySnapshots::default();
    store
        .save(Snapshot::new("order-1", OrderState { total: 10 }, 4))
        .await
        .unwrap();

    let snapshot = store.load::<OrderState>("order-1").await.unwrap().unwrap();
    assert_eq!(snapshot.stream_id(), "order-1");
    assert_eq!(snapshot.version(), 4);
    assert_eq!(snapshot.state().total, 10);

    let stale = store
        .save(Snapshot::new("order-1", OrderState { total: 7 }, 3))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), ErrorCode::Conflict);
    assert_eq!(
        store
            .load::<OrderState>("order-1")
            .await
            .unwrap()
            .unwrap()
            .state()
            .total,
        10
    );

    let mismatch = store.load::<u64>("order-1").await.unwrap_err();
    assert_eq!(mismatch.code(), ErrorCode::Validation);
    store.delete("order-1").await.unwrap();
    assert!(store.load::<OrderState>("order-1").await.unwrap().is_none());
}
