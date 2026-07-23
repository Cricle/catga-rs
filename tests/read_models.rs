//! Incremental read-model synchronization contract tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{
    CatgaError, ChangeKind, ChangeRecord, ChangeTracker, Envelope, ErrorCode, MessageMetadata,
    ReadModelStore, ReadModelSynchronizer, RealtimeSyncStrategy,
};
use catga_memory::{MemoryChangeTracker, MemoryReadModels};

#[tokio::test]
async fn synchronizer_only_marks_changes_after_the_strategy_succeeds() {
    let tracker = MemoryChangeTracker::default();
    tracker.track(ChangeRecord::new(
        "change-1",
        "order",
        "7",
        ChangeKind::Updated,
        Envelope::new(1, "order.updated", vec![7], MessageMetadata::new(1, None)),
    ));
    let deliveries = Arc::new(AtomicUsize::new(0));
    let strategy = RealtimeSyncStrategy::new({
        let deliveries = Arc::clone(&deliveries);
        move |_: &ChangeRecord| {
            deliveries.fetch_add(1, Ordering::Relaxed);
            async { Ok::<(), CatgaError>(()) }
        }
    });
    let synchronizer = ReadModelSynchronizer::new(&tracker, &strategy);

    synchronizer.sync().await.unwrap();
    synchronizer.sync().await.unwrap();

    assert_eq!(deliveries.load(Ordering::Relaxed), 1);
    assert!(tracker.pending().await.unwrap().is_empty());
    assert!(synchronizer.last_sync_time().is_some());

    let failing = RealtimeSyncStrategy::new(|_: &ChangeRecord| async {
        Err(CatgaError::new(ErrorCode::Transient, "temporary outage"))
    });
    tracker.track(ChangeRecord::new(
        "change-2",
        "order",
        "7",
        ChangeKind::Deleted,
        Envelope::new(2, "order.deleted", vec![], MessageMetadata::new(2, None)),
    ));
    ReadModelSynchronizer::new(&tracker, &failing)
        .sync()
        .await
        .unwrap_err();
    assert_eq!(tracker.pending().await.unwrap().len(), 1);
}

#[tokio::test]
async fn memory_read_models_share_immutable_values() {
    let models = MemoryReadModels::<String>::default();
    let model = Arc::new(String::from("ready"));

    models.save("order-7", Arc::clone(&model)).await.unwrap();
    let loaded = models.get("order-7").await.unwrap().unwrap();

    assert_eq!(loaded.as_str(), "ready");
    assert!(Arc::ptr_eq(&model, &loaded));
    models.delete("order-7").await.unwrap();
    assert!(models.get("order-7").await.unwrap().is_none());
}
