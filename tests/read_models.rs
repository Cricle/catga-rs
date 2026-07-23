//! Incremental read-model synchronization contract tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_core::{
    BatchSyncStrategy, CatgaError, ChangeKind, ChangeRecord, ChangeTracker, Envelope, ErrorCode,
    MessageMetadata, ReadModelStore, ReadModelSynchronizer, RealtimeSyncStrategy,
    ScheduledSyncStrategy, SyncStrategy,
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

#[tokio::test]
async fn batch_strategy_preserves_change_order_in_bounded_owned_batches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicUsize::new(0));
    let strategy = BatchSyncStrategy::new(2, {
        let calls = Arc::clone(&calls);
        let delivered = Arc::clone(&delivered);
        move |batch: Vec<ChangeRecord>| {
            let calls = Arc::clone(&calls);
            let delivered = Arc::clone(&delivered);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                delivered.fetch_add(batch.len(), Ordering::Relaxed);
                Ok::<(), CatgaError>(())
            }
        }
    })
    .unwrap();
    let changes = (0..5)
        .map(|id| {
            ChangeRecord::new(
                id.to_string(),
                "order",
                "7",
                ChangeKind::Updated,
                Envelope::new(id, "order.updated", vec![], MessageMetadata::new(id, None)),
            )
        })
        .collect::<Vec<_>>();

    strategy.execute(&changes).await.unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 3);
    assert_eq!(delivered.load(Ordering::Relaxed), 5);
    assert!(BatchSyncStrategy::<fn(Vec<ChangeRecord>) -> std::future::Ready<catga_core::CatgaResult<()>>>::new(0, |_| std::future::ready(Ok(()))).is_none());
}

#[tokio::test]
async fn scheduled_strategy_runs_once_per_interval_without_a_lock() {
    let calls = Arc::new(AtomicUsize::new(0));
    let strategy = ScheduledSyncStrategy::new(Duration::from_secs(60), {
        let calls = Arc::clone(&calls);
        move |_| {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok::<(), CatgaError>(())
            }
        }
    });
    let changes = vec![ChangeRecord::new(
        "one",
        "order",
        "7",
        ChangeKind::Updated,
        Envelope::new(1, "order.updated", vec![], MessageMetadata::new(1, None)),
    )];

    strategy.execute(&changes).await.unwrap();
    strategy.execute(&changes).await.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
