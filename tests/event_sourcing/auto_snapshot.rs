//! Automatic snapshot manager tests.

use std::sync::Arc;

use catga_core::memory::MemoryEnhancedSnapshots;
use catga_core::{AutoSnapshotManager, EventCountSnapshotStrategy, SnapshotStore};

#[tokio::test]
async fn automatic_snapshotting_reuses_the_latest_version_without_copying_shared_state() {
    let store = MemoryEnhancedSnapshots::default();
    let strategy = EventCountSnapshotStrategy::new(2).expect("nonzero interval");
    let manager = AutoSnapshotManager::new(&store, &strategy);

    assert!(
        !manager
            .check_and_save_shared("orders:7", Arc::new(10_u64), 0)
            .await
            .expect("check succeeds")
    );
    assert!(
        manager
            .check_and_save_shared("orders:7", Arc::new(20_u64), 1)
            .await
            .expect("snapshot is due")
    );
    assert!(
        !manager
            .check_and_save_shared("orders:7", Arc::new(30_u64), 2)
            .await
            .expect("snapshot is not yet due")
    );

    let snapshot = store
        .load::<u64>("orders:7")
        .await
        .expect("load succeeds")
        .expect("one snapshot is retained");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(*snapshot.state(), 20);
}
