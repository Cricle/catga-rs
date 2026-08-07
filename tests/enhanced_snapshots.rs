//! Multi-version snapshot contract tests.

use std::{sync::Arc, time::SystemTime};

use catga_core::memory::MemoryEnhancedSnapshots;
use catga_core::{EnhancedSnapshotStore, Snapshot, SnapshotStore};

#[tokio::test]
async fn enhanced_snapshots_find_version_history_and_cleanup_without_mutating_readers() {
    let snapshots = MemoryEnhancedSnapshots::default();
    let at_one = Snapshot::from_shared("account-7", Arc::new(10_u64), 1, SystemTime::now());
    let at_three = Snapshot::from_shared("account-7", Arc::new(30_u64), 3, SystemTime::now());
    let at_five = Snapshot::from_shared("account-7", Arc::new(50_u64), 5, SystemTime::now());

    snapshots.save(at_one).await.unwrap();
    snapshots.save(at_three).await.unwrap();
    snapshots.save(at_five).await.unwrap();

    let before_cleanup = snapshots
        .load_at_version::<u64>("account-7", 4)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_cleanup.version(), 3);
    assert_eq!(*before_cleanup.state(), 30);
    assert_eq!(
        snapshots
            .history("account-7")
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.version())
            .collect::<Vec<_>>(),
        [1, 3, 5]
    );

    snapshots
        .delete_before_version("account-7", 3)
        .await
        .unwrap();
    snapshots.cleanup("account-7", 1).await.unwrap();

    assert!(
        snapshots
            .load_at_version::<u64>("account-7", 2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        snapshots
            .history("account-7")
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.version())
            .collect::<Vec<_>>(),
        [5]
    );
    assert_eq!(*before_cleanup.state(), 30);
}
