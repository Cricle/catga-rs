//! Strict contract tests for the process-local snapshot stores: the
//! single-latest snapshot slot with optimistic version fencing and the
//! multi-version historical store with retention cleanup.

use catga_core::memory::{MemoryEnhancedSnapshots, MemorySnapshots};
use catga_core::{EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotStore};

// ---------------------------------------------------------------------------
// MemorySnapshots
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshots_keep_the_latest_version_per_stream() {
    let store = MemorySnapshots::default();

    assert!(
        store
            .load::<String>("s")
            .await
            .expect("load succeeds")
            .is_none()
    );

    store
        .save(Snapshot::new("s", "alpha".to_string(), 3))
        .await
        .expect("save succeeds");
    let snapshot = store
        .load::<String>("s")
        .await
        .expect("load succeeds")
        .expect("snapshot retained");
    assert_eq!(snapshot.stream_id(), "s");
    assert_eq!(snapshot.state(), "alpha");
    assert_eq!(snapshot.version(), 3);

    // A newer version replaces the slot; an older one conflicts.
    store
        .save(Snapshot::new("s", "beta".to_string(), 4))
        .await
        .expect("a newer version saves");
    let error = store
        .save(Snapshot::new("s", "stale".to_string(), 3))
        .await
        .expect_err("an older version conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);
    let snapshot = store
        .load::<String>("s")
        .await
        .expect("load succeeds")
        .expect("snapshot retained");
    assert_eq!(snapshot.state(), "beta");

    // A mismatched state type is a decode-time validation failure.
    let error = store
        .load::<u64>("s")
        .await
        .expect_err("a mismatched state type must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Deleting removes only the named stream.
    store
        .save(Snapshot::new("other", "gamma".to_string(), 0))
        .await
        .expect("save succeeds");
    store.delete("s").await.expect("delete succeeds");
    assert!(
        store
            .load::<String>("s")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .load::<String>("other")
            .await
            .expect("load succeeds")
            .is_some()
    );
    store.delete("s").await.expect("repeated delete succeeds");
}

// ---------------------------------------------------------------------------
// MemoryEnhancedSnapshots
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enhanced_snapshots_retain_a_versioned_history() {
    let store = MemoryEnhancedSnapshots::default();

    // Missing streams read as empty across every operation.
    assert!(
        store
            .load::<String>("s")
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .load_at_version::<String>("s", 3)
            .await
            .expect("load succeeds")
            .is_none()
    );
    assert!(
        store
            .history("s")
            .await
            .expect("history succeeds")
            .is_empty()
    );
    store
        .delete_before_version("s", 3)
        .await
        .expect("delete succeeds");
    store.cleanup("s", 1).await.expect("cleanup succeeds");

    for version in 0..3 {
        store
            .save(Snapshot::new("s", format!("state-{version}"), version))
            .await
            .expect("save succeeds");
    }

    // The latest snapshot answers the plain load.
    let latest = store
        .load::<String>("s")
        .await
        .expect("load succeeds")
        .expect("snapshot retained");
    assert_eq!(latest.state(), "state-2");

    // Historical loads floor to the newest retained version at or below the
    // bound; a bound before the first snapshot reads as empty.
    let at = store
        .load_at_version::<String>("s", 1)
        .await
        .expect("load succeeds")
        .expect("snapshot retained");
    assert_eq!(at.state(), "state-1");
    let at = store
        .load_at_version::<String>("s", 10)
        .await
        .expect("load succeeds")
        .expect("snapshot retained");
    assert_eq!(at.version(), 2);
    assert!(
        store
            .load_at_version::<String>("s", -1)
            .await
            .expect("load succeeds")
            .is_none()
    );

    // History reports ascending versions; stale saves conflict.
    let history = store.history("s").await.expect("history succeeds");
    let versions: Vec<i64> = history.iter().map(|info| info.version()).collect();
    assert_eq!(versions, [0, 1, 2]);
    let error = store
        .save(Snapshot::new("s", "stale".to_string(), 1))
        .await
        .expect_err("an older version conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);
    let error = store
        .load_at_version::<u64>("s", 2)
        .await
        .expect_err("a mismatched state type must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Version-bounded deletion keeps the inclusive boundary.
    store
        .delete_before_version("s", 1)
        .await
        .expect("delete succeeds");
    let history = store.history("s").await.expect("history succeeds");
    let versions: Vec<i64> = history.iter().map(|info| info.version()).collect();
    assert_eq!(versions, [1, 2]);

    // Cleanup retains only the newest entries.
    store
        .save(Snapshot::new("s", "state-3".to_string(), 3))
        .await
        .expect("save succeeds");
    store.cleanup("s", 1).await.expect("cleanup succeeds");
    let history = store.history("s").await.expect("history succeeds");
    let versions: Vec<i64> = history.iter().map(|info| info.version()).collect();
    assert_eq!(versions, [3]);
    store.cleanup("s", 5).await.expect("cleanup succeeds");
    assert_eq!(store.history("s").await.expect("history succeeds").len(), 1);

    // Deleting the stream drops its whole history.
    store.delete("s").await.expect("delete succeeds");
    assert!(
        store
            .history("s")
            .await
            .expect("history succeeds")
            .is_empty()
    );
}
