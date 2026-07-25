//! Strategy-driven aggregate snapshot persistence.

use std::{sync::Arc, time::SystemTime};

use crate::{CatgaResult, EnhancedSnapshotStore, Snapshot, SnapshotStrategy};

/// Saves immutable aggregate snapshots only when its strategy reports they are due.
///
/// The manager has no mutable state and therefore can be freely shared across
/// tasks. Snapshot stores retain the concurrency responsibility for one
/// stream, while callers can avoid cloning large aggregate states with
/// [`Self::check_and_save_shared`].
pub struct AutoSnapshotManager<'a, S: ?Sized, P: ?Sized> {
    snapshots: &'a S,
    strategy: &'a P,
}

impl<'a, S: ?Sized, P: ?Sized> AutoSnapshotManager<'a, S, P>
where
    S: EnhancedSnapshotStore,
    P: SnapshotStrategy,
{
    /// Creates a manager using an enhanced snapshot store and one strategy.
    pub const fn new(snapshots: &'a S, strategy: &'a P) -> Self {
        Self {
            snapshots,
            strategy,
        }
    }

    /// Saves `state` when the strategy considers `current_version` due.
    ///
    /// Moving the state lets callers save a snapshot without an additional
    /// aggregate clone. Returns whether a snapshot was written.
    pub async fn check_and_save<A>(
        &self,
        stream_id: &str,
        state: A,
        current_version: i64,
    ) -> CatgaResult<bool>
    where
        A: Send + Sync + 'static,
    {
        self.check_and_save_shared(stream_id, Arc::new(state), current_version)
            .await
    }

    /// Saves shared `state` when the strategy considers `current_version` due.
    ///
    /// Reusing an existing [`Arc`] avoids copying a large immutable aggregate
    /// solely for snapshot persistence. Returns whether a snapshot was
    /// written.
    pub async fn check_and_save_shared<A>(
        &self,
        stream_id: &str,
        state: Arc<A>,
        current_version: i64,
    ) -> CatgaResult<bool>
    where
        A: Send + Sync + 'static,
    {
        let last_snapshot_version = self
            .snapshots
            .load::<A>(stream_id)
            .await?
            .map_or(-1, |snapshot| snapshot.version());
        if !self
            .strategy
            .should_snapshot(current_version, last_snapshot_version)
        {
            return Ok(false);
        }
        self.snapshots
            .save(Snapshot::from_shared(
                stream_id,
                state,
                current_version,
                SystemTime::now(),
            ))
            .await?;
        Ok(true)
    }
}
