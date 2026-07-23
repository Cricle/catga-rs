//! Incremental read-model change tracking and synchronization contracts.

use std::{
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::{CatgaResult, Envelope};

/// The lifecycle operation represented by one read-model change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeKind {
    /// A new entity was created.
    Created,
    /// An existing entity was updated.
    Updated,
    /// An existing entity was deleted.
    Deleted,
}

/// One immutable change awaiting delivery to a read model.
#[derive(Clone, Debug)]
pub struct ChangeRecord {
    id: Box<str>,
    entity_type: Box<str>,
    entity_id: Box<str>,
    kind: ChangeKind,
    event: Arc<Envelope>,
    timestamp: SystemTime,
}

impl ChangeRecord {
    /// Creates a pending change from one serialized event.
    pub fn new(
        id: impl Into<Box<str>>,
        entity_type: impl Into<Box<str>>,
        entity_id: impl Into<Box<str>>,
        kind: ChangeKind,
        event: Envelope,
    ) -> Self {
        Self {
            id: id.into(),
            entity_type: entity_type.into(),
            entity_id: entity_id.into(),
            kind,
            event: Arc::new(event),
            timestamp: SystemTime::now(),
        }
    }

    /// Returns the idempotency identifier for this change.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Returns the changed entity type.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }
    /// Returns the changed entity identifier.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }
    /// Returns the lifecycle operation.
    pub const fn kind(&self) -> ChangeKind {
        self.kind
    }
    /// Returns the immutable event without cloning its payload.
    pub const fn event(&self) -> &Arc<Envelope> {
        &self.event
    }
    /// Returns when tracking occurred.
    pub const fn timestamp(&self) -> SystemTime {
        self.timestamp
    }
}

/// Tracks changes until a synchronization strategy has completed successfully.
#[async_trait]
pub trait ChangeTracker: Send + Sync {
    /// Adds or replaces a pending change by its stable identifier.
    fn track(&self, change: ChangeRecord);
    /// Returns a point-in-time view of unsynchronized changes.
    async fn pending(&self) -> CatgaResult<Vec<ChangeRecord>>;
    /// Marks only the supplied change identifiers as synchronized.
    async fn mark_synced(&self, change_ids: &[Box<str>]) -> CatgaResult<()>;
}

/// Applies a batch of pending changes to one external read model.
#[async_trait]
pub trait SyncStrategy: Send + Sync {
    /// Applies every supplied change or returns the first failure.
    async fn execute(&self, changes: &[ChangeRecord]) -> CatgaResult<()>;
}

/// Runs a user-supplied asynchronous action once for each change.
pub struct RealtimeSyncStrategy<F> {
    action: F,
}

impl<F> RealtimeSyncStrategy<F> {
    /// Creates a per-change synchronization strategy.
    pub const fn new(action: F) -> Self {
        Self { action }
    }
}

#[async_trait]
impl<F, Fut> SyncStrategy for RealtimeSyncStrategy<F>
where
    F: Fn(&ChangeRecord) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    async fn execute(&self, changes: &[ChangeRecord]) -> CatgaResult<()> {
        for change in changes {
            (self.action)(change).await?;
        }
        Ok(())
    }
}

/// Runs a user-supplied asynchronous action for fixed-size ordered change batches.
pub struct BatchSyncStrategy<F> {
    batch_size: NonZeroUsize,
    action: F,
}

/// Runs a full ordered change set no more often than one configured interval.
pub struct ScheduledSyncStrategy<F> {
    interval_millis: u64,
    action: F,
    state: AtomicU64,
}

impl<F> ScheduledSyncStrategy<F> {
    /// Creates a scheduled strategy. A zero interval permits every completed invocation.
    pub fn new(interval: std::time::Duration, action: F) -> Self {
        Self {
            interval_millis: u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
            action,
            state: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl<F, Fut> SyncStrategy for ScheduledSyncStrategy<F>
where
    F: Fn(Vec<ChangeRecord>) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    async fn execute(&self, changes: &[ChangeRecord]) -> CatgaResult<()> {
        const BUSY: u64 = 1 << 63;
        const TIMESTAMP: u64 = !BUSY;
        if changes.is_empty() {
            return Ok(());
        }
        loop {
            let state = self.state.load(Ordering::Acquire);
            if state & BUSY != 0 {
                return Ok(());
            }
            let last = state & TIMESTAMP;
            let now = now_millis();
            if last != 0 && now.saturating_sub(last) < self.interval_millis {
                return Ok(());
            }
            if self
                .state
                .compare_exchange(state, state | BUSY, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
            let result = (self.action)(changes.to_vec()).await;
            self.state
                .store(if result.is_ok() { now } else { last }, Ordering::Release);
            return result;
        }
    }
}

impl<F> BatchSyncStrategy<F> {
    /// Creates a batch strategy, or returns `None` for a zero batch size.
    pub fn new(batch_size: usize, action: F) -> Option<Self> {
        NonZeroUsize::new(batch_size).map(|batch_size| Self { batch_size, action })
    }

    /// Returns the maximum number of changes given to one action invocation.
    pub const fn batch_size(&self) -> NonZeroUsize {
        self.batch_size
    }
}

#[async_trait]
impl<F, Fut> SyncStrategy for BatchSyncStrategy<F>
where
    F: Fn(Vec<ChangeRecord>) -> Fut + Send + Sync,
    Fut: Future<Output = CatgaResult<()>> + Send,
{
    async fn execute(&self, changes: &[ChangeRecord]) -> CatgaResult<()> {
        for batch in changes.chunks(self.batch_size.get()) {
            (self.action)(batch.to_vec()).await?;
        }
        Ok(())
    }
}

/// Owns synchronization ordering for a tracker and one strategy.
pub struct ReadModelSynchronizer<'a, T: ?Sized, S: ?Sized> {
    tracker: &'a T,
    strategy: &'a S,
    last_sync_millis: AtomicU64,
}

impl<'a, T: ?Sized, S: ?Sized> ReadModelSynchronizer<'a, T, S>
where
    T: ChangeTracker,
    S: SyncStrategy,
{
    /// Creates a synchronizer with no completed runs.
    pub const fn new(tracker: &'a T, strategy: &'a S) -> Self {
        Self {
            tracker,
            strategy,
            last_sync_millis: AtomicU64::new(0),
        }
    }
    /// Applies all pending changes and acknowledges them only after strategy success.
    pub async fn sync(&self) -> CatgaResult<()> {
        let pending = self.tracker.pending().await?;
        if pending.is_empty() {
            return Ok(());
        }
        self.strategy.execute(&pending).await?;
        let ids = pending
            .iter()
            .map(|change| change.id().into())
            .collect::<Vec<Box<str>>>();
        self.tracker.mark_synced(&ids).await?;
        self.last_sync_millis.store(now_millis(), Ordering::Release);
        Ok(())
    }
    /// Returns the latest successful synchronization time.
    pub fn last_sync_time(&self) -> Option<SystemTime> {
        let millis = self.last_sync_millis.load(Ordering::Acquire);
        (millis != 0).then(|| UNIX_EPOCH + std::time::Duration::from_millis(millis))
    }
}

/// Stores immutable read models by stable identifier.
#[async_trait]
pub trait ReadModelStore<M: Send + Sync + 'static>: Send + Sync {
    /// Returns shared read-model state without copying it.
    async fn get(&self, id: &str) -> CatgaResult<Option<Arc<M>>>;
    /// Replaces the state for one identifier.
    async fn save(&self, id: &str, model: Arc<M>) -> CatgaResult<()>;
    /// Removes one read model.
    async fn delete(&self, id: &str) -> CatgaResult<()>;
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
