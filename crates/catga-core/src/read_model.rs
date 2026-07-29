//! Incremental read-model change tracking and synchronization contracts.

use std::{
    future::Future,
    num::NonZeroUsize,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, Envelope};

/// Largest number of pending changes one read-model synchronization pass may retain at once.
///
/// Callers with a larger backlog invoke [`ReadModelSynchronizer::sync`] repeatedly. This keeps
/// the synchronizer's change records and acknowledgement identifiers bounded independently of
/// how many changes a tracker has retained.
pub const MAX_READ_MODEL_PAGE_SIZE: usize = 1_024;

const DEFAULT_READ_MODEL_PAGE_SIZE: usize = 256;

/// Validates a requested pending-change page size.
///
/// Trackers must call this before allocating a pending-change page so all implementations share
/// the same bound.
///
/// ```
/// use catga_core::{validate_read_model_page_size, MAX_READ_MODEL_PAGE_SIZE};
///
/// assert!(validate_read_model_page_size(256).is_ok());
/// assert!(validate_read_model_page_size(0).is_err());
/// assert!(validate_read_model_page_size(MAX_READ_MODEL_PAGE_SIZE + 1).is_err());
/// ```
pub fn validate_read_model_page_size(max_count: usize) -> CatgaResult<()> {
    if max_count == 0 || max_count > MAX_READ_MODEL_PAGE_SIZE {
        return Err(CatgaError::new(
            crate::ErrorCode::Validation,
            "read-model page size must be between 1 and MAX_READ_MODEL_PAGE_SIZE",
        ));
    }
    Ok(())
}

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

    /// Returns at most `max_count` unsynchronized changes from a point-in-time view.
    ///
    /// Implementations must validate `max_count` with [`validate_read_model_page_size`] before
    /// allocating the page. The returned vector must contain no more than `max_count` records;
    /// callers invoke this method again after acknowledging a successful page to drain a larger
    /// backlog without materializing it all at once.
    async fn pending_page(&self, max_count: usize) -> CatgaResult<Vec<ChangeRecord>>;

    /// Marks only the supplied change identifiers as synchronized.
    async fn mark_synced(&self, change_ids: &[Box<str>]) -> CatgaResult<()>;
}

/// Applies a batch of pending changes to one external read model.
#[async_trait]
pub trait SyncStrategy: Send + Sync {
    /// Applies every supplied change or returns the first failure or deferral.
    ///
    /// A strategy that intentionally defers work must return a transient error
    /// rather than `Ok(())`, so [`ReadModelSynchronizer`] retains the supplied
    /// changes instead of acknowledging work that was not applied.
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
///
/// Calls made while another invocation is active or before the interval
/// elapses return [`crate::ErrorCode::Transient`]. This keeps pending changes
/// durable until an invocation actually runs and succeeds.
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
                return Err(CatgaError::new(
                    crate::ErrorCode::Transient,
                    "read-model synchronization is already running",
                ));
            }
            let last = state & TIMESTAMP;
            let now = now_millis();
            if last != 0 && now.saturating_sub(last) < self.interval_millis {
                return Err(CatgaError::new(
                    crate::ErrorCode::Transient,
                    "read-model synchronization is deferred until its scheduled interval",
                ));
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
    page_size: NonZeroUsize,
    last_sync_millis: AtomicU64,
}

impl<'a, T: ?Sized, S: ?Sized> ReadModelSynchronizer<'a, T, S>
where
    T: ChangeTracker,
    S: SyncStrategy,
{
    /// Creates a synchronizer with no completed runs.
    ///
    /// Each invocation of [`Self::sync`] processes at most 256 pending changes. Use
    /// [`Self::with_page_size`] when a different bounded page size is required.
    pub fn new(tracker: &'a T, strategy: &'a S) -> Self {
        Self::with_page_size(
            tracker,
            strategy,
            NonZeroUsize::new(DEFAULT_READ_MODEL_PAGE_SIZE).unwrap_or(NonZeroUsize::MIN),
        )
    }

    /// Creates a synchronizer with an explicit bounded pending-change page size.
    ///
    /// Values above [`MAX_READ_MODEL_PAGE_SIZE`] are capped to the shared limit. One call to
    /// [`Self::sync`] obtains, executes, and acknowledges no more than this many changes; call
    /// it repeatedly to drain a larger backlog.
    pub fn with_page_size(tracker: &'a T, strategy: &'a S, page_size: NonZeroUsize) -> Self {
        Self {
            tracker,
            strategy,
            page_size: NonZeroUsize::new(page_size.get().min(MAX_READ_MODEL_PAGE_SIZE))
                .unwrap_or(NonZeroUsize::MIN),
            last_sync_millis: AtomicU64::new(0),
        }
    }

    /// Returns the maximum number of changes one [`Self::sync`] call can retain.
    pub const fn page_size(&self) -> NonZeroUsize {
        self.page_size
    }

    /// Applies one bounded pending-change page and acknowledges it only after strategy success.
    ///
    /// A successful invocation leaves later pending changes untouched. This lets callers drain
    /// arbitrarily large backlogs through repeated calls while retaining only one page and its
    /// identifier list in memory. A strategy error leaves the complete page pending.
    pub async fn sync(&self) -> CatgaResult<()> {
        let pending = self.tracker.pending_page(self.page_size.get()).await?;
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
