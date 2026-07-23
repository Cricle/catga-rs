//! Event-sourced aggregate contracts and snapshot-aware persistence.

use std::{marker::PhantomData, num::NonZeroUsize};

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, Snapshot, SnapshotStore};

/// An event-sourced aggregate with locally managed state and version.
pub trait Aggregate: Clone + Send + Sync + 'static {
    /// Creates an empty aggregate for a new identifier at stream version `-1`.
    fn new(id: &str) -> Self;

    /// Returns the stable event-stream identifier for an aggregate identifier.
    fn stream_id(id: &str) -> Box<str>;

    /// Returns the aggregate identifier.
    fn id(&self) -> &str;

    /// Returns the last applied zero-based stream version, or `-1` before any event.
    fn version(&self) -> i64;

    /// Applies one persisted event and advances the aggregate version by one.
    fn apply(&mut self, event: &Envelope) -> CatgaResult<()>;

    /// Returns events already applied locally but not yet appended to the stream.
    fn pending_events(&self) -> &[Envelope];

    /// Clears events after a successful append.
    fn clear_pending_events(&mut self);
}

/// Decides whether an aggregate version should be snapshotted.
pub trait SnapshotStrategy: Send + Sync {
    /// Returns whether a snapshot should replace the last snapshot at this version.
    fn should_snapshot(&self, current_version: i64, last_snapshot_version: i64) -> bool;
}

/// Takes a snapshot after a fixed number of newly applied events.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventCountSnapshotStrategy {
    interval: NonZeroUsize,
}

impl EventCountSnapshotStrategy {
    /// Creates a strategy that snapshots after `interval` events since the last snapshot.
    pub fn new(interval: usize) -> Option<Self> {
        NonZeroUsize::new(interval).map(|interval| Self { interval })
    }

    /// Returns the configured event interval.
    pub const fn interval(&self) -> NonZeroUsize {
        self.interval
    }
}

impl SnapshotStrategy for EventCountSnapshotStrategy {
    fn should_snapshot(&self, current_version: i64, last_snapshot_version: i64) -> bool {
        current_version.saturating_sub(last_snapshot_version)
            >= i64::try_from(self.interval.get()).unwrap_or(i64::MAX)
    }
}

/// Persists aggregates with optimistic versions and periodic immutable snapshots.
pub struct AggregateRepository<'a, A, E: ?Sized, S: ?Sized> {
    events: &'a E,
    snapshots: &'a S,
    strategy: EventCountSnapshotStrategy,
    aggregate: PhantomData<A>,
}

impl<'a, A, E: ?Sized, S: ?Sized> AggregateRepository<'a, A, E, S>
where
    A: Aggregate,
    E: EventStore,
    S: SnapshotStore,
{
    /// Creates a snapshot-aware aggregate repository.
    pub const fn new(
        events: &'a E,
        snapshots: &'a S,
        strategy: EventCountSnapshotStrategy,
    ) -> Self {
        Self {
            events,
            snapshots,
            strategy,
            aggregate: PhantomData,
        }
    }

    /// Loads an aggregate from its latest snapshot and later stream events.
    pub async fn load(&self, id: &str) -> CatgaResult<Option<A>> {
        let stream_id = A::stream_id(id);
        let snapshot = self.snapshots.load::<A>(&stream_id).await?;
        let has_snapshot = snapshot.is_some();
        let (mut aggregate, from_version) = match snapshot {
            Some(snapshot) => {
                let aggregate = (*snapshot.shared_state()).clone();
                if aggregate.version() != snapshot.version() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "aggregate snapshot version does not match its state",
                    ));
                }
                (aggregate, snapshot.version().saturating_add(1))
            }
            None => (A::new(id), 0),
        };
        let stream = self
            .events
            .read(
                &stream_id,
                u64::try_from(from_version).unwrap_or(0),
                usize::MAX,
            )
            .await?;
        if !has_snapshot && stream.events().is_empty() {
            return Ok(None);
        }
        for stored in stream.events() {
            aggregate.apply(stored.envelope())?;
            if aggregate.version() != stored.version() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "aggregate apply did not advance to the stored event version",
                ));
            }
        }
        Ok(Some(aggregate))
    }

    /// Appends pending events using the aggregate's original stream version.
    pub async fn save(&self, aggregate: &mut A) -> CatgaResult<()> {
        let pending = aggregate.pending_events();
        if pending.is_empty() {
            return Ok(());
        }
        let pending_count = i64::try_from(pending.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "aggregate pending event count exceeds the supported stream version range",
            )
        })?;
        let expected_version = aggregate
            .version()
            .checked_sub(pending_count)
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "aggregate version is lower than its pending event count",
                )
            })?;
        let stream_id = A::stream_id(aggregate.id());
        self.events
            .append(&stream_id, pending.to_vec(), Some(expected_version))
            .await?;
        aggregate.clear_pending_events();

        let last_snapshot_version = self
            .snapshots
            .load::<A>(&stream_id)
            .await?
            .map_or(-1, |snapshot| snapshot.version());
        if self
            .strategy
            .should_snapshot(aggregate.version(), last_snapshot_version)
        {
            self.snapshots
                .save(Snapshot::new(
                    stream_id,
                    aggregate.clone(),
                    aggregate.version(),
                ))
                .await?;
        }
        Ok(())
    }
}
