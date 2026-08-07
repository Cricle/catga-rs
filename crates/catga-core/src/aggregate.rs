//! Event-sourced aggregate contracts and snapshot-aware persistence.

use std::{
    marker::PhantomData,
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};

use crate::{
    CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, MAX_EVENT_STORE_PAGE_SIZE, Snapshot,
    SnapshotStore,
};

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
///
/// ```
/// use catga_core::{EventCountSnapshotStrategy, SnapshotStrategy};
///
/// let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
/// assert_eq!(strategy.interval().get(), 10);
/// assert!(!strategy.should_snapshot(5, 0));
/// assert!(strategy.should_snapshot(10, 0));
/// assert!(strategy.should_snapshot(15, 5));
/// ```
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

/// Decides whether a snapshot is due after a fixed elapsed interval.
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use catga_core::TimeBasedSnapshotStrategy;
///
/// let strategy = TimeBasedSnapshotStrategy::new(Duration::from_secs(60));
/// let base = SystemTime::UNIX_EPOCH;
/// assert!(!strategy.should_snapshot(base, base + Duration::from_secs(30)));
/// assert!(strategy.should_snapshot(base, base + Duration::from_secs(60)));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeBasedSnapshotStrategy {
    interval: Duration,
}

impl TimeBasedSnapshotStrategy {
    /// Creates a time-based snapshot strategy. A zero interval snapshots immediately.
    pub const fn new(interval: Duration) -> Self {
        Self { interval }
    }
    /// Returns the configured minimum interval between snapshots.
    pub const fn interval(&self) -> Duration {
        self.interval
    }
    /// Returns whether `now` is at least one interval after the previous snapshot.
    pub fn should_snapshot(&self, last_snapshot: SystemTime, now: SystemTime) -> bool {
        now.duration_since(last_snapshot)
            .is_ok_and(|elapsed| elapsed >= self.interval)
    }
}

/// Combines event-count and elapsed-time snapshot decisions.
///
/// ```
/// use std::time::{Duration, SystemTime};
/// use catga_core::{CompositeSnapshotStrategy, EventCountSnapshotStrategy, TimeBasedSnapshotStrategy};
///
/// let events = EventCountSnapshotStrategy::new(100).expect("nonzero");
/// let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(30));
/// let composite = CompositeSnapshotStrategy::new(events, time);
///
/// let base = SystemTime::UNIX_EPOCH;
/// // Neither threshold met.
/// assert!(!composite.should_snapshot(5, 0, base, base + Duration::from_secs(10)));
/// // Time threshold met.
/// assert!(composite.should_snapshot(5, 0, base, base + Duration::from_secs(30)));
/// // Event threshold met.
/// assert!(composite.should_snapshot(100, 0, base, base));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompositeSnapshotStrategy {
    events: EventCountSnapshotStrategy,
    time: TimeBasedSnapshotStrategy,
}

impl CompositeSnapshotStrategy {
    /// Creates a strategy that snapshots when either input strategy triggers.
    pub const fn new(events: EventCountSnapshotStrategy, time: TimeBasedSnapshotStrategy) -> Self {
        Self { events, time }
    }
    /// Returns whether either the version or elapsed-time threshold has been met.
    pub fn should_snapshot(
        &self,
        current_version: i64,
        last_version: i64,
        last_snapshot: SystemTime,
        now: SystemTime,
    ) -> bool {
        self.events.should_snapshot(current_version, last_version)
            || self.time.should_snapshot(last_snapshot, now)
    }
}

/// Persists aggregates with optimistic versions and periodic immutable snapshots.
pub struct AggregateRepository<'a, A, E: ?Sized, S: ?Sized> {
    events: &'a E,
    snapshots: &'a S,
    strategy: EventCountSnapshotStrategy,
    aggregate: PhantomData<A>,
}

/// Returns the cursor immediately after a persisted event version.
///
/// `i64::MAX` is terminal in the signed event-version domain, so it has no
/// following cursor.
pub(crate) fn next_event_version(version: i64) -> Option<u64> {
    (version != i64::MAX).then(|| u64::try_from(version + 1).unwrap_or(0))
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
        let (mut aggregate, next_version) = match snapshot {
            Some(snapshot) => {
                let aggregate = (*snapshot.shared_state()).clone();
                if aggregate.version() != snapshot.version() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "aggregate snapshot version does not match its state",
                    ));
                }
                (aggregate, next_event_version(snapshot.version()))
            }
            None => (A::new(id), Some(0)),
        };
        let Some(mut next_version) = next_version else {
            return Ok(Some(aggregate));
        };
        let mut found_event = false;
        loop {
            let page = self
                .events
                .read_page(&stream_id, next_version, MAX_EVENT_STORE_PAGE_SIZE)
                .await?;
            for stored in page.stream().events() {
                found_event = true;
                aggregate.apply(stored.envelope())?;
                if aggregate.version() != stored.version() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "aggregate apply did not advance to the stored event version",
                    ));
                }
            }
            let Some(next) = page.next_version() else {
                break;
            };
            next_version = next;
        }
        if !has_snapshot && !found_event {
            return Ok(None);
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use crate::{
        Envelope, ErrorCode, EventPage, EventStore, EventStream, MessageMetadata, QualityOfService,
        Snapshot, SnapshotStore, StoredEvent, StreamIdsPage, VersionHistoryPage, VersionInfo,
    };

    use super::*;

    // Test implementations of the traits
    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestAggregate {
        id: String,
        version: i64,
        pending: Vec<Envelope>,
        state: u64,
    }

    impl TestAggregate {
        fn new_with_id(id: &str) -> Self {
            Self {
                id: id.to_string(),
                version: -1,
                pending: Vec::new(),
                state: 0,
            }
        }
    }

    impl Aggregate for TestAggregate {
        fn new(id: &str) -> Self {
            Self::new_with_id(id)
        }

        fn stream_id(id: &str) -> Box<str> {
            format!("test-{}", id).into_boxed_str()
        }

        fn id(&self) -> &str {
            &self.id
        }

        fn version(&self) -> i64 {
            self.version
        }

        fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
            self.version += 1;
            self.state += event.metadata().message_id();
            Ok(())
        }

        fn pending_events(&self) -> &[Envelope] {
            &self.pending
        }

        fn clear_pending_events(&mut self) {
            self.pending.clear();
        }
    }

    // Mock EventStore for testing
    #[allow(clippy::type_complexity)]
    struct MockEventStore {
        events: Vec<StoredEvent>,
        append_calls: Arc<std::sync::Mutex<Vec<(String, usize, Option<i64>)>>>,
        read_page_calls: Arc<std::sync::Mutex<Vec<(String, u64)>>>,
    }

    impl MockEventStore {
        fn new(events: Vec<StoredEvent>) -> Self {
            Self {
                events,
                append_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                read_page_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append(
            &self,
            stream_id: &str,
            _events: Vec<Envelope>,
            expected_version: Option<i64>,
        ) -> CatgaResult<i64> {
            self.append_calls.lock().expect("mutex poisoned").push((
                stream_id.to_string(),
                _events.len(),
                expected_version,
            ));
            let new_version = expected_version.unwrap_or(-1) + _events.len() as i64;
            Ok(new_version)
        }

        async fn read_page(
            &self,
            stream_id: &str,
            version: u64,
            _max_count: usize,
        ) -> CatgaResult<EventPage> {
            self.read_page_calls
                .lock()
                .expect("mutex poisoned")
                .push((stream_id.to_string(), version));

            let filtered: Vec<StoredEvent> = self
                .events
                .iter()
                .filter(|e| e.version() >= version as i64)
                .cloned()
                .collect();

            if filtered.is_empty() {
                let stream =
                    EventStream::new(stream_id.to_string(), version as i64 - 1, Vec::new());
                return Ok(EventPage::new(stream, None));
            }

            let max_version = filtered
                .iter()
                .map(|e| e.version())
                .max()
                .unwrap_or(version as i64);
            let stream = EventStream::new(stream_id.to_string(), max_version, filtered);
            Ok(EventPage::new(stream, Some(version + 1)))
        }

        async fn version(&self, _stream_id: &str) -> CatgaResult<i64> {
            Ok(self.events.iter().map(|e| e.version()).max().unwrap_or(-1))
        }

        async fn read_to_version_page(
            &self,
            stream_id: &str,
            from_version: u64,
            _to_version: i64,
            max_count: usize,
        ) -> CatgaResult<EventPage> {
            self.read_page(stream_id, from_version, max_count).await
        }

        async fn read_to_time_page(
            &self,
            stream_id: &str,
            from_version: u64,
            _upper_bound: SystemTime,
            max_count: usize,
        ) -> CatgaResult<EventPage> {
            self.read_page(stream_id, from_version, max_count).await
        }

        async fn version_history_page(
            &self,
            _stream_id: &str,
            from_version: u64,
            max_count: usize,
        ) -> CatgaResult<VersionHistoryPage> {
            let filtered: Vec<VersionInfo> = self
                .events
                .iter()
                .filter(|e| e.version() >= from_version as i64)
                .take(max_count)
                .map(|e| VersionInfo::new(e.version(), e.timestamp(), "test"))
                .collect();
            Ok(VersionHistoryPage::new(filtered, None))
        }

        async fn stream_ids_page(
            &self,
            _after: Option<&str>,
            _max_count: usize,
        ) -> CatgaResult<StreamIdsPage> {
            Ok(StreamIdsPage::new(Vec::new(), None))
        }
    }

    // Mock SnapshotStore for testing
    struct MockSnapshotStore {
        load_calls: Arc<std::sync::Mutex<Vec<String>>>,
        save_calls: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockSnapshotStore {
        fn new() -> Self {
            Self {
                load_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                save_calls: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl SnapshotStore for MockSnapshotStore {
        async fn save<S>(&self, snapshot: Snapshot<S>) -> CatgaResult<()>
        where
            S: Send + Sync + 'static,
        {
            let id = snapshot.stream_id().to_string();
            self.save_calls.lock().expect("mutex poisoned").push(id);
            Ok(())
        }

        async fn load<S>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<S>>>
        where
            S: Send + Sync + 'static,
        {
            self.load_calls
                .lock()
                .expect("mutex poisoned")
                .push(stream_id.to_string());
            Ok(None)
        }

        async fn delete(&self, _stream_id: &str) -> CatgaResult<()> {
            Ok(())
        }
    }

    // Helper to create a test envelope
    fn make_envelope(id: u64) -> Envelope {
        let metadata =
            MessageMetadata::new(id, None).with_quality_of_service(QualityOfService::AtLeastOnce);
        Envelope::new(id, "test-event", Vec::new(), metadata)
    }

    // Tests for EventCountSnapshotStrategy
    #[test]
    fn event_count_strategy_new_rejects_zero() {
        assert!(EventCountSnapshotStrategy::new(0).is_none());
    }

    #[test]
    fn event_count_strategy_new_accepts_nonzero() {
        let strategy = EventCountSnapshotStrategy::new(10);
        assert!(strategy.is_some());
        let strategy = strategy.expect("nonzero interval");
        assert_eq!(strategy.interval().get(), 10);
    }

    #[test]
    fn event_count_strategy_should_snapshot() {
        let strategy = EventCountSnapshotStrategy::new(5).expect("nonzero interval");
        assert!(!strategy.should_snapshot(0, 0));
        assert!(strategy.should_snapshot(5, 0));
        assert!(strategy.should_snapshot(10, 5));
        assert!(!strategy.should_snapshot(7, 3));
    }

    #[test]
    fn event_count_strategy_large_interval() {
        let strategy = EventCountSnapshotStrategy::new(usize::MAX).expect("nonzero interval");
        // i64::MAX - 0 = i64::MAX, which saturates to i64::MAX, which equals the max interval
        assert!(strategy.should_snapshot(i64::MAX, 0));
        // But i64::MAX - 1 = i64::MAX - 1, which is less than i64::MAX
        assert!(!strategy.should_snapshot(i64::MAX, 1));
    }

    // Tests for TimeBasedSnapshotStrategy
    #[test]
    fn time_based_strategy_new_accepts_zero_duration() {
        let strategy = TimeBasedSnapshotStrategy::new(Duration::ZERO);
        assert_eq!(strategy.interval(), Duration::ZERO);
    }

    #[test]
    fn time_based_strategy_should_snapshot() {
        let strategy = TimeBasedSnapshotStrategy::new(Duration::from_secs(60));
        let base = SystemTime::UNIX_EPOCH;

        assert!(!strategy.should_snapshot(base, base + Duration::from_secs(30)));
        assert!(strategy.should_snapshot(base, base + Duration::from_secs(60)));
        assert!(strategy.should_snapshot(base, base + Duration::from_secs(120)));
    }

    #[test]
    fn time_based_strategy_immediate_with_zero_interval() {
        let strategy = TimeBasedSnapshotStrategy::new(Duration::ZERO);
        let base = SystemTime::UNIX_EPOCH;
        assert!(strategy.should_snapshot(base, base));
    }

    // Tests for CompositeSnapshotStrategy
    #[test]
    fn composite_strategy_neither_threshold_met() {
        let events = EventCountSnapshotStrategy::new(100).expect("nonzero interval");
        let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(30));
        let composite = CompositeSnapshotStrategy::new(events, time);

        let base = SystemTime::UNIX_EPOCH;
        assert!(!composite.should_snapshot(5, 0, base, base + Duration::from_secs(10)));
    }

    #[test]
    fn composite_strategy_time_threshold_met() {
        let events = EventCountSnapshotStrategy::new(100).expect("nonzero interval");
        let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(30));
        let composite = CompositeSnapshotStrategy::new(events, time);

        let base = SystemTime::UNIX_EPOCH;
        assert!(composite.should_snapshot(5, 0, base, base + Duration::from_secs(30)));
    }

    #[test]
    fn composite_strategy_event_threshold_met() {
        let events = EventCountSnapshotStrategy::new(100).expect("nonzero interval");
        let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(30));
        let composite = CompositeSnapshotStrategy::new(events, time);

        let base = SystemTime::UNIX_EPOCH;
        assert!(composite.should_snapshot(100, 0, base, base));
    }

    #[test]
    fn composite_strategy_both_thresholds_met() {
        let events = EventCountSnapshotStrategy::new(100).expect("nonzero interval");
        let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(30));
        let composite = CompositeSnapshotStrategy::new(events, time);

        let base = SystemTime::UNIX_EPOCH;
        assert!(composite.should_snapshot(100, 0, base, base + Duration::from_secs(30)));
    }

    // Tests for next_event_version
    #[test]
    fn next_event_version_returns_next_for_normal_version() {
        assert_eq!(next_event_version(0), Some(1));
        assert_eq!(next_event_version(5), Some(6));
        assert_eq!(next_event_version(-1), Some(0));
    }

    #[test]
    fn next_event_version_returns_none_for_max() {
        assert_eq!(next_event_version(i64::MAX), None);
    }

    // Tests for AggregateRepository
    #[tokio::test]
    async fn repository_load_returns_none_for_missing_stream() {
        let event_store = MockEventStore::new(Vec::new());
        let snapshot_store = MockSnapshotStore::new();
        let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
        let repo = AggregateRepository::<TestAggregate, _, _>::new(
            &event_store,
            &snapshot_store,
            strategy,
        );

        let result = repo
            .load("nonexistent")
            .await
            .expect("load should not fail");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn repository_load_returns_none_when_no_snapshot_and_no_events() {
        let event_store = MockEventStore::new(Vec::new());
        let snapshot_store = MockSnapshotStore::new();
        let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
        let repo = AggregateRepository::<TestAggregate, _, _>::new(
            &event_store,
            &snapshot_store,
            strategy,
        );

        let result = repo.load("test-id").await.expect("load should not fail");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn repository_save_appends_pending_events() {
        let event_store = MockEventStore::new(Vec::new());
        let snapshot_store = MockSnapshotStore::new();
        let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
        let repo = AggregateRepository::<TestAggregate, _, _>::new(
            &event_store,
            &snapshot_store,
            strategy,
        );

        let mut aggregate = TestAggregate::new_with_id("test-id");
        aggregate.pending.push(make_envelope(1));
        aggregate.pending.push(make_envelope(2));

        repo.save(&mut aggregate)
            .await
            .expect("save should not fail");

        let calls = event_store.append_calls.lock().expect("mutex poisoned");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, 2);
    }

    #[tokio::test]
    async fn repository_save_does_nothing_when_no_pending_events() {
        let event_store = MockEventStore::new(Vec::new());
        let snapshot_store = MockSnapshotStore::new();
        let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
        let repo = AggregateRepository::<TestAggregate, _, _>::new(
            &event_store,
            &snapshot_store,
            strategy,
        );

        let mut aggregate = TestAggregate::new_with_id("test-id");

        repo.save(&mut aggregate)
            .await
            .expect("save should not fail");

        let calls = event_store.append_calls.lock().expect("mutex poisoned");
        assert!(calls.is_empty());
    }

    #[tokio::test]
    async fn repository_save_validates_version_consistency() {
        let event_store = MockEventStore::new(Vec::new());
        let snapshot_store = MockSnapshotStore::new();
        let strategy = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
        let repo = AggregateRepository::<TestAggregate, _, _>::new(
            &event_store,
            &snapshot_store,
            strategy,
        );

        let mut aggregate = TestAggregate::new_with_id("test-id");
        // Version is i64::MIN, trying to save 1 pending event
        // i64::MIN - 1 would overflow, triggering validation error
        aggregate.version = i64::MIN;
        aggregate.pending.push(make_envelope(1));

        let result = repo.save(&mut aggregate).await;
        assert!(result.is_err());
        assert_eq!(
            result.expect_err("save should fail").code(),
            ErrorCode::Validation
        );
    }
}
