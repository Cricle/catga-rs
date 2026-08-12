//! Contract coverage for event-sourced aggregates, snapshot strategies, and
//! the time-travel reconstruction services.

use std::time::{Duration, SystemTime};

use catga_core::memory::{MemoryEnhancedSnapshots, MemoryEventStore, MemorySnapshots};
use catga_core::{
    Aggregate, AggregateRepository, CatgaError, CatgaResult, CompositeSnapshotStrategy, Envelope,
    ErrorCode, EventCountSnapshotStrategy, EventStore, Snapshot, SnapshotStore, SnapshotStrategy,
    SnapshotTimeTravelService, TimeBasedSnapshotStrategy, TimeTravelService, assert_error_code,
    assert_failure, assert_success,
};

/// A counter aggregate whose event payload is one delta byte.
///
/// Payload byte `255` applies without error but advances the version by two,
/// letting tests exercise the version-mismatch guard deterministically.
#[derive(Clone, Debug, PartialEq)]
struct Counter {
    id: String,
    version: i64,
    count: i64,
    pending: Vec<Envelope>,
}

impl Counter {
    fn seed() -> Self {
        Self {
            id: String::new(),
            version: -1,
            count: 0,
            pending: Vec::new(),
        }
    }

    fn bump(&mut self, delta: u8) {
        self.count += i64::from(delta);
        self.version += 1;
        self.pending.push(Envelope::new(
            self.version as u64,
            "counter::Bumped",
            vec![delta],
            catga_core::MessageMetadata::new(self.version as u64, None),
        ));
    }

    fn at_version(version: i64) -> Self {
        Self {
            id: "fixed".into(),
            version,
            count: version.saturating_mul(10),
            pending: Vec::new(),
        }
    }
}

impl Aggregate for Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_owned(),
            ..Self::seed()
        }
    }

    fn stream_id(id: &str) -> Box<str> {
        format!("counter-{id}").into()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
        if event.message_type() == "counter::Boom" {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "counter refuses boom events",
            ));
        }
        let delta = event.payload().first().copied().unwrap_or(0);
        if delta == 255 {
            self.version += 2;
            return Ok(());
        }
        self.count += i64::from(delta);
        self.version += 1;
        Ok(())
    }

    fn pending_events(&self) -> &[Envelope] {
        &self.pending
    }

    fn clear_pending_events(&mut self) {
        self.pending.clear();
    }
}

fn bump_envelope(id: u64, delta: u8) -> Envelope {
    Envelope::new(
        id,
        "counter::Bumped",
        vec![delta],
        catga_core::MessageMetadata::new(id, None),
    )
}

async fn seed_stream(store: &MemoryEventStore, id: &str, deltas: &[u8]) {
    let events: Vec<Envelope> = deltas
        .iter()
        .enumerate()
        .map(|(index, delta)| bump_envelope(index as u64, *delta))
        .collect();
    assert_success(store.append(&Counter::stream_id(id), events, None).await);
}

// ---------------------------------------------------------------------------
// Snapshot strategies
// ---------------------------------------------------------------------------

#[test]
fn snapshot_strategies_respect_their_thresholds() {
    assert!(EventCountSnapshotStrategy::new(0).is_none());
    let events = EventCountSnapshotStrategy::new(10).expect("nonzero interval");
    assert_eq!(events.interval().get(), 10);
    assert!(!events.should_snapshot(9, 0));
    assert!(events.should_snapshot(10, 0));
    assert!(events.should_snapshot(16, 6));
    assert!(!events.should_snapshot(5, 0));

    let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(60));
    assert_eq!(time.interval(), Duration::from_secs(60));
    let base = SystemTime::UNIX_EPOCH;
    assert!(!time.should_snapshot(base, base + Duration::from_secs(59)));
    assert!(time.should_snapshot(base, base + Duration::from_secs(60)));
    // A clock skew backwards in time never requests a snapshot.
    assert!(!time.should_snapshot(base + Duration::from_secs(60), base));
    // A zero interval snapshots immediately.
    let immediate = TimeBasedSnapshotStrategy::new(Duration::ZERO);
    assert!(immediate.should_snapshot(base, base));

    let composite = CompositeSnapshotStrategy::new(events, time);
    assert!(!composite.should_snapshot(5, 0, base, base + Duration::from_secs(10)));
    assert!(composite.should_snapshot(5, 0, base, base + Duration::from_secs(60)));
    assert!(composite.should_snapshot(10, 0, base, base));
}

// ---------------------------------------------------------------------------
// AggregateRepository
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_repository_loads_saves_and_snapshots() {
    let events = MemoryEventStore::default();
    let snapshots = MemorySnapshots::default();
    let strategy = EventCountSnapshotStrategy::new(2).expect("nonzero");
    let repo = AggregateRepository::<Counter, _, _>::new(&events, &snapshots, strategy);

    // Unknown aggregates load as absent.
    assert!(assert_success(repo.load("missing").await).is_none());

    // Saving appends pending events at the aggregate's original version.
    let mut counter = Counter::new("orders");
    counter.bump(3);
    counter.bump(4);
    assert_success(repo.save(&mut counter).await);
    assert!(counter.pending_events().is_empty());
    assert_eq!(
        assert_success(events.version(&Counter::stream_id("orders")).await),
        1
    );

    // The snapshot interval of two events recorded a snapshot.
    let snapshot = assert_success(
        snapshots
            .load::<Counter>(&Counter::stream_id("orders"))
            .await,
    );
    let snapshot = snapshot.expect("snapshot saved at the interval");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.shared_state().count, 7);

    // Reloading rebuilds from the snapshot and replays nothing further.
    let reloaded = assert_success(repo.load("orders").await).expect("aggregate exists");
    assert_eq!(reloaded.version(), 1);
    assert_eq!(reloaded.count, 7);

    // Further saves extend the stream and replay over the snapshot on load.
    let mut reloaded = reloaded;
    reloaded.bump(5);
    assert_success(repo.save(&mut reloaded).await);
    let final_state = assert_success(repo.load("orders").await).expect("aggregate exists");
    assert_eq!(final_state.version(), 2);
    assert_eq!(final_state.count, 12);

    // Saving without pending events is a no-op.
    let mut idle = final_state;
    assert_success(repo.save(&mut idle).await);
}

#[tokio::test]
async fn aggregate_repository_reports_conflicts_and_corruption() {
    let events = MemoryEventStore::default();
    let snapshots = MemorySnapshots::default();
    let strategy = EventCountSnapshotStrategy::new(100).expect("nonzero");
    let repo = AggregateRepository::<Counter, _, _>::new(&events, &snapshots, strategy);

    seed_stream(&events, "conflicted", &[1, 2]).await;

    // A stale aggregate expecting an empty stream conflicts on save.
    let mut stale = Counter::new("conflicted");
    stale.bump(9);
    let error = assert_failure(repo.save(&mut stale).await);
    assert_eq!(error.code(), ErrorCode::Conflict);

    // An event whose application fails propagates during load.
    let stream = Counter::stream_id("boom");
    assert_success(
        events
            .append(
                &stream,
                vec![
                    bump_envelope(0, 1),
                    Envelope::new(
                        1,
                        "counter::Boom",
                        Vec::new(),
                        catga_core::MessageMetadata::new(1, None),
                    ),
                ],
                None,
            )
            .await,
    );
    assert_error_code(repo.load("boom").await, ErrorCode::Validation);

    // An event that advances the version too far trips the replay guard.
    let skewed = Counter::stream_id("skewed");
    assert_success(
        events
            .append(&skewed, vec![bump_envelope(0, 255)], None)
            .await,
    );
    assert_error_code(repo.load("skewed").await, ErrorCode::Validation);

    // A snapshot whose version disagrees with its state is rejected.
    let corrupted = Counter::stream_id("corrupted");
    assert_success(
        snapshots
            .save(Snapshot::new(corrupted.clone(), Counter::at_version(5), 3))
            .await,
    );
    assert_error_code(repo.load("corrupted").await, ErrorCode::Validation);

    // A terminal snapshot at i64::MAX short-circuits event replay.
    let terminal = Counter::stream_id("terminal");
    assert_success(
        snapshots
            .save(Snapshot::new(
                terminal.clone(),
                Counter::at_version(i64::MAX),
                i64::MAX,
            ))
            .await,
    );
    let loaded = assert_success(repo.load("terminal").await).expect("terminal snapshot");
    assert_eq!(loaded.version(), i64::MAX);
}

// ---------------------------------------------------------------------------
// TimeTravelService
// ---------------------------------------------------------------------------

#[tokio::test]
async fn time_travel_rebuilds_states_at_versions_and_times() {
    let events = MemoryEventStore::default();
    seed_stream(&events, "journey", &[1, 2, 3, 4, 5]).await;
    let service = TimeTravelService::<Counter, _>::new(&events);

    assert!(assert_success(service.state_at_version("journey", -1).await).is_none());
    assert!(assert_success(service.state_at_version("ghost", 3).await).is_none());

    let at_two =
        assert_success(service.state_at_version("journey", 2).await).expect("events exist");
    assert_eq!(at_two.version(), 2);
    assert_eq!(at_two.count, 6);

    let at_end =
        assert_success(service.state_at_version("journey", 99).await).expect("events exist");
    assert_eq!(at_end.count, 15);

    // Time bounds: everything is after the epoch and before an hour from now.
    assert!(
        assert_success(
            service
                .state_at_time("journey", SystemTime::UNIX_EPOCH)
                .await
        )
        .is_none()
    );
    let future = SystemTime::now() + Duration::from_secs(3600);
    let by_time =
        assert_success(service.state_at_time("journey", future).await).expect("events exist");
    assert_eq!(by_time.count, 15);
    assert!(assert_success(service.state_at_time("ghost", future).await).is_none());

    let history = assert_success(service.version_history_page("journey", 0, 10).await);
    assert_eq!(history.entries().len(), 5);

    // Comparisons rebuild both endpoints and list the events between them.
    let comparison = assert_success(service.compare_versions("journey", 1, 3).await);
    assert_eq!(comparison.from_version(), 1);
    assert_eq!(comparison.to_version(), 3);
    assert_eq!(comparison.from_state().expect("state").count, 3);
    assert_eq!(comparison.to_state().expect("state").count, 10);
    let between: Vec<i64> = comparison
        .events_between()
        .iter()
        .map(|info| info.version())
        .collect();
    assert_eq!(between, vec![2, 3]);

    // A negative endpoint yields no state at that end.
    let negative = assert_success(service.compare_versions("journey", -1, 0).await);
    assert!(negative.from_state().is_none());
    assert_eq!(negative.events_between().len(), 1);

    // Reversed comparisons are rejected.
    assert_error_code(
        service.compare_versions("journey", 4, 2).await,
        ErrorCode::Validation,
    );
}

#[tokio::test]
async fn time_travel_comparison_history_is_bounded() {
    let events = MemoryEventStore::default();
    let stream = Counter::stream_id("wide");
    let bulk: Vec<Envelope> = (0..1025_u64).map(|index| bump_envelope(index, 1)).collect();
    assert_success(events.append(&stream, bulk, None).await);

    let service = TimeTravelService::<Counter, _>::new(&events);
    let comparison = service.compare_versions("wide", -1, 1024).await;
    assert_error_code(comparison, ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// SnapshotTimeTravelService
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_time_travel_prefers_snapshots_and_replays_the_tail() {
    let events = MemoryEventStore::default();
    let snapshots = MemoryEnhancedSnapshots::default();
    seed_stream(&events, "anchored", &[1, 2, 3, 4, 5]).await;

    // Snapshot the state at version 2 (count 6) and rebuild later versions.
    let mut mid = Counter::new("anchored");
    for delta in [1_u8, 2, 3] {
        mid.bump(delta);
    }
    let stream = Counter::stream_id("anchored");
    assert_success(
        snapshots
            .save(Snapshot::new(stream.clone(), mid.clone(), 2))
            .await,
    );

    let service = SnapshotTimeTravelService::<Counter, _, _>::new(&events, &snapshots);

    assert!(assert_success(service.state_at_version("anchored", -1).await).is_none());
    assert!(assert_success(service.state_at_version("ghost", 2).await).is_none());

    let at_four =
        assert_success(service.state_at_version("anchored", 4).await).expect("events exist");
    assert_eq!(at_four.version(), 4);
    assert_eq!(at_four.count, 15);

    // Exactly at the snapshot version no replay is required.
    let at_snapshot =
        assert_success(service.state_at_version("anchored", 2).await).expect("snapshot present");
    assert_eq!(at_snapshot.count, 6);

    // A terminal snapshot ends reconstruction immediately.
    let terminal = Counter::stream_id("terminal");
    assert_success(
        snapshots
            .save(Snapshot::new(
                terminal.clone(),
                Counter::at_version(i64::MAX),
                i64::MAX,
            ))
            .await,
    );
    let terminal_state = assert_success(service.state_at_version("terminal", i64::MAX).await)
        .expect("terminal snapshot");
    assert_eq!(terminal_state.version(), i64::MAX);

    // Time-based reconstruction finds the last event, anchors on the nearest
    // snapshot, and replays the remainder.
    let future = SystemTime::now() + Duration::from_secs(3600);
    let by_time =
        assert_success(service.state_at_time("anchored", future).await).expect("events exist");
    assert_eq!(by_time.version(), 4);
    assert_eq!(by_time.count, 15);
    assert!(
        assert_success(
            service
                .state_at_time("anchored", SystemTime::UNIX_EPOCH)
                .await
        )
        .is_none()
    );

    let history = assert_success(service.version_history_page("anchored", 0, 10).await);
    assert_eq!(history.entries().len(), 5);

    let comparison = assert_success(service.compare_versions("anchored", 1, 3).await);
    assert_eq!(comparison.from_state().expect("state").count, 3);
    assert_eq!(comparison.to_state().expect("state").count, 10);
    assert_eq!(comparison.events_between().len(), 2);

    assert_error_code(
        service.compare_versions("anchored", 3, 1).await,
        ErrorCode::Validation,
    );

    // A corrupted snapshot poisons reconstruction with a validation error.
    let corrupted = Counter::stream_id("corrupted");
    assert_success(
        snapshots
            .save(Snapshot::new(corrupted.clone(), Counter::at_version(9), 4))
            .await,
    );
    assert_error_code(
        service.state_at_version("corrupted", 4).await,
        ErrorCode::Validation,
    );
}
