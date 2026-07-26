//! Aggregate time-travel contract tests.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{
    Aggregate, CatgaError, CatgaResult, Envelope, ErrorCode, EventPage, EventStore, EventStream,
    MessageMetadata, Snapshot, SnapshotStore, SnapshotTimeTravelService, StreamIdsPage,
    TimeTravelService, VersionHistoryPage,
};
use catga_memory::{MemoryEnhancedSnapshots, MemoryEventStore};

#[derive(Clone)]
struct Counter {
    id: Box<str>,
    version: i64,
    total: u64,
    from_snapshot: bool,
}

impl Aggregate for Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
            from_snapshot: false,
        }
    }

    fn stream_id(id: &str) -> Box<str> {
        format!("counter:{id}").into()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn apply(&mut self, event: &Envelope) -> catga_core::CatgaResult<()> {
        self.total += event.payload()[0] as u64;
        self.version += 1;
        Ok(())
    }

    fn pending_events(&self) -> &[Envelope] {
        &[]
    }

    fn clear_pending_events(&mut self) {}
}

fn event(id: u64, amount: u8) -> Envelope {
    Envelope::new(
        id,
        "counter.incremented",
        vec![amount],
        MessageMetadata::new(id, None),
    )
}

struct TerminalSnapshotEventStore {
    read_calls: AtomicUsize,
}

impl TerminalSnapshotEventStore {
    const fn new() -> Self {
        Self {
            read_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl EventStore for TerminalSnapshotEventStore {
    async fn append(
        &self,
        _stream_id: &str,
        _events: Vec<Envelope>,
        _expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "read-only test store",
        ))
    }

    async fn read_page(
        &self,
        stream_id: &str,
        _from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        Ok(EventPage::new(
            EventStream::new(stream_id, i64::MAX, Vec::new()),
            None,
        ))
    }

    async fn version(&self, _stream_id: &str) -> CatgaResult<i64> {
        Ok(i64::MAX)
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
        _from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        Ok(VersionHistoryPage::new(Vec::new(), None))
    }

    async fn stream_ids_page(
        &self,
        _after: Option<&str>,
        _max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        Ok(StreamIdsPage::new(Vec::new(), None))
    }
}

#[tokio::test]
async fn time_travel_rebuilds_aggregate_state_at_versions_times_and_comparison_boundaries() {
    let store = MemoryEventStore::default();
    store
        .append(
            "counter:one",
            vec![event(1, 1), event(2, 2), event(3, 3)],
            None,
        )
        .await
        .unwrap();
    let time_travel = TimeTravelService::<Counter, _>::new(&store);

    let at_version = time_travel
        .state_at_version("one", 1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(at_version.version(), 1);
    assert_eq!(at_version.total, 3);
    assert_eq!(
        time_travel
            .state_at_time("one", SystemTime::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap()
            .total,
        6
    );
    assert!(
        time_travel
            .state_at_version("missing", 0)
            .await
            .unwrap()
            .is_none()
    );

    let comparison = time_travel.compare_versions("one", 0, 2).await.unwrap();
    assert_eq!(comparison.from_state().unwrap().total, 1);
    assert_eq!(comparison.to_state().unwrap().total, 6);
    assert_eq!(comparison.events_between().len(), 2);
    assert_eq!(comparison.events_between()[0].version(), 1);
}

#[tokio::test]
async fn snapshot_time_travel_replays_only_events_after_the_selected_snapshot() {
    let events = MemoryEventStore::default();
    events
        .append(
            "counter:one",
            vec![event(1, 1), event(2, 2), event(3, 3)],
            None,
        )
        .await
        .unwrap();
    let snapshots = MemoryEnhancedSnapshots::default();
    snapshots
        .save(Snapshot::new(
            "counter:one",
            Counter {
                id: "one".into(),
                version: 1,
                total: 3,
                from_snapshot: true,
            },
            1,
        ))
        .await
        .unwrap();
    let time_travel = SnapshotTimeTravelService::<Counter, _, _>::new(&events, &snapshots);

    let state = time_travel
        .state_at_version("one", 2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.version(), 2);
    assert_eq!(state.total, 6);
    assert!(state.from_snapshot);
}

#[tokio::test]
async fn snapshot_time_travel_rejects_a_snapshot_with_a_mismatched_aggregate_version() {
    let events = MemoryEventStore::default();
    events
        .append("counter:one", vec![event(1, 1), event(2, 2)], None)
        .await
        .unwrap();
    let snapshots = MemoryEnhancedSnapshots::default();
    snapshots
        .save(Snapshot::new(
            "counter:one",
            Counter {
                id: "one".into(),
                version: 0,
                total: 3,
                from_snapshot: true,
            },
            1,
        ))
        .await
        .unwrap();
    let time_travel = SnapshotTimeTravelService::<Counter, _, _>::new(&events, &snapshots);

    let error = match time_travel.state_at_version("one", 1).await {
        Err(error) => error,
        Ok(_) => panic!("mismatched snapshot version must fail"),
    };
    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
}

#[tokio::test]
async fn snapshot_time_travel_does_not_replay_a_terminal_snapshot_event() {
    let events = TerminalSnapshotEventStore::new();
    let snapshots = MemoryEnhancedSnapshots::default();
    snapshots
        .save(Snapshot::new(
            "counter:one",
            Counter {
                id: "one".into(),
                version: i64::MAX,
                total: 42,
                from_snapshot: true,
            },
            i64::MAX,
        ))
        .await
        .unwrap();
    let time_travel = SnapshotTimeTravelService::<Counter, _, _>::new(&events, &snapshots);

    let state = time_travel
        .state_at_version("one", i64::MAX)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(state.version(), i64::MAX);
    assert_eq!(state.total, 42);
    assert_eq!(events.read_calls.load(Ordering::Relaxed), 0);
}
