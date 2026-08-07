//! Aggregate repository contract tests.

use std::time::{Duration, SystemTime};

use catga_core::memory::{MemoryEventStore, MemorySnapshots};
use catga_core::{
    Aggregate, AggregateRepository, CompositeSnapshotStrategy, Envelope,
    EventCountSnapshotStrategy, EventStore, MessageMetadata, SnapshotStore,
    TimeBasedSnapshotStrategy,
};

#[derive(Clone)]
struct Counter {
    id: Box<str>,
    version: i64,
    total: u64,
    pending: Vec<Envelope>,
}

impl Counter {
    fn raise(&mut self, event: Envelope) {
        self.apply(&event).unwrap();
        self.pending.push(event);
    }
}

impl Aggregate for Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
            pending: Vec::new(),
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
        &self.pending
    }

    fn clear_pending_events(&mut self) {
        self.pending.clear();
    }
}

fn increment(id: u64, amount: u8) -> Envelope {
    Envelope::new(
        id,
        "counter.incremented",
        vec![amount],
        MessageMetadata::new(id, None),
    )
}

#[test]
fn time_and_composite_snapshot_strategies_trigger_without_state_or_locks() {
    let time = TimeBasedSnapshotStrategy::new(Duration::from_secs(10));
    let last = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    assert!(!time.should_snapshot(last, last + Duration::from_secs(9)));
    assert!(time.should_snapshot(last, last + Duration::from_secs(10)));
    let composite =
        CompositeSnapshotStrategy::new(EventCountSnapshotStrategy::new(5).unwrap(), time);
    assert!(composite.should_snapshot(5, 0, last, last + Duration::from_secs(1)));
    assert!(composite.should_snapshot(1, 0, last, last + Duration::from_secs(10)));
}

#[tokio::test]
async fn aggregate_repository_saves_with_optimistic_versions_and_recovers_from_snapshots() {
    let events = MemoryEventStore::default();
    let snapshots = MemorySnapshots::default();
    let repository = AggregateRepository::<Counter, _, _>::new(
        &events,
        &snapshots,
        EventCountSnapshotStrategy::new(2).unwrap(),
    );
    let mut counter = Counter::new("one");
    counter.raise(increment(1, 2));
    repository.save(&mut counter).await.unwrap();
    counter.raise(increment(2, 3));
    repository.save(&mut counter).await.unwrap();

    assert!(counter.pending_events().is_empty());
    assert_eq!(
        snapshots
            .load::<Counter>("counter:one")
            .await
            .unwrap()
            .unwrap()
            .version(),
        1
    );

    events
        .append("counter:one", vec![increment(3, 5)], Some(1))
        .await
        .unwrap();
    let restored = repository.load("one").await.unwrap().unwrap();
    assert_eq!(restored.version(), 2);
    assert_eq!(restored.total, 10);

    let mut stale = Counter::new("one");
    stale.raise(increment(4, 7));
    assert_eq!(
        repository.save(&mut stale).await.unwrap_err().code(),
        catga_core::ErrorCode::Conflict
    );
}
