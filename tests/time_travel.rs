//! Aggregate time-travel contract tests.

use std::time::{Duration, SystemTime};

use catga_core::{Aggregate, Envelope, EventStore, MessageMetadata, TimeTravelService};
use catga_memory::MemoryEventStore;

#[derive(Clone)]
struct Counter {
    id: Box<str>,
    version: i64,
    total: u64,
}

impl Aggregate for Counter {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
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
