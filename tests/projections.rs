//! Projection checkpoint and catch-up contract tests.

use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatchUpProjectionRunner, CatgaResult, Envelope, EventStore, MessageMetadata, Projection,
    ProjectionCheckpointStore, StoredEvent,
};
use catga_memory::{MemoryEventStore, MemoryProjectionCheckpoints};

struct SumProjection {
    total: AtomicUsize,
}

impl SumProjection {
    const fn new() -> Self {
        Self {
            total: AtomicUsize::new(0),
        }
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Projection for SumProjection {
    fn name(&self) -> &str {
        "order-total"
    }

    async fn apply(&self, event: &StoredEvent) -> CatgaResult<()> {
        self.total
            .fetch_add(event.envelope().payload()[0] as usize, Ordering::AcqRel);
        Ok(())
    }

    async fn reset(&self) -> CatgaResult<()> {
        self.total.store(0, Ordering::Release);
        Ok(())
    }
}

fn event(id: u64, value: u8) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![value],
        MessageMetadata::new(id, None),
    )
}

#[tokio::test]
async fn catch_up_projection_tracks_each_stream_and_rebuilds_without_skipping_versions() {
    let events = MemoryEventStore::default();
    let checkpoints = MemoryProjectionCheckpoints::default();
    let projection = SumProjection::new();
    let runner = CatchUpProjectionRunner::with_batch_size(
        &events,
        &checkpoints,
        &projection,
        NonZeroUsize::new(1).unwrap(),
    );

    events
        .append("orders-a", vec![event(1, 1), event(2, 2)], None)
        .await
        .unwrap();
    events
        .append("orders-b", vec![event(3, 3)], None)
        .await
        .unwrap();

    assert_eq!(runner.run().await.unwrap().applied(), 3);
    assert_eq!(projection.total(), 6);
    assert_eq!(
        checkpoints
            .load("order-total", "orders-a")
            .await
            .unwrap()
            .unwrap()
            .version(),
        1
    );

    assert_eq!(runner.run().await.unwrap().applied(), 0);
    events
        .append("orders-b", vec![event(4, 4)], Some(0))
        .await
        .unwrap();
    assert_eq!(runner.run().await.unwrap().applied(), 1);
    assert_eq!(projection.total(), 10);

    assert_eq!(runner.rebuild().await.unwrap().applied(), 4);
    assert_eq!(projection.total(), 10);
    assert_eq!(
        checkpoints
            .load("order-total", "orders-b")
            .await
            .unwrap()
            .unwrap()
            .version(),
        1
    );
}
