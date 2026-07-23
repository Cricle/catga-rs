//! Persistent subscription contract tests.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_core::{
    CompetingSubscriptionRunner, Envelope, EventStore, MessageMetadata, PersistentSubscription,
    StoredEvent, SubscriptionHandler, SubscriptionRunner, SubscriptionStore,
};
use catga_memory::{MemoryEventStore, MemorySubscriptions};

struct SumHandler(AtomicUsize);

#[async_trait]
impl SubscriptionHandler for SumHandler {
    async fn handle(&self, event: &StoredEvent) -> catga_core::CatgaResult<()> {
        self.0
            .fetch_add(event.envelope().payload()[0] as usize, Ordering::AcqRel);
        Ok(())
    }
}

fn event(id: u64, event_type: &str, value: u8) -> Envelope {
    Envelope::new(id, event_type, vec![value], MessageMetadata::new(id, None))
}

#[tokio::test]
async fn subscriptions_filter_events_checkpoint_each_stream_and_exclusively_lease_consumers() {
    let events = MemoryEventStore::default();
    let subscriptions = MemorySubscriptions::default();
    let handler = SumHandler(AtomicUsize::new(0));
    subscriptions
        .save(PersistentSubscription::new("orders", "orders-*").with_event_types(["order.created"]))
        .await
        .unwrap();
    events
        .append(
            "orders-a",
            vec![
                event(1, "order.created", 1),
                event(2, "order.cancelled", 20),
            ],
            None,
        )
        .await
        .unwrap();
    events
        .append("orders-b", vec![event(3, "order.created", 3)], None)
        .await
        .unwrap();
    events
        .append("invoices-a", vec![event(4, "order.created", 40)], None)
        .await
        .unwrap();

    let runner = SubscriptionRunner::new(&events, &subscriptions, &handler);
    let run = runner.run_once("orders").await.unwrap();
    assert_eq!(run.handled(), 2);
    assert_eq!(handler.0.load(Ordering::Acquire), 4);
    assert_eq!(
        subscriptions
            .load_checkpoint("orders", "orders-a")
            .await
            .unwrap()
            .unwrap()
            .version(),
        1
    );

    events
        .append("orders-a", vec![event(5, "order.created", 5)], Some(1))
        .await
        .unwrap();
    assert_eq!(runner.run_once("orders").await.unwrap().handled(), 1);
    assert_eq!(handler.0.load(Ordering::Acquire), 9);

    assert!(
        subscriptions
            .try_acquire("orders", "worker-a")
            .await
            .unwrap()
    );
    assert!(
        !subscriptions
            .try_acquire("orders", "worker-b")
            .await
            .unwrap()
    );
    subscriptions.release("orders", "worker-a").await.unwrap();
    assert!(
        subscriptions
            .try_acquire("orders", "worker-b")
            .await
            .unwrap()
    );
    subscriptions.release("orders", "worker-b").await.unwrap();

    let competing =
        CompetingSubscriptionRunner::new(&events, &subscriptions, &handler, "orders", "worker-a");
    assert_eq!(
        competing.try_run_once().await.unwrap().unwrap().handled(),
        0
    );
    assert!(
        subscriptions
            .try_acquire("orders", "worker-b")
            .await
            .unwrap()
    );
}
