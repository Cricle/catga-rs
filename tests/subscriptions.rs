//! Persistent subscription contract tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::memory::{MemoryEventStore, MemorySubscriptions};
use catga_core::{
    CatgaError, CatgaResult, CompetingSubscriptionRunner, Envelope, ErrorCode, EventPage,
    EventStore, MessageMetadata, PersistentSubscription, StoredEvent, StreamIdsPage,
    SubscriptionHandler, SubscriptionLoopOptions, SubscriptionRunner, SubscriptionStore,
    VersionHistoryPage,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct SumHandler(AtomicUsize);

struct StaticEventStore {
    event: StoredEvent,
}

#[async_trait]
impl EventStore for StaticEventStore {
    async fn append(
        &self,
        _stream_id: &str,
        _events: Vec<Envelope>,
        _expected_version: Option<i64>,
    ) -> CatgaResult<i64> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "static event store is read-only",
        ))
    }

    async fn read_page(
        &self,
        stream_id: &str,
        _from_version: u64,
        _max_count: usize,
    ) -> CatgaResult<EventPage> {
        Ok(EventPage::new(
            catga_core::EventStream::new(stream_id, self.event.version(), vec![self.event.clone()]),
            None,
        ))
    }

    async fn version(&self, _stream_id: &str) -> CatgaResult<i64> {
        Ok(self.event.version())
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
        Ok(VersionHistoryPage::new(
            vec![catga_core::VersionInfo::new(
                self.event.version(),
                self.event.timestamp(),
                self.event.envelope().message_type(),
            )],
            None,
        ))
    }

    async fn stream_ids_page(
        &self,
        _after: Option<&str>,
        _max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        Ok(StreamIdsPage::new(vec![String::from("orders-a")], None))
    }
}

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

#[tokio::test]
async fn competing_subscription_processes_at_most_one_matching_event_per_lease() {
    let events = MemoryEventStore::default();
    let subscriptions = MemorySubscriptions::default();
    let handler = SumHandler(AtomicUsize::new(0));
    subscriptions
        .save(PersistentSubscription::new("orders", "orders-*").with_event_types(["selected"]))
        .await
        .unwrap();
    events
        .append(
            "orders-a",
            vec![
                event(1, "ignored", 40),
                event(2, "selected", 2),
                event(3, "selected", 3),
            ],
            None,
        )
        .await
        .unwrap();

    let first =
        CompetingSubscriptionRunner::new(&events, &subscriptions, &handler, "orders", "worker-a");
    let second =
        CompetingSubscriptionRunner::new(&events, &subscriptions, &handler, "orders", "worker-b");

    assert_eq!(first.try_process_next().await.unwrap(), Some(true));
    assert_eq!(handler.0.load(Ordering::Acquire), 2);
    assert_eq!(
        subscriptions
            .load_checkpoint("orders", "orders-a")
            .await
            .unwrap()
            .unwrap()
            .version(),
        1
    );

    assert_eq!(second.try_process_next().await.unwrap(), Some(true));
    assert_eq!(handler.0.load(Ordering::Acquire), 5);
    assert_eq!(first.try_process_next().await.unwrap(), Some(false));
}

#[tokio::test]
async fn subscription_runtime_runs_immediately_and_stops_on_cancellation() {
    assert_eq!(
        SubscriptionLoopOptions::new(Duration::ZERO)
            .expect_err("zero interval is invalid")
            .code(),
        ErrorCode::Validation
    );

    let events = Arc::new(MemoryEventStore::default());
    let subscriptions = Arc::new(MemorySubscriptions::default());
    let handler = Arc::new(SumHandler(AtomicUsize::new(0)));
    subscriptions
        .save(PersistentSubscription::new("orders", "orders-*").with_event_types(["selected"]))
        .await
        .unwrap();
    events
        .append("orders-a", vec![event(1, "selected", 7)], None)
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn({
        let events = Arc::clone(&events);
        let subscriptions = Arc::clone(&subscriptions);
        let handler = Arc::clone(&handler);
        let shutdown = shutdown.clone();
        async move {
            SubscriptionRunner::new(events.as_ref(), subscriptions.as_ref(), handler.as_ref())
                .run_until_cancelled(
                    "orders",
                    SubscriptionLoopOptions::new(Duration::from_secs(60))
                        .expect("positive interval is valid"),
                    shutdown,
                )
                .await
        }
    });

    timeout(Duration::from_secs(1), async {
        while handler.0.load(Ordering::Acquire) != 7 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first subscription pass runs immediately");

    shutdown.cancel();
    let run = timeout(Duration::from_secs(1), worker)
        .await
        .expect("the subscription loop observes cancellation")
        .expect("the caller-owned task does not panic")
        .expect("the completed subscription loop succeeds");
    assert_eq!(run.handled(), 1);
    assert_eq!(run.streams(), 1);
}

#[tokio::test]
async fn subscription_treats_the_maximum_event_version_as_a_completed_stream() {
    let events = StaticEventStore {
        event: StoredEvent::new(
            i64::MAX,
            Arc::new(event(1, "ignored", 9)),
            SystemTime::now(),
        ),
    };
    let subscriptions = MemorySubscriptions::default();
    let handler = SumHandler(AtomicUsize::new(0));
    subscriptions
        .save(PersistentSubscription::new("orders", "orders-*").with_event_types(["selected"]))
        .await
        .unwrap();

    let runner = SubscriptionRunner::new(&events, &subscriptions, &handler);
    assert_eq!(runner.run_once("orders").await.unwrap().handled(), 0);
    assert_eq!(runner.run_once("orders").await.unwrap().handled(), 0);
    assert_eq!(handler.0.load(Ordering::Acquire), 0);
    assert_eq!(
        subscriptions
            .load_checkpoint("orders", "orders-a")
            .await
            .unwrap()
            .unwrap()
            .version(),
        i64::MAX
    );
}
