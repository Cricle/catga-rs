//! Batch and stream dispatch tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, ErrorCode, Event, EventHandler, Handler, MAX_MEDIATOR_BATCH_SIZE, Mediator,
    Registry, Request,
};
use futures::{StreamExt, stream};

#[derive(Debug)]
struct Work(u64);

impl catga_core::Message for Work {}

impl Request for Work {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Clone)]
struct Published;

impl catga_core::Message for Published {}
impl Event for Published { type TypeId = catga_core::DefaultMessageTypeId; }

#[derive(Default)]
struct Probe {
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

struct WorkHandler(Arc<Probe>);

struct PublishedHandler(Arc<Probe>);

#[async_trait]
impl Handler<Work> for WorkHandler {
    async fn handle(&self, work: Work) -> CatgaResult<u64> {
        let now = self.0.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.max_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(work.0 * 2)
    }
}

#[async_trait]
impl EventHandler<Published> for PublishedHandler {
    async fn handle(&self, _: Published) -> CatgaResult<()> {
        let now = self.0.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.max_in_flight.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

fn mediator(probe: Arc<Probe>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<Work, _>(WorkHandler(probe))
        .unwrap();
    Mediator::new(registry)
}

#[tokio::test]
async fn batch_dispatch_preserves_input_order_and_respects_concurrency_limit() {
    let probe = Arc::new(Probe::default());

    let results = mediator(probe.clone())
        .send_batch(vec![Work(1), Work(2), Work(3)], 2)
        .await
        .unwrap();

    assert_eq!(results, vec![Ok(2), Ok(4), Ok(6)]);
    assert_eq!(probe.max_in_flight.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn batch_dispatch_rejects_more_than_the_bounded_input_limit_before_work_starts() {
    let probe = Arc::new(Probe::default());

    let error = mediator(Arc::clone(&probe))
        .send_batch(
            (0..=MAX_MEDIATOR_BATCH_SIZE).map(|value| Work(value as u64)),
            2,
        )
        .await
        .expect_err("oversized batches must use the streaming API");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(probe.max_in_flight.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stream_dispatch_returns_a_result_for_each_request() {
    let results = mediator(Arc::new(Probe::default()))
        .send_stream(stream::iter([Work(3), Work(4)]))
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results, vec![Ok(6), Ok(8)]);
}

#[tokio::test]
async fn batch_event_publication_respects_the_concurrency_limit() {
    let probe = Arc::new(Probe::default());
    let mut registry = Registry::new();
    registry.register_event::<Published, _>(PublishedHandler(Arc::clone(&probe)));
    let mediator = Mediator::new(registry);

    mediator
        .publish_batch([Published, Published, Published], 2)
        .await
        .expect("every event handler must complete");

    assert_eq!(probe.max_in_flight.load(Ordering::SeqCst), 2);
}
