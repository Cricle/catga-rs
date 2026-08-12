//! Strict contracts for persistent subscriptions: pattern matching, paged
//! replay, checkpoint advancement, loop lifecycle, and competing-consumer
//! lease semantics.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CompetingSubscriptionRunner, Envelope, ErrorCode, EventPage,
    EventStore, EventStream, MessageMetadata, PersistentSubscription, StoredEvent, StreamIdsPage,
    SubscriptionCheckpoint, SubscriptionHandler, SubscriptionLoopOptions, SubscriptionRunner,
    SubscriptionStore, VersionHistoryPage, memory::MemorySubscriptions,
};
use tokio_util::sync::CancellationToken;

fn stored(stream_version: i64, id: u64, message_type: &str) -> StoredEvent {
    StoredEvent::new(
        stream_version,
        Arc::new(Envelope::new(
            id,
            message_type,
            vec![id as u8],
            MessageMetadata::new(id, None),
        )),
        SystemTime::now(),
    )
}

// ---------------------------------------------------------------------------
// Event store stub with forced paging and fault injection
// ---------------------------------------------------------------------------

/// Paged in-memory event store used to drive the subscription runner.
struct MiniEvents {
    streams: BTreeMap<String, Vec<StoredEvent>>,
    id_page_size: usize,
    read_page_size: usize,
    fail_stream_ids: AtomicBool,
    fail_read: AtomicBool,
}

impl MiniEvents {
    fn new(id_page_size: usize, read_page_size: usize) -> Self {
        Self {
            streams: BTreeMap::new(),
            id_page_size,
            read_page_size,
            fail_stream_ids: AtomicBool::new(false),
            fail_read: AtomicBool::new(false),
        }
    }

    fn push(&mut self, stream_id: &str, event: StoredEvent) {
        self.streams
            .entry(stream_id.into())
            .or_default()
            .push(event);
    }
}

#[async_trait]
impl EventStore for MiniEvents {
    async fn append(&self, _: &str, _: Vec<Envelope>, _: Option<i64>) -> CatgaResult<i64> {
        Err(CatgaError::new(ErrorCode::Unsupported, "read-only stub"))
    }

    async fn read_page(
        &self,
        stream_id: &str,
        from_version: u64,
        max_count: usize,
    ) -> CatgaResult<EventPage> {
        if self.fail_read.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "read failed"));
        }
        let events = self
            .streams
            .get(stream_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let last_version = events.last().map(StoredEvent::version).unwrap_or(-1);
        let start = i64::try_from(from_version).unwrap_or(i64::MAX);
        let remaining: Vec<StoredEvent> = events
            .iter()
            .filter(|event| event.version() >= start)
            .cloned()
            .collect();
        let take = remaining.len().min(self.read_page_size).min(max_count);
        let page_events: Vec<StoredEvent> = remaining.into_iter().take(take).collect();
        let next_version = if take == 0 {
            None
        } else {
            let next = from_version.saturating_add(take as u64);
            let has_more = events
                .iter()
                .any(|event| event.version() >= i64::try_from(next).unwrap_or(i64::MAX));
            has_more.then_some(next)
        };
        Ok(EventPage::new(
            EventStream::new(stream_id, last_version, page_events),
            next_version,
        ))
    }

    async fn version(&self, stream_id: &str) -> CatgaResult<i64> {
        Ok(self
            .streams
            .get(stream_id)
            .and_then(|events| events.last())
            .map(StoredEvent::version)
            .unwrap_or(-1))
    }

    async fn read_to_version_page(
        &self,
        _: &str,
        _: u64,
        _: i64,
        _: usize,
    ) -> CatgaResult<EventPage> {
        Err(CatgaError::new(ErrorCode::Unsupported, "read-only stub"))
    }

    async fn read_to_time_page(
        &self,
        _: &str,
        _: u64,
        _: SystemTime,
        _: usize,
    ) -> CatgaResult<EventPage> {
        Err(CatgaError::new(ErrorCode::Unsupported, "read-only stub"))
    }

    async fn version_history_page(
        &self,
        _: &str,
        _: u64,
        _: usize,
    ) -> CatgaResult<VersionHistoryPage> {
        Err(CatgaError::new(ErrorCode::Unsupported, "read-only stub"))
    }

    async fn stream_ids_page(
        &self,
        after: Option<&str>,
        max_count: usize,
    ) -> CatgaResult<StreamIdsPage> {
        if self.fail_stream_ids.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "stream ids failed"));
        }
        let keys: Vec<String> = self
            .streams
            .keys()
            .filter(|key| after.is_none_or(|cursor| key.as_str() > cursor))
            .cloned()
            .collect();
        let take = keys.len().min(self.id_page_size).min(max_count);
        let ids: Vec<String> = keys.iter().take(take).cloned().collect();
        let next = if keys.len() > take {
            ids.last().cloned()
        } else {
            None
        };
        Ok(StreamIdsPage::new(ids, next))
    }
}

/// Records every handled event version and optionally fails one message type.
struct RecordingHandler {
    seen: Mutex<Vec<i64>>,
    fail_type: &'static str,
}

#[async_trait]
impl SubscriptionHandler for RecordingHandler {
    async fn handle(&self, event: &StoredEvent) -> CatgaResult<()> {
        if event.envelope().message_type() == self.fail_type {
            return Err(CatgaError::new(ErrorCode::Validation, "handler refused"));
        }
        self.seen
            .lock()
            .expect("handler lock")
            .push(event.version());
        Ok(())
    }
}

/// Subscription store wrapper with selectable faults.
struct FaultySubs {
    inner: MemorySubscriptions,
    fail_release: AtomicBool,
    fail_save_checkpoint: AtomicBool,
    fail_load_checkpoint: AtomicBool,
}

impl FaultySubs {
    fn new() -> Self {
        Self {
            inner: MemorySubscriptions::default(),
            fail_release: AtomicBool::new(false),
            fail_save_checkpoint: AtomicBool::new(false),
            fail_load_checkpoint: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl SubscriptionStore for FaultySubs {
    async fn save(&self, subscription: PersistentSubscription) -> CatgaResult<()> {
        self.inner.save(subscription).await
    }

    async fn load(&self, name: &str) -> CatgaResult<Option<PersistentSubscription>> {
        self.inner.load(name).await
    }

    async fn delete(&self, name: &str) -> CatgaResult<()> {
        self.inner.delete(name).await
    }

    async fn list(&self) -> CatgaResult<Vec<PersistentSubscription>> {
        self.inner.list().await
    }

    async fn save_checkpoint(&self, checkpoint: SubscriptionCheckpoint) -> CatgaResult<()> {
        if self.fail_save_checkpoint.load(Ordering::SeqCst) {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "checkpoint store down",
            ));
        }
        self.inner.save_checkpoint(checkpoint).await
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<SubscriptionCheckpoint>> {
        if self.fail_load_checkpoint.load(Ordering::SeqCst) {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "checkpoint store down",
            ));
        }
        self.inner
            .load_checkpoint(subscription_name, stream_id)
            .await
    }

    async fn try_acquire(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<bool> {
        self.inner.try_acquire(subscription_name, consumer_id).await
    }

    async fn release(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<()> {
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(CatgaError::new(ErrorCode::Unavailable, "release failed"));
        }
        self.inner.release(subscription_name, consumer_id).await
    }
}

/// Builds the canonical fixture: two matching streams plus an outside stream.
fn standard_fixture() -> (MiniEvents, FaultySubs, RecordingHandler) {
    let mut events = MiniEvents::new(1, 2);
    events.push("order-1", stored(0, 1, "OrderPlaced"));
    events.push("order-1", stored(1, 2, "OrderNote"));
    events.push("order-1", stored(2, 3, "OrderPlaced"));
    events.push("order-2", stored(0, 4, "OrderPlaced"));
    events.push("other-1", stored(0, 5, "OrderPlaced"));
    let subs = FaultySubs::new();
    let handler = RecordingHandler {
        seen: Mutex::new(Vec::new()),
        fail_type: "",
    };
    (events, subs, handler)
}

// ---------------------------------------------------------------------------
// Definition contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn subscription_definition_matching_and_options() {
    let exact = PersistentSubscription::new("s", "order-1");
    assert!(exact.matches_stream("order-1"));
    assert!(!exact.matches_stream("order-11"));

    let prefix = PersistentSubscription::new("s", "order-*");
    assert!(prefix.matches_stream("order-1"));
    assert!(prefix.matches_stream("order-"));
    assert!(!prefix.matches_stream("orders-1"));

    let all = PersistentSubscription::new("s", "*");
    assert!(all.matches_stream("anything"));

    assert!(exact.matches_event_type("Any"), "no filter accepts all");
    let filtered = exact
        .clone()
        .with_event_types(["OrderPlaced", "OrderShipped"]);
    assert!(filtered.matches_event_type("OrderPlaced"));
    assert!(filtered.matches_event_type("OrderShipped"));
    assert!(!filtered.matches_event_type("OrderNote"));
    assert_eq!(filtered.event_types().len(), 2);
    assert_eq!(filtered.name(), "s");
    assert_eq!(filtered.stream_pattern(), "order-1");

    let checkpoint = SubscriptionCheckpoint::new("s", "order-1", 41);
    assert_eq!(checkpoint.subscription_name(), "s");
    assert_eq!(checkpoint.stream_id(), "order-1");
    assert_eq!(checkpoint.version(), 41);
    let _ = checkpoint.updated_at();

    let options = SubscriptionLoopOptions::default();
    assert_eq!(options.poll_interval(), Duration::from_millis(100));
    let options = SubscriptionLoopOptions::new(Duration::from_millis(5)).expect("nonzero interval");
    assert_eq!(options.poll_interval(), Duration::from_millis(5));
    let error = SubscriptionLoopOptions::new(Duration::ZERO).expect_err("zero interval fails");
    assert_eq!(error.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// SubscriptionRunner contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn runner_requires_an_existing_subscription() {
    let (events, subs, handler) = standard_fixture();
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let error = runner
        .run_once("missing")
        .await
        .expect_err("no such subscription");
    assert_eq!(error.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn runner_filters_streams_and_event_types_while_checkpointing() {
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "order-*").with_event_types(["OrderPlaced"]))
        .await
        .expect("save succeeds");
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let run = runner.run_once("orders").await.expect("run succeeds");

    // Three matching events across two streams; the note is filtered but
    // still advances its checkpoint.
    assert_eq!(run.streams(), 2);
    assert_eq!(run.handled(), 3);
    let mut seen = handler.seen.lock().expect("handler lock").clone();
    seen.sort_unstable();
    assert_eq!(seen, vec![0, 0, 2]);

    let checkpoint = subs
        .inner
        .load_checkpoint("orders", "order-1")
        .await
        .expect("load succeeds")
        .expect("the checkpoint exists");
    assert_eq!(
        checkpoint.version(),
        2,
        "filtered events advance checkpoints"
    );

    // A second pass finds nothing new.
    let run = runner.run_once("orders").await.expect("run succeeds");
    assert_eq!(run.handled(), 0);
    assert_eq!(run.streams(), 2);
}

#[tokio::test]
async fn runner_follows_stream_id_and_event_pages() {
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("all", "*"))
        .await
        .expect("save succeeds");
    // One stream id and one event per page force every loop branch.
    let runner = SubscriptionRunner::with_batch_size(
        &events,
        &subs,
        &handler,
        NonZeroUsize::new(1).expect("nonzero"),
    );
    let run = runner.run_once("all").await.expect("run succeeds");
    assert_eq!(run.streams(), 3);
    assert_eq!(run.handled(), 5);

    // A huge batch size is capped by the store-wide page limit.
    let capped = SubscriptionRunner::with_batch_size(
        &events,
        &subs,
        &handler,
        NonZeroUsize::new(usize::MAX).expect("nonzero"),
    );
    let run = capped.run_once("all").await.expect("run succeeds");
    assert_eq!(run.handled(), 0, "everything was already checkpointed");
}

#[tokio::test]
async fn runner_treats_a_max_checkpoint_as_terminal() {
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "order-1"))
        .await
        .expect("save succeeds");
    subs.inner
        .save_checkpoint(SubscriptionCheckpoint::new("orders", "order-1", i64::MAX))
        .await
        .expect("save succeeds");
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let run = runner.run_once("orders").await.expect("run succeeds");
    assert_eq!(run.streams(), 1);
    assert_eq!(run.handled(), 0, "the terminal checkpoint ends the stream");
}

#[tokio::test]
async fn runner_propagates_store_and_handler_errors() {
    // Handler failures surface and stop the pass.
    let (events, subs, mut handler) = standard_fixture();
    handler.fail_type = "OrderNote";
    subs.inner
        .save(PersistentSubscription::new("orders", "order-*"))
        .await
        .expect("save succeeds");
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let error = runner
        .run_once("orders")
        .await
        .expect_err("the handler fails");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Stream-id paging failures surface.
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "*"))
        .await
        .expect("save succeeds");
    events.fail_stream_ids.store(true, Ordering::SeqCst);
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let error = runner
        .run_once("orders")
        .await
        .expect_err("stream ids fail");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // Read failures surface.
    events.fail_stream_ids.store(false, Ordering::SeqCst);
    events.fail_read.store(true, Ordering::SeqCst);
    let error = runner.run_once("orders").await.expect_err("reads fail");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    events.fail_read.store(false, Ordering::SeqCst);

    // Checkpoint load failures surface.
    subs.fail_load_checkpoint.store(true, Ordering::SeqCst);
    let error = runner
        .run_once("orders")
        .await
        .expect_err("checkpoint loads fail");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    subs.fail_load_checkpoint.store(false, Ordering::SeqCst);

    // Checkpoint save failures surface.
    subs.fail_save_checkpoint.store(true, Ordering::SeqCst);
    let error = runner
        .run_once("orders")
        .await
        .expect_err("checkpoint saves fail");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn runner_loops_until_cancelled() {
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "*"))
        .await
        .expect("save succeeds");
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let options = SubscriptionLoopOptions::new(Duration::from_millis(5)).expect("nonzero interval");

    let token = CancellationToken::new();
    let run = runner.run_until_cancelled("orders", options, token.clone());
    tokio::pin!(run);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let total = loop {
        tokio::select! {
            result = &mut run => break result.expect("the loop completes"),
            _ = tokio::time::sleep(Duration::from_millis(1)) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the loop never drained the streams"
                );
                if handler.seen.lock().expect("handler lock").len() >= 5 {
                    token.cancel();
                }
            }
        }
    };
    assert_eq!(total.handled(), 5);

    // A pre-cancelled token never runs a pass.
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "*"))
        .await
        .expect("save succeeds");
    let runner = SubscriptionRunner::new(&events, &subs, &handler);
    let stopped = CancellationToken::new();
    stopped.cancel();
    let total = runner
        .run_until_cancelled("orders", options, stopped)
        .await
        .expect("cancelled run completes");
    assert_eq!(total.handled(), 0);
}

// ---------------------------------------------------------------------------
// CompetingSubscriptionRunner contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn competing_runner_honors_leases_and_processes_one_event() {
    let (events, subs, handler) = standard_fixture();
    subs.inner
        .save(PersistentSubscription::new("orders", "order-1").with_event_types(["OrderPlaced"]))
        .await
        .expect("save succeeds");

    // Another consumer holds the lease.
    subs.inner
        .try_acquire("orders", "other")
        .await
        .expect("seed lease succeeds");
    let runner = CompetingSubscriptionRunner::new(&events, &subs, &handler, "orders", "worker");
    assert!(
        runner
            .try_run_once()
            .await
            .expect("lease check succeeds")
            .is_none(),
        "a held lease yields no run"
    );
    assert!(
        runner
            .try_process_next()
            .await
            .expect("lease check succeeds")
            .is_none(),
        "a held lease yields no step"
    );
    subs.inner
        .release("orders", "other")
        .await
        .expect("release succeeds");

    // One full pass handles both matching events and releases the lease.
    let run = runner
        .try_run_once()
        .await
        .expect("run succeeds")
        .expect("the lease is free");
    assert_eq!(run.handled(), 2);
    assert!(
        subs.inner
            .try_acquire("orders", "probe")
            .await
            .expect("probe"),
        "the lease was released after the run"
    );
    subs.inner
        .release("orders", "probe")
        .await
        .expect("release succeeds");

    // Stepwise processing handles one selected event per call while filtered
    // events advance the checkpoint within the same call.
    let subs2 = FaultySubs::new();
    subs2
        .inner
        .save(PersistentSubscription::new("orders", "order-1").with_event_types(["OrderPlaced"]))
        .await
        .expect("save succeeds");
    let runner = CompetingSubscriptionRunner::new(&events, &subs2, &handler, "orders", "worker");
    let first = runner
        .try_process_next()
        .await
        .expect("step succeeds")
        .expect("the lease is free");
    assert!(first, "the first selected event is handled");
    let second = runner
        .try_process_next()
        .await
        .expect("step succeeds")
        .expect("the lease is free");
    assert!(second, "the second selected event is handled");
    let third = runner
        .try_process_next()
        .await
        .expect("step succeeds")
        .expect("the lease is free");
    assert!(!third, "no matching event remains");

    // A failing release surfaces after the run and keeps the lease held.
    subs2.fail_release.store(true, Ordering::SeqCst);
    let error = runner
        .try_run_once()
        .await
        .expect_err("the release failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert!(
        runner
            .try_run_once()
            .await
            .expect("lease check succeeds")
            .is_none(),
        "a failed release keeps the lease locked"
    );

    // Free the stuck lease, then the same fault surfaces from a step.
    subs2.fail_release.store(false, Ordering::SeqCst);
    subs2
        .inner
        .release("orders", "worker")
        .await
        .expect("manual release succeeds");
    subs2.fail_release.store(true, Ordering::SeqCst);
    let error = runner
        .try_process_next()
        .await
        .expect_err("the release failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}
