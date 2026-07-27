//! Tracing behavior integration tests.

use std::{
    collections::{BTreeSet, HashMap},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    RaftCommittedEntry, RaftMember, RaftNode, RaftRuntime, RaftRuntimeError, RaftStateMachine,
    RaftStateMachineDriver, RaftStateMachineRuntime, RaftStateMachineRuntimeError, RaftTransport,
};
use catga_core::{
    CachedResultCodec, CatgaError, CatgaResult, CircuitBreakerBehavior, Command, CommandBehavior,
    CommandHandler, CommandNext, CommandPipeline, DistributedLockBehavior, DistributedLockKey,
    Envelope, ErrorCode, EventStore, Handler, IdempotencyStore, InboxBehavior, InboxKey,
    InboxStore, LeaseStore, LoggingBehavior, MAX_PIPELINE_DEPTH, Mediator, MessageMetadata,
    MessageTransport, OutboxMessage, OutboxProcessor, OutboxStore, Pipeline, QualityOfService,
    Registry, Request, RetryBehavior, TracingBehavior, current_correlation_id,
    scope_correlation_id,
};
use catga_flow::{DslFlow, FlowDefinition, FlowRuntime, FlowStepOutcome, MemoryFlowScheduler};
use catga_memory::{
    MemoryEventStore, MemoryIdempotency, MemoryInbox, MemoryLeases, MemoryOutbox,
    MemoryPubSubTransport, MemorySuspendedFlows, MemoryTransport,
};
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use tokio::sync::oneshot;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, filter::LevelFilter, layer::SubscriberExt, registry::LookupSpan};

struct ReconcileStock;

impl catga_core::Message for ReconcileStock {}

impl Request for ReconcileStock {
    type Response = u64;
}

struct CorrelationHandler;

#[async_trait]
impl Handler<ReconcileStock> for CorrelationHandler {
    async fn handle(&self, _: ReconcileStock) -> CatgaResult<u64> {
        current_correlation_id().ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "correlation must stay visible to the handler",
            )
        })
    }
}

struct RejectReconcile;

#[async_trait]
impl Handler<ReconcileStock> for RejectReconcile {
    async fn handle(&self, _: ReconcileStock) -> CatgaResult<u64> {
        Err(CatgaError::new(ErrorCode::Validation, "stock is invalid"))
    }
}

struct RebuildInventory;

impl catga_core::Message for RebuildInventory {}
impl Command for RebuildInventory {}

struct RebuildInventoryHandler;

#[async_trait]
impl CommandHandler<RebuildInventory> for RebuildInventoryHandler {
    async fn handle(&self, _: RebuildInventory) -> CatgaResult<()> {
        Ok(())
    }
}

struct CountingReconcileHandler(Arc<AtomicUsize>);

#[async_trait]
impl Handler<ReconcileStock> for CountingReconcileHandler {
    async fn handle(&self, _: ReconcileStock) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(0)
    }
}

struct CountingRebuildInventoryHandler(Arc<AtomicUsize>);

#[async_trait]
impl CommandHandler<RebuildInventory> for CountingRebuildInventoryHandler {
    async fn handle(&self, _: RebuildInventory) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct CommandPassThrough;

#[async_trait]
impl CommandBehavior<RebuildInventory> for CommandPassThrough {
    async fn handle(
        &self,
        command: RebuildInventory,
        next: CommandNext<RebuildInventory>,
    ) -> CatgaResult<()> {
        next.run(command).await
    }
}

struct WaitForCancellation;

impl catga_core::Message for WaitForCancellation {}

impl Request for WaitForCancellation {
    type Response = ();
}

struct WaitForCancellationHandler(Arc<tokio::sync::Notify>);

#[async_trait]
impl Handler<WaitForCancellation> for WaitForCancellationHandler {
    async fn handle(&self, _: WaitForCancellation) -> CatgaResult<()> {
        self.0.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct WaitForCommandCancellation;

impl catga_core::Message for WaitForCommandCancellation {}
impl Command for WaitForCommandCancellation {}

struct WaitForCommandCancellationHandler(Arc<tokio::sync::Notify>);

#[async_trait]
impl CommandHandler<WaitForCommandCancellation> for WaitForCommandCancellationHandler {
    async fn handle(&self, _: WaitForCommandCancellation) -> CatgaResult<()> {
        self.0.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct FailingRaftTransport;

#[async_trait]
impl RaftTransport for FailingRaftTransport {
    async fn send(
        &self,
        _: catga_cluster::RaftMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(io::Error::other("transport is unavailable")))
    }
}

#[derive(Default)]
struct MetricsStateMachine;

impl RaftStateMachine for MetricsStateMachine {
    fn apply(&mut self, _: &RaftCommittedEntry) -> CatgaResult<()> {
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(catga_core::Message)]
struct TaggedReconcile {
    #[catga(trace_tag = "inventory.sku")]
    sku: &'static str,
}

impl Request for TaggedReconcile {
    type Response = ();
}

struct TaggedHandler;

#[async_trait]
impl Handler<TaggedReconcile> for TaggedHandler {
    async fn handle(&self, _: TaggedReconcile) -> CatgaResult<()> {
        Ok(())
    }
}

struct ResponseTaggedReconcile;

impl catga_core::Message for ResponseTaggedReconcile {}

struct ReconciledStock {
    available: u64,
    internal_note: &'static str,
}

impl Request for ResponseTaggedReconcile {
    type Response = ReconciledStock;

    fn visit_response_trace_tags(
        response: &Self::Response,
        visit: &mut dyn FnMut(&str, &dyn std::fmt::Display),
    ) {
        visit("inventory.available", &response.available);
    }
}

struct ResponseTaggedHandler;

#[async_trait]
impl Handler<ResponseTaggedReconcile> for ResponseTaggedHandler {
    async fn handle(&self, _: ResponseTaggedReconcile) -> CatgaResult<ReconciledStock> {
        Ok(ReconciledStock {
            available: 12,
            internal_note: "do not expose",
        })
    }
}

#[derive(Clone, Copy)]
struct RetryReconcile;

impl catga_core::Message for RetryReconcile {}

impl Request for RetryReconcile {
    type Response = ();
}

struct SucceedsAfterOneRetry(AtomicUsize);

#[async_trait]
impl Handler<RetryReconcile> for SucceedsAfterOneRetry {
    async fn handle(&self, _: RetryReconcile) -> CatgaResult<()> {
        if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(ErrorCode::Transient, "retry once"));
        }
        Ok(())
    }
}

struct AlwaysTransient;

#[async_trait]
impl Handler<RetryReconcile> for AlwaysTransient {
    async fn handle(&self, _: RetryReconcile) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Transient,
            "backend is unavailable",
        ))
    }
}

#[derive(Clone, Copy)]
struct ObservedInboxRequest;

impl catga_core::Message for ObservedInboxRequest {}

impl Request for ObservedInboxRequest {
    type Response = ();
}

impl InboxKey for ObservedInboxRequest {
    fn inbox_message_id(&self) -> u64 {
        9_901
    }
}

struct ObservedInboxHandler(Arc<AtomicUsize>);

#[async_trait]
impl Handler<ObservedInboxRequest> for ObservedInboxHandler {
    async fn handle(&self, _: ObservedInboxRequest) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct UnitResultCodec;

impl CachedResultCodec<()> for UnitResultCodec {
    fn encode(&self, _value: &()) -> CatgaResult<Arc<[u8]>> {
        Ok(Arc::from([]))
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<()> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Internal,
                "cached unit response is malformed",
            ))
        }
    }
}

struct ObservedLockWork;

impl catga_core::Message for ObservedLockWork {}

impl Request for ObservedLockWork {
    type Response = ();
}

impl DistributedLockKey for ObservedLockWork {
    fn distributed_lock_key(&self) -> Box<str> {
        "inventory:lock-observability".into()
    }
}

struct ObservedLockHandler(MetricRecorder);

#[async_trait]
impl Handler<ObservedLockWork> for ObservedLockHandler {
    async fn handle(&self, _: ObservedLockWork) -> CatgaResult<()> {
        assert_eq!(self.0.gauge("catga.distributed_lock.held|"), 1.0);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ObservedLeaseBehavior {
    Success,
    Contention,
    AcquireFailure,
    ReleaseFailure,
    OwnershipLost,
}

struct ObservedLeaseStore(ObservedLeaseBehavior);

#[async_trait]
impl LeaseStore for ObservedLeaseStore {
    async fn try_acquire(&self, _: &str, _: &str, _: Duration) -> CatgaResult<bool> {
        match self.0 {
            ObservedLeaseBehavior::Success
            | ObservedLeaseBehavior::ReleaseFailure
            | ObservedLeaseBehavior::OwnershipLost => Ok(true),
            ObservedLeaseBehavior::Contention => Ok(false),
            ObservedLeaseBehavior::AcquireFailure => Err(CatgaError::new(
                ErrorCode::Unavailable,
                "lease store is unavailable",
            )),
        }
    }

    async fn renew(&self, _: &str, _: &str, _: Duration) -> CatgaResult<bool> {
        Ok(true)
    }

    async fn release(&self, _: &str, _: &str) -> CatgaResult<bool> {
        match self.0 {
            ObservedLeaseBehavior::ReleaseFailure => Err(CatgaError::new(
                ErrorCode::Unavailable,
                "lease store is unavailable",
            )),
            ObservedLeaseBehavior::OwnershipLost => Ok(false),
            _ => Ok(true),
        }
    }
}

async fn observe_distributed_lock(
    behavior: ObservedLeaseBehavior,
    recorder: MetricRecorder,
) -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry
        .register_request::<ObservedLockWork, _>(ObservedLockHandler(recorder))
        .expect("observability handler is registered");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(DistributedLockBehavior::new(
        Arc::new(ObservedLeaseStore(behavior)),
        "observability",
        Duration::from_secs(1),
    ));
    mediator.send_with(ObservedLockWork, &pipeline).await
}

#[derive(Clone, Default)]
struct MetricRecorder {
    counters: Arc<Mutex<HashMap<String, u64>>>,
    gauges: Arc<Mutex<HashMap<String, f64>>>,
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl MetricRecorder {
    fn counter(&self, key: &str) -> u64 {
        self.counters
            .lock()
            .expect("metric recorder lock")
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    fn histogram_samples(&self, key: &str) -> usize {
        self.histograms
            .lock()
            .expect("metric recorder lock")
            .get(key)
            .map_or(0, Vec::len)
    }

    fn histogram_values(&self, key: &str) -> Vec<f64> {
        self.histograms
            .lock()
            .expect("metric recorder lock")
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    fn pipeline_metric_keys(&self) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        keys.extend(
            self.counters
                .lock()
                .expect("metric recorder lock")
                .keys()
                .filter(|key| key.starts_with("catga.pipeline."))
                .cloned(),
        );
        keys.extend(
            self.histograms
                .lock()
                .expect("metric recorder lock")
                .keys()
                .filter(|key| key.starts_with("catga.pipeline."))
                .cloned(),
        );
        keys
    }

    fn gauge(&self, key: &str) -> f64 {
        self.gauges
            .lock()
            .expect("metric recorder lock")
            .get(key)
            .copied()
            .unwrap_or_default()
    }
}

struct RecordedCounter {
    key: String,
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl CounterFn for RecordedCounter {
    fn increment(&self, value: u64) {
        *self
            .counters
            .lock()
            .expect("metric recorder lock")
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.counters
            .lock()
            .expect("metric recorder lock")
            .insert(self.key.clone(), value);
    }
}

struct RecordedGauge {
    key: String,
    gauges: Arc<Mutex<HashMap<String, f64>>>,
}

impl GaugeFn for RecordedGauge {
    fn increment(&self, value: f64) {
        *self
            .gauges
            .lock()
            .expect("metric recorder lock")
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn decrement(&self, value: f64) {
        *self
            .gauges
            .lock()
            .expect("metric recorder lock")
            .entry(self.key.clone())
            .or_default() -= value;
    }

    fn set(&self, value: f64) {
        self.gauges
            .lock()
            .expect("metric recorder lock")
            .insert(self.key.clone(), value);
    }
}

struct RecordedHistogram {
    key: String,
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl HistogramFn for RecordedHistogram {
    fn record(&self, value: f64) {
        self.histograms
            .lock()
            .expect("metric recorder lock")
            .entry(self.key.clone())
            .or_default()
            .push(value);
    }
}

impl Recorder for MetricRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecordedCounter {
            key: metric_key(key),
            counters: Arc::clone(&self.counters),
        }))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(RecordedGauge {
            key: metric_key(key),
            gauges: Arc::clone(&self.gauges),
        }))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(RecordedHistogram {
            key: metric_key(key),
            histograms: Arc::clone(&self.histograms),
        }))
    }
}

fn metric_key(key: &Key) -> String {
    let mut labels: Vec<_> = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect();
    labels.sort_unstable();
    format!("{}|{}", key.name(), labels.join(","))
}

fn assert_pipeline_metrics(
    recorder: &MetricRecorder,
    kind: &str,
    outcome: &str,
    behavior_count: f64,
) {
    let labels = format!("kind={kind},outcome={outcome}");
    assert_eq!(
        recorder.counter(&format!("catga.pipeline.executed|{labels}")),
        1
    );
    assert_eq!(
        recorder.histogram_values(&format!("catga.pipeline.behavior_count|{labels}")),
        vec![behavior_count]
    );
    let durations = recorder.histogram_values(&format!("catga.pipeline.duration|{labels}"));
    assert_eq!(durations.len(), 1);
    assert!(durations[0] >= 0.0);
}

#[test]
fn raft_node_emits_low_cardinality_leadership_and_progress_metrics() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mut node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .expect("a single-node Raft cluster is valid");

    node.campaign().expect("the single node can become leader");
    drop(guard);

    assert_eq!(recorder.gauge("catga.cluster.raft.leader.id|"), 1.0);
    assert_eq!(recorder.gauge("catga.cluster.raft.is_leader|"), 1.0);
    assert_eq!(recorder.gauge("catga.cluster.raft.role|role=leader"), 1.0);
    assert!(recorder.gauge("catga.cluster.raft.term|") >= 1.0);
    assert!(recorder.gauge("catga.cluster.raft.commit.index|") >= 1.0);
    assert!(recorder.gauge("catga.cluster.raft.apply.index|") >= 1.0);
    assert_eq!(
        recorder.counter("catga.cluster.raft.leadership.transitions|transition=acquired"),
        1
    );
}

#[test]
fn raft_node_reports_pending_commands_and_rejected_proposals() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mut node = RaftNode::new_with_pending_commit_capacity(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
        1,
    )
    .expect("a single-node Raft cluster is valid");

    assert!(node.try_propose(b"not-leader").is_err());
    node.campaign().expect("the single node can become leader");
    node.try_propose(b"apply-command")
        .expect("the leader accepts one bounded command");
    assert_eq!(recorder.gauge("catga.cluster.raft.pending_commits|"), 1.0);
    let entries = node.drain_committed();
    drop(guard);

    assert_eq!(entries.len(), 1);
    assert_eq!(recorder.gauge("catga.cluster.raft.pending_commits|"), 0.0);
    assert_eq!(
        recorder.counter("catga.cluster.raft.failures|kind=proposal"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn raft_runtime_reports_queue_depth_and_transport_failures() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let runtime = RaftRuntime::spawn(
        RaftNode::new(
            1,
            "http://node-1",
            vec![
                RaftMember::new(1, "http://node-1"),
                RaftMember::new(2, "http://node-2"),
            ],
        )
        .expect("the Raft node is valid"),
        Arc::new(FailingRaftTransport),
        Duration::from_millis(1),
    )
    .expect("the runtime starts");

    assert!(matches!(
        runtime.campaign().await,
        Err(RaftRuntimeError::Stopped)
    ));
    runtime.shutdown();
    assert!(matches!(
        runtime.join().await,
        Err(RaftRuntimeError::Transport(_))
    ));
    drop(guard);

    assert_eq!(
        recorder.gauge("catga.cluster.runtime.inbound.depth|runtime=raft"),
        0.0
    );
    assert_eq!(
        recorder.gauge("catga.cluster.runtime.command.depth|runtime=raft"),
        0.0
    );
    assert_eq!(
        recorder.counter("catga.cluster.raft.failures|kind=transport"),
        1
    );
}

#[test]
fn raft_state_machine_records_applied_commands_and_progress() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .expect("a single-node Raft cluster is valid");
    let mut driver = RaftStateMachineDriver::new(node, MetricsStateMachine)
        .expect("the state machine driver initializes");

    driver
        .campaign()
        .expect("the single node can become leader");
    driver
        .propose(b"apply-command")
        .expect("the leader accepts the application command");
    assert_eq!(
        driver
            .apply_committed()
            .expect("the state machine applies the committed command"),
        1
    );
    drop(guard);

    assert_eq!(recorder.counter("catga.cluster.raft.commands.applied|"), 1);
    assert!(recorder.gauge("catga.cluster.raft.apply.index|") >= 1.0);
}

#[tokio::test(flavor = "current_thread")]
async fn state_machine_runtime_reports_queue_depth_and_transport_failures() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let node = RaftNode::new(
        1,
        "http://node-1",
        vec![
            RaftMember::new(1, "http://node-1"),
            RaftMember::new(2, "http://node-2"),
        ],
    )
    .expect("the Raft node is valid");
    let runtime = RaftStateMachineRuntime::spawn(
        RaftStateMachineDriver::new(node, MetricsStateMachine)
            .expect("the state machine driver initializes"),
        Arc::new(FailingRaftTransport),
        Duration::from_millis(1),
    )
    .expect("the runtime starts");

    assert!(matches!(
        runtime.campaign().await,
        Err(RaftStateMachineRuntimeError::Stopped)
    ));
    runtime.shutdown();
    assert!(matches!(
        runtime.join().await,
        Err(RaftStateMachineRuntimeError::Transport(_))
    ));
    drop(guard);

    assert_eq!(
        recorder.gauge("catga.cluster.runtime.inbound.depth|runtime=state_machine"),
        0.0
    );
    assert_eq!(
        recorder.gauge("catga.cluster.runtime.command.depth|runtime=state_machine"),
        0.0
    );
    assert_eq!(
        recorder.counter("catga.cluster.raft.failures|kind=transport"),
        1
    );
}

#[derive(Clone)]
struct TraceTagLayer(Arc<Mutex<Vec<(String, String)>>>);

impl<S> Layer<S> for TraceTagLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != catga_core::TRACING_TARGET {
            return;
        }
        let mut visitor = TraceTagVisitor::default();
        event.record(&mut visitor);
        if let (Some(name), Some(value)) = (visitor.name, visitor.value) {
            self.0
                .lock()
                .expect("trace tag collector lock")
                .push((name, value));
        }
    }
}

#[derive(Default)]
struct TraceTagVisitor {
    name: Option<String>,
    value: Option<String>,
}

impl Visit for TraceTagVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.record_value(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }
}

impl TraceTagVisitor {
    fn record_value(&mut self, field: &tracing::field::Field, value: String) {
        match field.name() {
            "catga_trace_tag" => self.name = Some(value),
            "catga_trace_value" => self.value = Some(value.trim_matches('"').to_owned()),
            _ => {}
        }
    }
}

fn mediator<H>(handler: H) -> Mediator
where
    H: Handler<ReconcileStock> + 'static,
{
    let mut registry = Registry::new();
    registry
        .register_request::<ReconcileStock, _>(handler)
        .unwrap();
    Mediator::new(registry)
}

#[tokio::test]
async fn tracing_behavior_preserves_task_local_context_and_the_response() {
    let mediator = Arc::new(mediator(CorrelationHandler));
    let pipeline = Pipeline::new().with(TracingBehavior);

    let response = scope_correlation_id(19, mediator.send_with(ReconcileStock, &pipeline))
        .await
        .unwrap();

    assert_eq!(response, 19);
    assert_eq!(current_correlation_id(), None);
}

#[tokio::test]
async fn tracing_behavior_keeps_the_original_handler_error() {
    let mediator = mediator(RejectReconcile);
    let pipeline = Pipeline::new().with(TracingBehavior);

    let error = mediator
        .send_with(ReconcileStock, &pipeline)
        .await
        .expect_err("handler error must be retained");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "stock is invalid");
}

#[tokio::test(flavor = "current_thread")]
async fn mediator_pipeline_emits_bounded_metrics_without_message_type_labels() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mediator = mediator(CorrelationHandler);
    let pipeline = Pipeline::new().with(TracingBehavior);

    let response = scope_correlation_id(19, mediator.send_with(ReconcileStock, &pipeline))
        .await
        .expect("pipeline dispatch succeeds");

    assert_eq!(response, 19);
    assert_pipeline_metrics(&recorder, "request", "success", 1.0);
    assert!(
        recorder
            .pipeline_metric_keys()
            .iter()
            .all(|key| !key.contains("message_type=")),
        "pipeline metrics must not contain dynamic message type labels"
    );
    assert_eq!(
        recorder.counter("catga.requests.executed|outcome=success"),
        1
    );
    assert_eq!(
        recorder.counter("catga.commands.executed|outcome=success"),
        0
    );
    assert_eq!(
        recorder.histogram_samples("catga.request.duration|outcome=success"),
        1
    );
    assert_eq!(
        recorder.histogram_samples("catga.command.duration|outcome=success"),
        0
    );
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn command_pipeline_emits_one_bounded_success_metric_set() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mut registry = Registry::new();
    registry
        .register_command::<RebuildInventory, _>(RebuildInventoryHandler)
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let pipeline = CommandPipeline::new();

    mediator
        .send_command_with(RebuildInventory, &pipeline)
        .await
        .expect("command pipeline dispatch succeeds");

    assert_pipeline_metrics(&recorder, "command", "success", 0.0);
    assert_eq!(
        recorder.counter("catga.commands.executed|outcome=success"),
        1
    );
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn request_pipeline_failure_emits_one_bounded_failure_metric_set() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mediator = mediator(RejectReconcile);
    let pipeline = Pipeline::new();

    let error = mediator
        .send_with(ReconcileStock, &pipeline)
        .await
        .expect_err("handler fails");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_pipeline_metrics(&recorder, "request", "failure", 0.0);
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_request_pipeline_emits_one_failure_metric_set_without_invoking_its_handler() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mediator = mediator(CountingReconcileHandler(Arc::clone(&handler_calls)));
    let mut pipeline = Pipeline::new();
    for _ in 0..=MAX_PIPELINE_DEPTH {
        pipeline = pipeline.with(TracingBehavior);
    }

    let error = mediator
        .send_with(ReconcileStock, &pipeline)
        .await
        .expect_err("an oversized request pipeline is rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
    assert_pipeline_metrics(
        &recorder,
        "request",
        "failure",
        (MAX_PIPELINE_DEPTH + 1) as f64,
    );
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_command_pipeline_emits_one_failure_metric_set_without_invoking_its_handler() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_command::<RebuildInventory, _>(CountingRebuildInventoryHandler(Arc::clone(
            &handler_calls,
        )))
        .expect("command handler registers");
    let mediator = Mediator::new(registry);
    let mut pipeline = CommandPipeline::new();
    for _ in 0..=MAX_PIPELINE_DEPTH {
        pipeline = pipeline.with(CommandPassThrough);
    }

    let error = mediator
        .send_command_with(RebuildInventory, &pipeline)
        .await
        .expect_err("an oversized command pipeline is rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
    assert_pipeline_metrics(
        &recorder,
        "command",
        "failure",
        (MAX_PIPELINE_DEPTH + 1) as f64,
    );
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_request_and_command_pipelines_emit_one_aborted_metric_set_each() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let request_started = Arc::new(tokio::sync::Notify::new());
    let mut request_registry = Registry::new();
    request_registry
        .register_request::<WaitForCancellation, _>(WaitForCancellationHandler(Arc::clone(
            &request_started,
        )))
        .expect("request handler registers");
    let request_mediator = Mediator::new(request_registry);
    let request_pipeline = Pipeline::new().with(TracingBehavior);
    let request_cancellation = tokio_util::sync::CancellationToken::new();
    let request_canceller = request_cancellation.clone();
    let request_cancel_task = tokio::spawn(async move {
        request_started.notified().await;
        request_canceller.cancel();
    });

    let request_error = request_mediator
        .send_with_cancellation_and_pipeline(
            WaitForCancellation,
            &request_pipeline,
            request_cancellation,
        )
        .await
        .expect_err("in-flight request is aborted");
    request_cancel_task
        .await
        .expect("request cancellation task joins");

    let command_started = Arc::new(tokio::sync::Notify::new());
    let mut registry = Registry::new();
    registry
        .register_command::<WaitForCommandCancellation, _>(WaitForCommandCancellationHandler(
            Arc::clone(&command_started),
        ))
        .expect("command handler registers");
    let command_mediator = Mediator::new(registry);
    let command_pipeline = CommandPipeline::new();
    let command_cancellation = tokio_util::sync::CancellationToken::new();
    let command_canceller = command_cancellation.clone();
    let command_cancel_task = tokio::spawn(async move {
        command_started.notified().await;
        command_canceller.cancel();
    });
    let command_error = command_mediator
        .send_command_with_cancellation_and_pipeline(
            WaitForCommandCancellation,
            &command_pipeline,
            command_cancellation,
        )
        .await
        .expect_err("in-flight command is aborted");
    command_cancel_task
        .await
        .expect("command cancellation task joins");

    assert_eq!(request_error.code(), ErrorCode::Cancelled);
    assert_eq!(command_error.code(), ErrorCode::Cancelled);
    assert_pipeline_metrics(&recorder, "request", "aborted", 1.0);
    assert_pipeline_metrics(&recorder, "command", "aborted", 0.0);
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_tracing_behaviors_do_not_duplicate_pipeline_metrics() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let mediator = mediator(CorrelationHandler);
    let pipeline = Pipeline::new().with(TracingBehavior).with(TracingBehavior);

    let response = scope_correlation_id(19, mediator.send_with(ReconcileStock, &pipeline))
        .await
        .expect("pipeline dispatch succeeds");

    assert_eq!(response, 19);
    assert_pipeline_metrics(&recorder, "request", "success", 2.0);
    drop(guard);
}

#[tokio::test]
async fn logging_behavior_keeps_the_original_handler_error() {
    let mediator = mediator(RejectReconcile);
    let pipeline = Pipeline::new().with(LoggingBehavior);

    let error = mediator
        .send_with(ReconcileStock, &pipeline)
        .await
        .expect_err("logging must not replace a handler error");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "stock is invalid");
}

#[tokio::test(flavor = "current_thread")]
async fn mediator_records_opted_in_message_tags_as_structured_tracing_events() {
    let tags = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(TraceTagLayer(Arc::clone(&tags)));
    let guard = tracing::subscriber::set_default(subscriber);
    let mut registry = Registry::new();
    registry
        .register_request::<TaggedReconcile, _>(TaggedHandler)
        .expect("one typed handler can be registered");
    let mediator = Mediator::new(registry);

    let disabled =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(LevelFilter::OFF));
    mediator
        .send(TaggedReconcile { sku: "sku-42" })
        .await
        .expect("tagged request succeeds with tracing disabled");
    drop(disabled);

    mediator
        .send(TaggedReconcile { sku: "sku-42" })
        .await
        .expect("tagged request succeeds");
    drop(guard);

    assert_eq!(
        *tags.lock().expect("trace tag collector lock"),
        [("inventory.sku".to_owned(), "sku-42".to_owned())]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn mediator_records_only_opted_in_successful_response_tags_as_structured_tracing_events() {
    let recorder = MetricRecorder::default();
    let metrics_guard = metrics::set_default_local_recorder(&recorder);
    let tags = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(TraceTagLayer(Arc::clone(&tags)));
    let guard = tracing::subscriber::set_default(subscriber);
    let mut registry = Registry::new();
    registry
        .register_request::<ResponseTaggedReconcile, _>(ResponseTaggedHandler)
        .expect("one typed handler can be registered");
    let mediator = Mediator::new(registry);

    let response = mediator
        .send(ResponseTaggedReconcile)
        .await
        .expect("tagged response request succeeds");
    drop(guard);
    drop(metrics_guard);

    assert_eq!(response.available, 12);
    assert_eq!(response.internal_note, "do not expose");
    assert_eq!(
        *tags.lock().expect("trace tag collector lock"),
        [("inventory.available".to_owned(), "12".to_owned())]
    );
    assert_eq!(
        recorder.counter("catga.requests.executed|outcome=success"),
        1
    );
    assert!(
        recorder.counter("catga.requests.executed|inventory.available=12,outcome=success") == 0,
        "response values must never become metrics labels"
    );
}

#[test]
fn telemetry_operation_records_a_completed_outcome_once() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let result: CatgaResult<()> = Ok(());
    let mut operation =
        catga_core::telemetry::persistence_operation("memory", "event_store", "append");

    operation.complete(&result);
    operation.complete(&result);
    drop(operation);
    drop(guard);

    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=event_store,operation=append,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.histogram_samples(
            "catga.persistence.duration|backend=memory,component=event_store,operation=append,outcome=success"
        ),
        1
    );
}

#[test]
fn telemetry_operation_records_an_aborted_outcome_on_drop() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let operation = catga_core::telemetry::persistence_operation("memory", "event_store", "read");

    drop(operation);
    drop(guard);

    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=event_store,operation=read,outcome=aborted"
        ),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn telemetry_record_persistence_preserves_an_async_failure() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let result =
        catga_core::telemetry::record_persistence("redis", "event_store", "append", async {
            Err::<(), _>(CatgaError::new(ErrorCode::Conflict, "version conflict"))
        })
        .await;
    drop(guard);

    let error = result.expect_err("the original failure is retained");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=redis,component=event_store,operation=append,outcome=failure"
        ),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn telemetry_message_publish_records_success_and_failure_without_changing_results() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let successful = catga_core::telemetry::record_message_publish("memory", "queue", async {
        Ok::<_, CatgaError>(())
    })
    .await;
    let failed = catga_core::telemetry::record_message_publish("memory", "queue", async {
        Err::<(), _>(CatgaError::new(ErrorCode::Unavailable, "receiver closed"))
    })
    .await;
    drop(guard);

    assert!(successful.is_ok());
    assert_eq!(
        failed.expect_err("publish failure is retained").code(),
        ErrorCode::Unavailable
    );
    assert_eq!(
        recorder.counter("catga.messages.published|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.counter("catga.messages.failed|backend=memory,mode=queue"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn telemetry_message_receive_records_success_failure_and_abort_without_changing_results() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let successful = catga_core::telemetry::record_message_receive("memory", "queue", async {
        Ok::<_, CatgaError>(())
    })
    .await;
    let failed = catga_core::telemetry::record_message_receive("memory", "queue", async {
        Err::<(), _>(CatgaError::new(ErrorCode::Transient, "receiver closed"))
    })
    .await;
    let mut aborted = Box::pin(catga_core::telemetry::record_message_receive(
        "memory",
        "queue",
        std::future::pending::<CatgaResult<()>>(),
    ));
    tokio::select! {
        _ = &mut aborted => panic!("pending receive unexpectedly completed"),
        _ = tokio::task::yield_now() => {}
    }
    drop(aborted);
    drop(guard);

    assert!(successful.is_ok());
    assert_eq!(
        failed.expect_err("receive failure is retained").code(),
        ErrorCode::Transient
    );
    assert_eq!(
        recorder.counter("catga.messages.received|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.counter("catga.messages.receive.failed|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.counter("catga.messages.receive.aborted|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.histogram_samples(
            "catga.messages.receive.duration|backend=memory,mode=queue,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.histogram_samples(
            "catga.messages.receive.duration|backend=memory,mode=queue,outcome=failure"
        ),
        1
    );
    assert_eq!(
        recorder.histogram_samples(
            "catga.messages.receive.duration|backend=memory,mode=queue,outcome=aborted"
        ),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_transport_publish_reports_queue_and_pubsub_metrics() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let queue = MemoryTransport::new(1).expect("positive queue capacity");
    let pubsub = MemoryPubSubTransport::new(1).expect("positive Pub/Sub capacity");

    queue
        .publish(Envelope::new(
            91,
            "inventory.queued",
            vec![9, 1],
            MessageMetadata::new(91, None),
        ))
        .await
        .expect("queue publication succeeds");
    pubsub
        .publish(Envelope::new(
            92,
            "inventory.broadcast",
            vec![9, 2],
            MessageMetadata::new(92, None).with_quality_of_service(QualityOfService::AtMostOnce),
        ))
        .await
        .expect("Pub/Sub publication succeeds");
    drop(guard);

    assert_eq!(
        recorder.counter("catga.messages.published|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.counter("catga.messages.published|backend=memory,mode=pubsub"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn memory_transport_receive_reports_queue_and_pubsub_outcomes() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let queue = MemoryTransport::new(1).expect("positive queue capacity");
    let pubsub = MemoryPubSubTransport::new(1).expect("positive Pub/Sub capacity");
    let slow_subscriber = pubsub.subscribe();

    queue
        .publish(Envelope::new(
            93,
            "inventory.queued",
            vec![9, 3],
            MessageMetadata::new(93, None),
        ))
        .await
        .expect("queue publication succeeds");
    queue.receive().await.expect("queue receive succeeds");

    for id in [94, 95] {
        pubsub
            .publish(Envelope::new(
                id,
                "inventory.broadcast",
                vec![9, 4],
                MessageMetadata::new(id, None)
                    .with_quality_of_service(QualityOfService::AtMostOnce),
            ))
            .await
            .expect("Pub/Sub publication succeeds");
    }
    let error = slow_subscriber
        .receive()
        .await
        .expect_err("slow subscriber must report lag");
    drop(guard);

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(
        recorder.counter("catga.messages.received|backend=memory,mode=queue"),
        1
    );
    assert_eq!(
        recorder.counter("catga.messages.receive.failed|backend=memory,mode=pubsub"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn flow_runtime_records_terminal_and_step_metrics() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let successful = FlowRuntime::new(
        Arc::clone(&store),
        Arc::clone(&scheduler),
        FlowDefinition::new("metrics-success")
            .step("reserve", |_| async { Ok(FlowStepOutcome::Advance) })
            .step("finish", |_| async { Ok(FlowStepOutcome::Complete) }),
        "metrics-worker",
    );
    let failing = FlowRuntime::new(
        store,
        scheduler,
        FlowDefinition::new("metrics-failure").step("reject", |_| async {
            Err(CatgaError::new(ErrorCode::Validation, "flow step rejected"))
        }),
        "metrics-worker",
    );

    assert!(
        successful
            .start("metrics-success/1", [])
            .await
            .expect("successful flow completes")
            .is_success()
    );
    assert!(
        failing
            .start("metrics-failure/1", [])
            .await
            .expect("failing flow persists its terminal state")
            .is_failure()
    );
    drop(guard);

    assert_eq!(recorder.counter("catga.flow.started|"), 2);
    assert_eq!(recorder.counter("catga.flow.completed|"), 1);
    assert_eq!(recorder.counter("catga.flow.failed|"), 1);
    assert_eq!(recorder.counter("catga.flow.step.executed|"), 3);
    assert_eq!(recorder.counter("catga.flow.step.succeeded|"), 2);
    assert_eq!(recorder.counter("catga.flow.step.failed|"), 1);
    assert_eq!(
        recorder.histogram_samples("catga.flow.latency|outcome=success"),
        1
    );
    assert_eq!(
        recorder.histogram_samples("catga.flow.latency|outcome=failure"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dsl_flow_records_terminal_and_step_metrics() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let successful = DslFlow::new().action(|state: &mut u32| {
        Box::pin(async move {
            *state = 1;
            Ok(())
        })
    });
    let failing = DslFlow::new().action(|state: &mut u32| {
        Box::pin(async move {
            let _ = state;
            Err(CatgaError::new(ErrorCode::Validation, "DSL flow rejected"))
        })
    });
    let mut successful_state = 0;
    let mut failing_state = 0;

    successful
        .run(&mut successful_state)
        .await
        .expect("successful DSL flow completes");
    assert_eq!(
        failing
            .run(&mut failing_state)
            .await
            .expect_err("failing DSL flow returns its error")
            .code(),
        ErrorCode::Validation
    );
    drop(guard);

    assert_eq!(recorder.counter("catga.flow.started|"), 2);
    assert_eq!(recorder.counter("catga.flow.completed|"), 1);
    assert_eq!(recorder.counter("catga.flow.failed|"), 1);
    assert_eq!(recorder.counter("catga.flow.step.executed|"), 2);
    assert_eq!(recorder.counter("catga.flow.step.succeeded|"), 1);
    assert_eq!(recorder.counter("catga.flow.step.failed|"), 1);
    assert_eq!(
        recorder.histogram_samples("catga.flow.duration|outcome=success"),
        1
    );
    assert_eq!(
        recorder.histogram_samples("catga.flow.duration|outcome=failure"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn flow_runtime_cancellation_releases_active_execution_metric() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);
    let (entered_sender, entered_receiver) = oneshot::channel();
    let entered_sender = Arc::new(Mutex::new(Some(entered_sender)));
    let runtime = FlowRuntime::new(
        Arc::new(MemorySuspendedFlows::default()),
        Arc::new(MemoryFlowScheduler::default()),
        FlowDefinition::new("metrics-cancelled").step("wait", move |_| {
            let entered_sender = Arc::clone(&entered_sender);
            async move {
                if let Some(sender) = entered_sender
                    .lock()
                    .expect("cancellation test sender lock")
                    .take()
                {
                    let _ = sender.send(());
                }
                std::future::pending::<CatgaResult<FlowStepOutcome>>().await
            }
        }),
        "metrics-worker",
    );
    let mut start = Box::pin(runtime.start("metrics-cancelled/1", []));

    tokio::select! {
        _ = entered_receiver => {}
        result = &mut start => panic!("pending flow unexpectedly completed: {result:?}"),
    }
    assert_eq!(recorder.gauge("catga.flow.active|"), 1.0);
    drop(start);
    drop(guard);

    assert_eq!(recorder.gauge("catga.flow.active|"), 0.0);
    assert_eq!(
        recorder.histogram_samples("catga.flow.duration|outcome=aborted"),
        1
    );
}

#[tokio::test(flavor = "current_thread")]
async fn durable_behavior_metrics_count_completed_deliveries_retries_and_circuit_openings() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);

    let outbox = Arc::new(MemoryOutbox::default());
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            48,
            "inventory.updated",
            vec![4, 8],
            MessageMetadata::new(48, None),
        )))
        .await
        .expect("outbox accepts a unique message");
    let processor = OutboxProcessor::new(
        Arc::clone(&outbox),
        Arc::new(MemoryTransport::new(1).expect("positive queue capacity")),
        "worker-a",
        1,
    )
    .expect("valid processor configuration");
    assert_eq!(
        processor
            .flush_once()
            .await
            .expect("outbox delivery succeeds")
            .published(),
        1
    );

    let mut retry_registry = Registry::new();
    retry_registry
        .register_request::<RetryReconcile, _>(SucceedsAfterOneRetry(AtomicUsize::new(0)))
        .expect("one retry handler is accepted");
    let retry_mediator = Mediator::new(retry_registry);
    let retry_pipeline = Pipeline::new().with(RetryBehavior::new(1, Duration::ZERO));
    retry_mediator
        .send_with(RetryReconcile, &retry_pipeline)
        .await
        .expect("second attempt succeeds");

    let mut circuit_registry = Registry::new();
    circuit_registry
        .register_request::<RetryReconcile, _>(AlwaysTransient)
        .expect("one circuit handler is accepted");
    let circuit_mediator = Mediator::new(circuit_registry);
    let circuit_pipeline = Pipeline::new().with(
        CircuitBreakerBehavior::new(1, Duration::from_secs(1))
            .expect("valid circuit breaker configuration"),
    );
    assert!(
        circuit_mediator
            .send_with(RetryReconcile, &circuit_pipeline)
            .await
            .is_err()
    );
    assert!(
        circuit_mediator
            .send_with(RetryReconcile, &circuit_pipeline)
            .await
            .is_err()
    );
    drop(guard);

    assert_eq!(recorder.counter("catga.outbox.published|"), 1);
    assert_eq!(recorder.counter("catga.resilience.retries|"), 1);
    assert_eq!(recorder.counter("catga.resilience.circuit.opened|"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn distributed_lock_metrics_record_acquisition_release_and_held_lock_outcomes() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);

    observe_distributed_lock(ObservedLeaseBehavior::Success, recorder.clone())
        .await
        .expect("owned lock is released successfully");
    assert_eq!(
        observe_distributed_lock(ObservedLeaseBehavior::Contention, recorder.clone())
            .await
            .expect_err("contended lock is rejected")
            .code(),
        ErrorCode::Conflict
    );
    assert_eq!(
        observe_distributed_lock(ObservedLeaseBehavior::AcquireFailure, recorder.clone())
            .await
            .expect_err("acquisition error is preserved")
            .code(),
        ErrorCode::Unavailable
    );
    assert_eq!(
        observe_distributed_lock(ObservedLeaseBehavior::ReleaseFailure, recorder.clone())
            .await
            .expect_err("release error is preserved")
            .code(),
        ErrorCode::Unavailable
    );
    assert_eq!(
        observe_distributed_lock(ObservedLeaseBehavior::OwnershipLost, recorder.clone())
            .await
            .expect_err("lost ownership is reported")
            .code(),
        ErrorCode::Internal
    );
    drop(guard);

    assert_eq!(
        recorder.histogram_samples("catga.distributed_lock.acquire.duration|outcome=success"),
        3
    );
    assert_eq!(
        recorder.histogram_samples("catga.distributed_lock.acquire.duration|outcome=contention"),
        1
    );
    assert_eq!(
        recorder.histogram_samples("catga.distributed_lock.acquire.duration|outcome=failure"),
        1
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.acquire.outcomes|outcome=success"),
        3
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.acquire.outcomes|outcome=contention"),
        1
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.acquire.outcomes|outcome=failure"),
        1
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.release.outcomes|outcome=success"),
        1
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.release.outcomes|outcome=failure"),
        1
    );
    assert_eq!(
        recorder.counter("catga.distributed_lock.release.outcomes|outcome=ownership_lost"),
        1
    );
    assert_eq!(recorder.gauge("catga.distributed_lock.held|"), 0.0);
}

#[tokio::test(flavor = "current_thread")]
async fn cqrs_observability_metrics_record_retry_backlog_and_inbox_hits() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);

    let mut retry_registry = Registry::new();
    retry_registry
        .register_request::<RetryReconcile, _>(SucceedsAfterOneRetry(AtomicUsize::new(0)))
        .expect("one retry handler is accepted");
    let retry_mediator = Mediator::new(retry_registry);
    let retry_pipeline = Pipeline::new().with(RetryBehavior::new(1, Duration::from_millis(50)));
    let mut retry = Box::pin(retry_mediator.send_with(RetryReconcile, &retry_pipeline));
    tokio::select! {
        result = &mut retry => panic!("retry completed before its configured backoff: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }
    assert_eq!(recorder.gauge("catga.resilience.retry.pending|"), 1.0);
    retry.await.expect("retry completes after backoff");
    assert_eq!(recorder.gauge("catga.resilience.retry.pending|"), 0.0);

    let mut cancelled_retry_registry = Registry::new();
    cancelled_retry_registry
        .register_request::<RetryReconcile, _>(AlwaysTransient)
        .expect("transient retry handler is accepted");
    let cancelled_retry_mediator = Mediator::new(cancelled_retry_registry);
    let mut cancelled_retry =
        Box::pin(cancelled_retry_mediator.send_with(RetryReconcile, &retry_pipeline));
    tokio::select! {
        result = &mut cancelled_retry => panic!("retry completed before its configured backoff: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }
    assert_eq!(recorder.gauge("catga.resilience.retry.pending|"), 1.0);
    drop(cancelled_retry);
    assert_eq!(recorder.gauge("catga.resilience.retry.pending|"), 0.0);

    let handler_calls = Arc::new(AtomicUsize::new(0));
    let mut inbox_registry = Registry::new();
    inbox_registry
        .register_request::<ObservedInboxRequest, _>(ObservedInboxHandler(Arc::clone(
            &handler_calls,
        )))
        .expect("inbox handler is accepted");
    let inbox_mediator = Mediator::new(inbox_registry);
    let inbox_pipeline = Pipeline::new().with(InboxBehavior::new(
        Arc::new(MemoryInbox::default()),
        UnitResultCodec,
    ));
    inbox_mediator
        .send_with(ObservedInboxRequest, &inbox_pipeline)
        .await
        .expect("first inbox request is processed");
    inbox_mediator
        .send_with(ObservedInboxRequest, &inbox_pipeline)
        .await
        .expect("second inbox request uses the cache");
    drop(guard);

    assert_eq!(handler_calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        recorder.counter("catga.inbox.outcomes|outcome=processed"),
        1
    );
    assert_eq!(recorder.counter("catga.inbox.outcomes|outcome=hit"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn memory_store_operations_emit_bounded_backend_component_and_outcome_labels() {
    let recorder = MetricRecorder::default();
    let guard = metrics::set_default_local_recorder(&recorder);

    let events = MemoryEventStore::default();
    let envelope = Envelope::new(
        73,
        "order.created",
        vec![7, 3],
        MessageMetadata::new(73, None),
    );
    events
        .append("order-73", vec![envelope], None)
        .await
        .expect("nonempty event batch succeeds");
    let conflicting = Envelope::new(
        74,
        "order.updated",
        vec![7, 4],
        MessageMetadata::new(74, None),
    );
    assert!(
        events
            .append("order-73", vec![conflicting], Some(-1))
            .await
            .is_err()
    );
    assert_eq!(
        events
            .append("order-73", Vec::new(), None)
            .await
            .expect("empty event batch is a successful no-op"),
        0
    );
    events
        .read_page("order-73", 0, 8)
        .await
        .expect("event read succeeds");
    events
        .version("order-73")
        .await
        .expect("event version succeeds");
    events
        .read_to_version_page("order-73", 0, 0, 8)
        .await
        .expect("historical event read succeeds");
    events
        .read_to_time_page("order-73", 0, std::time::SystemTime::now(), 8)
        .await
        .expect("time-bounded event read succeeds");
    events
        .version_history_page("order-73", 0, 8)
        .await
        .expect("version history succeeds");
    events
        .stream_ids_page(None, 8)
        .await
        .expect("stream enumeration succeeds");

    let inbox = MemoryInbox::default();
    let claim = inbox
        .try_claim(81)
        .await
        .expect("inbox claim succeeds")
        .expect("inbox claim is acquired");
    inbox
        .complete(claim, None)
        .await
        .expect("inbox completion succeeds");
    inbox.state(81).await.expect("inbox state succeeds");
    inbox.result(81).await.expect("inbox result succeeds");
    let claim = inbox
        .try_claim(82)
        .await
        .expect("second inbox claim succeeds")
        .expect("second inbox claim is acquired");
    inbox
        .fail(claim)
        .await
        .expect("inbox failure transition succeeds");

    let idempotency = MemoryIdempotency::default();
    assert!(
        idempotency
            .try_claim("order-83")
            .await
            .expect("idempotency claim succeeds")
    );
    idempotency
        .complete("order-83", None)
        .await
        .expect("idempotency completion succeeds");
    idempotency
        .state("order-83")
        .await
        .expect("idempotency state succeeds");
    idempotency
        .result("order-83")
        .await
        .expect("idempotency result succeeds");
    assert!(
        idempotency
            .try_claim("order-84")
            .await
            .expect("second idempotency claim succeeds")
    );
    idempotency
        .fail("order-84")
        .await
        .expect("idempotency failure transition succeeds");

    let outbox = MemoryOutbox::default();
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            85,
            "order.ready",
            vec![8, 5],
            MessageMetadata::new(85, None),
        )))
        .await
        .expect("outbox enqueue succeeds");
    let published_claim = outbox
        .claim("worker-a", 1)
        .await
        .expect("outbox claim succeeds")
        .pop()
        .expect("outbox claim returns a message");
    outbox
        .ack("worker-a", 85, published_claim.claim_token().unwrap())
        .await
        .expect("outbox acknowledgement succeeds");
    outbox
        .enqueue(OutboxMessage::new(Envelope::new(
            86,
            "order.retry",
            vec![8, 6],
            MessageMetadata::new(86, None),
        )))
        .await
        .expect("second outbox enqueue succeeds");
    let retry_claim = outbox
        .claim("worker-a", 1)
        .await
        .expect("second outbox claim succeeds")
        .pop()
        .expect("second outbox claim returns a message");
    outbox
        .release("worker-a", 86, retry_claim.claim_token().unwrap())
        .await
        .expect("outbox release succeeds");
    assert!(outbox.cancel(86).await.expect("outbox cancel succeeds"));

    let leases = MemoryLeases::default();
    assert!(
        leases
            .try_acquire("outbox", "worker-a", Duration::from_secs(1))
            .await
            .expect("lease acquisition succeeds")
    );
    assert!(
        !leases
            .try_acquire("outbox", "worker-b", Duration::from_secs(1))
            .await
            .expect("owned lease reports contention without failing")
    );
    assert!(
        leases
            .renew("outbox", "worker-a", Duration::from_secs(1))
            .await
            .expect("lease renewal succeeds")
    );
    assert!(
        leases
            .release("outbox", "worker-a")
            .await
            .expect("lease release succeeds")
    );
    drop(guard);

    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=event_store,operation=append,outcome=success"
        ),
        2
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=event_store,operation=append,outcome=failure"
        ),
        1
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.conflicts|backend=memory,component=event_store,operation=append"
        ),
        1
    );
    for operation in [
        "read_page",
        "version",
        "read_to_version_page",
        "read_to_time_page",
        "version_history_page",
        "stream_ids_page",
    ] {
        assert_eq!(
            recorder.counter(&format!(
                "catga.persistence.operations|backend=memory,component=event_store,operation={operation},outcome=success"
            )),
            1,
            "{operation} must emit one successful memory event-store observation"
        );
    }
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=inbox,operation=complete,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=idempotency,operation=complete,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=outbox,operation=release,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.operations|backend=memory,component=lease,operation=renew,outcome=success"
        ),
        1
    );
    assert_eq!(
        recorder.counter(
            "catga.persistence.contention|backend=memory,component=lease,operation=try_acquire"
        ),
        1
    );
}
