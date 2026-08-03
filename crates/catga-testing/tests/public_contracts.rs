//! Direct public-contract coverage for the Catga testing helpers.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    Aggregate, CatgaError, CatgaResult, Envelope, ErrorCode, Event, EventHandler, Handler,
    MessageMetadata, Request,
};
use catga_flow::{FlowDefinition, FlowRuntime, FlowStatus, FlowStepOutcome, SuspendedFlowStore};
use catga_testing::{
    AggregateScenario, CatgaTestHarness, EventHandlerSpy, FlowTestContext, HandlerSpy,
    MessageCapture, assert_contains, assert_error_code, assert_failure, assert_success,
    assert_value,
};

#[derive(Clone, Debug, Eq, PartialEq, catga_core::Message)]
struct Double(u32);

impl Request for Double {
    type Response = u32;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, request: Double) -> CatgaResult<u32> {
        Ok(request.0.saturating_mul(2))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, catga_core::Message)]
struct Doubled(u32);

impl Event for Doubled {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct EventCounter(Arc<std::sync::atomic::AtomicU32>);

#[async_trait]
impl EventHandler<Doubled> for EventCounter {
    async fn handle(&self, event: Doubled) -> CatgaResult<()> {
        self.0
            .fetch_add(event.0, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn spies_record_requests_and_events_while_preserving_handlers() {
    let request_spy = HandlerSpy::new(DoubleHandler);
    assert_eq!(
        request_spy
            .handle(Double(21))
            .await
            .expect("request spy delegates to its handler"),
        42
    );
    assert_eq!(request_spy.calls(), [Double(21)]);

    let total = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let event_spy = EventHandlerSpy::with_handler(EventCounter(Arc::clone(&total)));
    event_spy
        .handle(Doubled(7))
        .await
        .expect("event spy delegates to its handler");
    assert_eq!(event_spy.calls(), [Doubled(7)]);
    assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 7);
}

#[tokio::test]
async fn spy_actions_and_message_capture_preserve_assertion_data() {
    let action_spy =
        HandlerSpy::<Double, _>::with_action(|request| async move { Ok(request.0 + 1) });
    assert_eq!(
        action_spy
            .handle(Double(8))
            .await
            .expect("action spy returns its result"),
        9
    );
    assert_eq!(action_spy.last_call(), Some(Double(8)));

    let missing_spy = HandlerSpy::<Double, _>::without_handler();
    let error = missing_spy
        .handle(Double(3))
        .await
        .expect_err("missing handler reports not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(missing_spy.calls(), [Double(3)]);

    let action_total = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let event_spy = EventHandlerSpy::<Doubled>::with_action({
        let action_total = Arc::clone(&action_total);
        move |event| {
            let action_total = Arc::clone(&action_total);
            async move {
                action_total.fetch_add(event.0, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
        }
    });
    event_spy
        .handle(Doubled(5))
        .await
        .expect("action event spy accepts the event");
    assert_eq!(action_total.load(std::sync::atomic::Ordering::Relaxed), 5);

    let capture = MessageCapture::default();
    capture.record_published("created");
    capture.record_consumed("handled");
    assert_eq!(capture.published(), ["created"]);
    assert_eq!(capture.consumed(), ["handled"]);
    capture.clear();
    assert!(capture.published().is_empty());
    assert_eq!(assert_success(Ok(3_u32)), 3);
    assert_eq!(assert_value(Ok(4_u32), 4), 4);
    let validation = CatgaError::new(ErrorCode::Validation, "invalid");
    assert_eq!(
        assert_failure::<()>(Err(validation)).code(),
        ErrorCode::Validation
    );
    assert_eq!(
        assert_error_code::<()>(
            Err(CatgaError::new(ErrorCode::Timeout, "late")),
            ErrorCode::Timeout,
        )
        .code(),
        ErrorCode::Timeout
    );
    assert_eq!(
        assert_contains([1_u32, 2, 3], |value| *value % 2 == 1),
        [1, 3]
    );
}

#[tokio::test]
async fn harness_dispatches_and_captures_typed_messages() {
    let mut harness = CatgaTestHarness::new().expect("test harness constructs");
    harness
        .register_captured_request::<Double, _>(DoubleHandler)
        .expect("request handler registers");
    harness.capture_event::<Doubled>();
    let running = harness.start();

    assert_eq!(
        running
            .mediator()
            .send(Double(9))
            .await
            .expect("harness dispatches request"),
        18
    );
    running
        .mediator()
        .publish(Doubled(18))
        .await
        .expect("harness publishes event");
    assert_eq!(running.consumed_of::<Double>(), [Double(9)]);
    assert_eq!(running.published_of::<Doubled>(), [Doubled(18)]);
    running.clear_captures();
    assert!(running.consumed_of::<Double>().is_empty());
    assert!(running.published_of::<Doubled>().is_empty());
}

#[derive(Clone)]
struct Balance {
    id: Box<str>,
    version: i64,
    total: u64,
    pending: Vec<Envelope>,
}

impl Aggregate for Balance {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            version: -1,
            total: 0,
            pending: Vec::new(),
        }
    }

    fn stream_id(id: &str) -> Box<str> {
        format!("balance:{id}").into()
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn apply(&mut self, event: &Envelope) -> CatgaResult<()> {
        self.total += u64::from(event.payload()[0]);
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

#[tokio::test]
async fn aggregate_and_flow_context_support_deterministic_replays() {
    let history = vec![Envelope::new(
        1,
        "balance.credited",
        vec![5],
        MessageMetadata::new(1, None),
    )];
    let replay = AggregateScenario::<Balance>::new("account-42")
        .expect("aggregate scenario constructs")
        .replay(&history)
        .await
        .expect("history replays");
    assert_eq!(replay.aggregate().total, 5);
    replay
        .assert_version(0)
        .expect("replayed version matches history");
    replay
        .assert_events(&history)
        .expect("replayed events match history");
    assert_eq!(
        replay
            .assert_version(1)
            .expect_err("incorrect version is rejected")
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        AggregateScenario::<Balance>::new("")
            .err()
            .expect("an empty aggregate id is invalid")
            .code(),
        ErrorCode::Validation
    );

    let context = FlowTestContext::new();
    let runtime = FlowRuntime::new(
        context.suspended_flows(),
        context.scheduler(),
        FlowDefinition::new("contract-flow")
            .step("complete", |_| async { Ok(FlowStepOutcome::complete()) }),
        "test-node",
    );
    assert!(
        runtime
            .start("flow-1", vec![])
            .await
            .expect("flow runtime starts")
            .is_success()
    );
    assert_eq!(
        context
            .suspended_flows()
            .get("flow-1")
            .await
            .expect("suspended flow store reads")
            .expect("completed flow state is stored")
            .state()
            .status(),
        FlowStatus::Done
    );
}
