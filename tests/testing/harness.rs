//! Reusable mediator test harness components.

use async_trait::async_trait;
use catga_core::{CatgaResult, Event, EventHandler, Handler, Request};
use catga_core::flow::{FlowDefinition, FlowRuntime, FlowStepOutcome};
use catga_testing::CatgaTestHarness;

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

#[derive(Clone, catga_core::Message, Debug, Eq, PartialEq)]
struct Doubled(u32);

impl Event for Doubled {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Noop;

#[async_trait]
impl EventHandler<Doubled> for Noop {
    async fn handle(&self, _: Doubled) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_harness_runs_typed_handlers_and_captures_selected_messages() {
    let mut harness = CatgaTestHarness::new().unwrap();
    harness
        .register_captured_request::<Double, _>(DoubleHandler)
        .unwrap();
    harness.register_event::<Doubled, _>(Noop);
    let running = harness.start();

    assert_eq!(running.mediator().send(Double(21)).await.unwrap(), 42);
    running.mediator().publish(Doubled(42)).await.unwrap();

    assert_eq!(running.consumed_of::<Double>(), [Double(21)]);
    assert_eq!(running.published_of::<Doubled>(), [Doubled(42)]);
}

#[tokio::test]
async fn test_harness_captures_one_published_event_when_multiple_handlers_are_registered() {
    let mut harness = CatgaTestHarness::new().unwrap();
    harness.register_event::<Doubled, _>(Noop);
    harness.register_event::<Doubled, _>(Noop);
    let running = harness.start();

    running.mediator().publish(Doubled(42)).await.unwrap();

    assert_eq!(running.published_of::<Doubled>(), [Doubled(42)]);
}

#[tokio::test]
async fn test_harness_exposes_durable_flow_test_infrastructure() {
    let running = CatgaTestHarness::new().unwrap().start();
    let runtime = FlowRuntime::new(
        running.suspended_flows(),
        running.flow_scheduler(),
        FlowDefinition::new("test-flow")
            .step("complete", |_| async { Ok(FlowStepOutcome::complete()) }),
        "test-node",
    );

    assert!(
        runtime
            .start("flow-1", b"input".to_vec())
            .await
            .unwrap()
            .is_success()
    );
}
