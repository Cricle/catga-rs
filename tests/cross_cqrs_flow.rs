//! Cross-system integration: CQRS Mediator triggers durable Flow, flow completion publishes
//! events through the mediator, and projections advance.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Mediator, Message, Request,
    catga_handlers,
};
use catga_flow::{FlowRuntime, FlowStepOutcome, MemoryFlowScheduler, flow_definition};
use catga_memory::MemorySuspendedFlows;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Domain messages
// ---------------------------------------------------------------------------

/// Command that initiates a durable checkout flow.
#[derive(Clone)]
struct StartCheckout {
    order_id: String,
}
impl Message for StartCheckout {}
impl Command for StartCheckout {}

/// Event published when the checkout flow completes.
#[derive(Clone)]
struct CheckoutCompleted {
    order_id: String,
}
impl Message for CheckoutCompleted {}
impl Event for CheckoutCompleted {}

/// Query that reads the projection state.
#[derive(Clone)]
struct GetCheckoutStatus {
    order_id: String,
}
impl Message for GetCheckoutStatus {}
impl Request for GetCheckoutStatus {
    type Response = Option<String>;
}

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ProjectionState {
    completed: Mutex<Vec<String>>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

struct StartCheckoutHandler {
    flows: Arc<MemorySuspendedFlows>,
    scheduler: Arc<MemoryFlowScheduler>,
}

#[async_trait]
impl CommandHandler<StartCheckout> for StartCheckoutHandler {
    async fn handle(&self, command: StartCheckout) -> CatgaResult<()> {
        let definition = flow_definition! {
            "checkout";
            "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
            "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
        };
        let runtime = FlowRuntime::new(
            self.flows.clone(),
            self.scheduler.clone(),
            definition,
            "command-handler",
        );
        runtime.start(command.order_id, Vec::new()).await?;
        Ok(())
    }
}

struct CheckoutProjection {
    state: Arc<ProjectionState>,
}

#[async_trait]
impl EventHandler<CheckoutCompleted> for CheckoutProjection {
    async fn handle(&self, event: CheckoutCompleted) -> CatgaResult<()> {
        self.state.completed.lock().await.push(event.order_id);
        Ok(())
    }
}

struct GetCheckoutStatusHandler {
    state: Arc<ProjectionState>,
}

#[async_trait]
impl catga_core::Handler<GetCheckoutStatus> for GetCheckoutStatusHandler {
    async fn handle(&self, query: GetCheckoutStatus) -> CatgaResult<Option<String>> {
        let completed = self.state.completed.lock().await;
        Ok(completed.iter().find(|id| **id == query.order_id).cloned())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cqrs_command_triggers_durable_flow_and_completes() -> CatgaResult<()> {
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = flow_definition! {
        "checkout";
        "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };

    let runtime = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "test-owner");

    // Start the flow through a simulated command handler.
    runtime.start("order-1", Vec::new()).await?;

    // Resume until completion.
    let result = runtime.resume("order-1").await?;
    assert!(result.is_success());

    Ok(())
}

#[tokio::test]
async fn cqrs_mediator_dispatches_flow_command_and_event_projection() -> CatgaResult<()> {
    let projection_state = Arc::new(ProjectionState::default());
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let _definition = flow_definition! {
        "checkout";
        "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };

    let registry = catga_handlers! {
        command StartCheckout => StartCheckoutHandler {
            flows: Arc::clone(&flows),
            scheduler: Arc::clone(&scheduler),
        };
        event CheckoutCompleted => [CheckoutProjection { state: Arc::clone(&projection_state) }];
        request GetCheckoutStatus => GetCheckoutStatusHandler { state: Arc::clone(&projection_state) };
    }?;

    let mediator = Mediator::new(registry);

    // Dispatch the command that starts the flow.
    mediator
        .send_command(StartCheckout {
            order_id: "order-42".into(),
        })
        .await?;

    // The flow is now suspended; resume it to completion.
    let resume_definition = flow_definition! {
        "checkout";
        "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };
    let runtime = FlowRuntime::new(flows, scheduler, resume_definition, "test-owner");
    let result = runtime.resume("order-42").await?;
    assert!(result.is_success());

    // Simulate the flow completion publishing an event through the mediator.
    mediator
        .publish(CheckoutCompleted {
            order_id: "order-42".into(),
        })
        .await?;

    // Verify the projection advanced.
    let status = mediator
        .send(GetCheckoutStatus {
            order_id: "order-42".into(),
        })
        .await?;
    assert_eq!(status, Some("order-42".into()));

    Ok(())
}

#[tokio::test]
async fn cqrs_flow_failure_does_not_publish_completion_event() -> CatgaResult<()> {
    let projection_state = Arc::new(ProjectionState::default());
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = flow_definition! {
        "failing-checkout";
        "reserve" => |_| async {
            Err(catga_core::CatgaError::new(
                catga_core::ErrorCode::HandlerFailed,
                "payment declined",
            ))
        };
    };

    let runtime = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "test-owner");

    runtime.start("order-fail", Vec::new()).await?;
    let result = runtime.resume("order-fail").await?;
    assert!(result.is_failure());

    // No event should be published for a failed flow.
    let registry = catga_handlers! {
        event CheckoutCompleted => [CheckoutProjection { state: Arc::clone(&projection_state) }];
        request GetCheckoutStatus => GetCheckoutStatusHandler { state: Arc::clone(&projection_state) };
    }?;
    let mediator = Mediator::new(registry);

    let status = mediator
        .send(GetCheckoutStatus {
            order_id: "order-fail".into(),
        })
        .await?;
    assert_eq!(status, None);

    Ok(())
}
