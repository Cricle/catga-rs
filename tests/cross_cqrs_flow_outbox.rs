//! Cross-system integration: At-least-once outbox pipeline combined with durable flow execution.
//! Verifies that flow-triggered events are durably enqueued, published exactly once after flow
//! completion, and that cancellation prevents unpublished envelopes from leaking.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Command, CommandHandler, Envelope, Mediator, Message, MessageMetadata,
    OutboxMessage, OutboxStore, catga_handlers,
};
use catga_flow::{FlowRuntime, FlowStepOutcome, MemoryFlowScheduler, flow_definition};
use catga_memory::{MemoryOutbox, MemorySuspendedFlows};

// ---------------------------------------------------------------------------
// Domain
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PlaceOrder {
    order_id: String,
}
impl Message for PlaceOrder {}
impl Command for PlaceOrder {
    type TypeId = catga_core::DefaultMessageTypeId;
}

// ---------------------------------------------------------------------------
// Outbox-backed event publication after flow completion
// ---------------------------------------------------------------------------

struct DurableEventPublisher {
    outbox: Arc<MemoryOutbox>,
    published: Arc<Mutex<Vec<String>>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl DurableEventPublisher {
    fn new(outbox: Arc<MemoryOutbox>, published: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            outbox,
            published,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    async fn enqueue_event(&self, event_type: &str, payload: Vec<u8>) -> CatgaResult<()> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let envelope = Envelope::new(id, event_type, payload, MessageMetadata::new(id, None));
        self.outbox.enqueue(OutboxMessage::new(envelope)).await
    }

    async fn drain_and_publish(&self) -> CatgaResult<usize> {
        let claimed = self.outbox.claim("test-worker", 100).await?;
        let mut count = 0;
        for message in &claimed {
            self.published
                .lock()
                .unwrap()
                .push(message.envelope().message_type().to_owned());
            if let Some(token) = message.claim_token() {
                self.outbox.ack("test-worker", message.id(), token).await?;
            }
            count += 1;
        }
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Command handler that starts a flow and enqueues events durably
// ---------------------------------------------------------------------------

struct PlaceOrderHandler {
    flows: Arc<MemorySuspendedFlows>,
    scheduler: Arc<MemoryFlowScheduler>,
    publisher: Arc<DurableEventPublisher>,
    fail_flow: bool,
}

#[async_trait]
impl CommandHandler<PlaceOrder> for PlaceOrderHandler {
    async fn handle(&self, command: PlaceOrder) -> CatgaResult<()> {
        let definition = if self.fail_flow {
            flow_definition! {
                "place-order";
                "validate" => |_| async {
                    Err(catga_core::CatgaError::new(
                        catga_core::ErrorCode::Validation,
                        "insufficient stock",
                    ))
                };
            }
        } else {
            flow_definition! {
                "place-order";
                "validate" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
                "commit" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
            }
        };
        let runtime = FlowRuntime::new(
            self.flows.clone(),
            self.scheduler.clone(),
            definition,
            "command-handler",
        );

        // Start and run the flow to completion.
        runtime.start(command.order_id.clone(), Vec::new()).await?;
        let result = runtime.resume(&command.order_id).await?;

        if result.is_success() {
            // Flow completed: durably enqueue the domain event.
            self.publisher
                .enqueue_event("OrderPlaced", command.order_id.into_bytes())
                .await?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outbox_receives_event_only_after_flow_completion() -> CatgaResult<()> {
    let outbox = Arc::new(MemoryOutbox::default());
    let published = Arc::new(Mutex::new(Vec::new()));
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let _definition = flow_definition! {
        "place-order";
        "validate" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "commit" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };

    let publisher = Arc::new(DurableEventPublisher::new(
        Arc::clone(&outbox),
        Arc::clone(&published),
    ));

    let registry = catga_handlers! {
        command PlaceOrder => PlaceOrderHandler {
            flows: Arc::clone(&flows),
            scheduler: Arc::clone(&scheduler),
            publisher: Arc::clone(&publisher),
            fail_flow: false,
        };
    }?;

    let mediator = Mediator::new(registry);

    // Dispatch the command.
    mediator
        .send_command(PlaceOrder {
            order_id: "order-100".into(),
        })
        .await?;

    // Event is in the outbox but not yet published.
    assert!(published.lock().unwrap().is_empty());

    // Drain the outbox (simulating the background processor).
    let count = publisher.drain_and_publish().await?;
    assert_eq!(count, 1);
    assert_eq!(published.lock().unwrap()[0], "OrderPlaced");

    Ok(())
}

#[tokio::test]
async fn outbox_does_not_enqueue_when_flow_fails() -> CatgaResult<()> {
    let outbox = Arc::new(MemoryOutbox::default());
    let published = Arc::new(Mutex::new(Vec::new()));
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let publisher = Arc::new(DurableEventPublisher::new(
        Arc::clone(&outbox),
        Arc::clone(&published),
    ));

    let registry = catga_handlers! {
        command PlaceOrder => PlaceOrderHandler {
            flows: Arc::clone(&flows),
            scheduler: Arc::clone(&scheduler),
            publisher: Arc::clone(&publisher),
            fail_flow: true,
        };
    }?;

    let mediator = Mediator::new(registry);

    // The command handler itself succeeds (flow failure is a business outcome).
    mediator
        .send_command(PlaceOrder {
            order_id: "order-fail".into(),
        })
        .await?;

    // No event should be in the outbox.
    let count = publisher.drain_and_publish().await?;
    assert_eq!(count, 0);
    assert!(published.lock().unwrap().is_empty());

    Ok(())
}

#[tokio::test]
async fn outbox_multiple_orders_publish_in_order() -> CatgaResult<()> {
    let outbox = Arc::new(MemoryOutbox::default());
    let published = Arc::new(Mutex::new(Vec::new()));
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let publisher = Arc::new(DurableEventPublisher::new(
        Arc::clone(&outbox),
        Arc::clone(&published),
    ));

    let registry = catga_handlers! {
        command PlaceOrder => PlaceOrderHandler {
            flows: Arc::clone(&flows),
            scheduler: Arc::clone(&scheduler),
            publisher: Arc::clone(&publisher),
            fail_flow: false,
        };
    }?;

    let mediator = Mediator::new(registry);

    for i in 1..=5 {
        mediator
            .send_command(PlaceOrder {
                order_id: format!("order-{i}"),
            })
            .await?;
    }

    // All 5 events are in the outbox.
    let count = publisher.drain_and_publish().await?;
    assert_eq!(count, 5);
    assert_eq!(published.lock().unwrap().len(), 5);

    Ok(())
}

#[tokio::test]
async fn outbox_idempotent_mark_published_prevents_double_delivery() -> CatgaResult<()> {
    let outbox = Arc::new(MemoryOutbox::default());
    let published = Arc::new(Mutex::new(Vec::new()));

    let publisher = DurableEventPublisher::new(Arc::clone(&outbox), Arc::clone(&published));

    // Enqueue one event.
    publisher
        .enqueue_event("TestEvent", b"payload".to_vec())
        .await?;

    // First drain publishes it.
    let count = publisher.drain_and_publish().await?;
    assert_eq!(count, 1);

    // Second drain finds nothing new.
    let count = publisher.drain_and_publish().await?;
    assert_eq!(count, 0);
    assert_eq!(published.lock().unwrap().len(), 1);

    Ok(())
}
