//! Cross-system integration: Durable flow completion triggers event store appends, which advance
//! subscription checkpoints and projection state. Verifies the end-to-end path from flow step
//! execution through event persistence to subscription consumption.

use std::sync::Arc;

use catga_core::{CatgaResult, Envelope, EventStore, MessageMetadata};
use catga_flow::{FlowRuntime, FlowStepOutcome, MemoryFlowScheduler, flow_definition};
use catga_memory::{MemoryEventStore, MemorySuspendedFlows};
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Projection state driven by subscription
// ---------------------------------------------------------------------------

#[derive(Default)]
struct OrderProjection {
    processed: Mutex<Vec<String>>,
}

impl OrderProjection {
    fn process_event(&self, envelope: &Envelope) {
        let payload = String::from_utf8_lossy(envelope.payload());
        self.processed.lock().unwrap().push(payload.to_string());
    }

    fn processed_count(&self) -> usize {
        self.processed.lock().unwrap().len()
    }

    fn last_processed(&self) -> Option<String> {
        self.processed.lock().unwrap().last().cloned()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flow_completion_appends_events_that_advance_subscription() -> CatgaResult<()> {
    let event_store = Arc::new(MemoryEventStore::default());
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let projection = Arc::new(OrderProjection::default());

    let definition = flow_definition! {
        "order-lifecycle";
        "reserve" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "charge" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::Advance) };
        "ship" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };

    let runtime = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "test-owner");

    // Run the flow to completion.
    runtime.start("order-sub-1", Vec::new()).await?;
    let result = runtime.resume("order-sub-1").await?;
    assert!(result.is_success());

    // Simulate: each completed step appends an event to the event store.
    let stream_id = "order-sub-1";
    for step in ["reserved", "charged", "shipped"] {
        let envelope = Envelope::new(
            0,
            "OrderStepCompleted",
            step.as_bytes().to_vec(),
            MessageMetadata::new(1, None),
        );
        event_store.append(stream_id, vec![envelope], None).await?;
    }

    // Verify events are in the store.
    let version = event_store.version(stream_id).await?;
    assert_eq!(version, 2); // 0-based: 3 events = version 2

    // Simulate subscription consumption: read all events and advance projection.
    let page = event_store.read_page(stream_id, 0, 100).await?;
    for stored in page.stream().events() {
        projection.process_event(stored.envelope());
    }

    assert_eq!(projection.processed_count(), 3);
    assert_eq!(projection.last_processed(), Some("shipped".into()));

    Ok(())
}

#[tokio::test]
async fn subscription_checkpoint_prevents_reprocessing() -> CatgaResult<()> {
    let event_store = Arc::new(MemoryEventStore::default());
    let projection = Arc::new(OrderProjection::default());
    let stream_id = "order-checkpoint";

    // Append 5 events.
    for i in 0..5 {
        let envelope = Envelope::new(
            0,
            "StepDone",
            format!("step-{i}").into_bytes(),
            MessageMetadata::new(i as u64, None),
        );
        event_store.append(stream_id, vec![envelope], None).await?;
    }

    // First pass: consume all events from version 0.
    let page = event_store.read_page(stream_id, 0, 100).await?;
    let mut checkpoint: u64 = 0;
    for stored in page.stream().events() {
        projection.process_event(stored.envelope());
        checkpoint = stored.version() as u64 + 1;
    }
    assert_eq!(projection.processed_count(), 5);
    assert_eq!(checkpoint, 5);

    // Second pass: resume from checkpoint — no new events.
    let page = event_store.read_page(stream_id, checkpoint, 100).await?;
    assert!(page.stream().events().is_empty());
    assert_eq!(projection.processed_count(), 5); // unchanged

    Ok(())
}

#[tokio::test]
async fn multiple_flows_share_one_stream_and_subscription_advances() -> CatgaResult<()> {
    let event_store = Arc::new(MemoryEventStore::default());
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let projection = Arc::new(OrderProjection::default());

    let _definition = flow_definition! {
        "batch-item";
        "process" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
    };

    // Run 3 flows, each appending to the same aggregate stream.
    let stream_id = "batch-stream";
    for i in 1..=3 {
        let flow_id = format!("batch-{i}");
        let batch_definition = flow_definition! {
            "batch-item";
            "process" => |_| async { Ok::<_, catga_core::CatgaError>(FlowStepOutcome::complete()) };
        };
        let runtime = FlowRuntime::new(
            flows.clone(),
            scheduler.clone(),
            batch_definition,
            "test-owner",
        );
        runtime.start(flow_id.as_str(), Vec::new()).await?;
        let result = runtime.resume(&flow_id).await?;
        assert!(result.is_success());

        let envelope = Envelope::new(
            0,
            "ItemProcessed",
            flow_id.into_bytes(),
            MessageMetadata::new(i, None),
        );
        event_store.append(stream_id, vec![envelope], None).await?;
    }

    // Subscription consumes all.
    let page = event_store.read_page(stream_id, 0, 100).await?;
    for stored in page.stream().events() {
        projection.process_event(stored.envelope());
    }

    assert_eq!(projection.processed_count(), 3);
    assert_eq!(projection.last_processed(), Some("batch-3".into()));

    Ok(())
}

#[tokio::test]
async fn failed_flow_does_not_append_to_event_stream() -> CatgaResult<()> {
    let event_store = Arc::new(MemoryEventStore::default());
    let flows = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());

    let definition = flow_definition! {
        "failing-flow";
        "explode" => |_| async {
            Err(catga_core::CatgaError::new(
                catga_core::ErrorCode::HandlerFailed,
                "boom",
            ))
        };
    };

    let runtime = FlowRuntime::new(flows.clone(), scheduler.clone(), definition, "test-owner");

    runtime.start("flow-no-event", Vec::new()).await?;
    let result = runtime.resume("flow-no-event").await?;
    assert!(result.is_failure());

    // No event should be appended for a failed flow.
    let version = event_store.version("flow-no-event").await?;
    assert_eq!(version, -1); // empty stream

    Ok(())
}

#[tokio::test]
async fn incremental_subscription_polling_processes_new_events_only() -> CatgaResult<()> {
    let event_store = Arc::new(MemoryEventStore::default());
    let projection = Arc::new(OrderProjection::default());
    let stream_id = "incremental-stream";

    // Append 2 events.
    for i in 0..2 {
        let envelope = Envelope::new(
            0,
            "Event",
            format!("first-{i}").into_bytes(),
            MessageMetadata::new(i, None),
        );
        event_store.append(stream_id, vec![envelope], None).await?;
    }

    // First poll.
    let page = event_store.read_page(stream_id, 0, 100).await?;
    let mut next: u64 = 0;
    for stored in page.stream().events() {
        projection.process_event(stored.envelope());
        next = stored.version() as u64 + 1;
    }
    assert_eq!(projection.processed_count(), 2);

    // Append 2 more events.
    for i in 0..2 {
        let envelope = Envelope::new(
            0,
            "Event",
            format!("second-{i}").into_bytes(),
            MessageMetadata::new(i + 2, None),
        );
        event_store.append(stream_id, vec![envelope], None).await?;
    }

    // Second poll from checkpoint — only new events.
    let page = event_store.read_page(stream_id, next, 100).await?;
    for stored in page.stream().events() {
        projection.process_event(stored.envelope());
    }
    assert_eq!(projection.processed_count(), 4);
    assert_eq!(projection.last_processed(), Some("second-1".into()));

    Ok(())
}
