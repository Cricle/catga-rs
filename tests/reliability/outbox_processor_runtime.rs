//! Background outbox processing tests.

use std::{sync::Arc, time::Duration};

use catga_core::{
    CatgaResult, Envelope, ErrorCode, MessageMetadata, MessageTransport, OutboxLoopOptions,
    OutboxMessage, OutboxProcessor, OutboxStore,
};
use catga_core::memory::{MemoryOutbox, MemoryTransport};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn message(id: u64) -> OutboxMessage {
    OutboxMessage::new(Envelope::new(
        id,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(id, None),
    ))
}

#[tokio::test]
async fn background_processor_publishes_then_stops_on_cancellation() -> CatgaResult<()> {
    let store = Arc::new(MemoryOutbox::default());
    let transport = Arc::new(MemoryTransport::new(1)?);
    store.enqueue(message(41)).await?;
    let processor = OutboxProcessor::new(Arc::clone(&store), Arc::clone(&transport), "worker", 8)?;
    let shutdown = CancellationToken::new();
    let options = OutboxLoopOptions::new(Duration::from_secs(60), Duration::from_millis(1))?;

    let task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { processor.run_until_cancelled(options, shutdown).await }
    });

    let delivery = timeout(Duration::from_secs(1), transport.receive())
        .await
        .map_err(|_| catga_core::CatgaError::new(ErrorCode::Timeout, "outbox did not publish"))??;
    assert_eq!(delivery.envelope().id(), 41);

    shutdown.cancel();
    let run = timeout(Duration::from_secs(1), task)
        .await
        .map_err(|_| catga_core::CatgaError::new(ErrorCode::Timeout, "outbox did not stop"))?
        .map_err(|error| {
            catga_core::CatgaError::new(ErrorCode::Internal, format!("outbox task failed: {error}"))
        })??;
    assert_eq!(run.published(), 1);
    assert_eq!(run.failed(), 0);
    Ok(())
}

#[test]
fn background_processor_rejects_zero_intervals() {
    let error = OutboxLoopOptions::new(Duration::ZERO, Duration::from_millis(1))
        .expect_err("zero scan interval is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);

    let error = OutboxLoopOptions::new(Duration::from_millis(1), Duration::ZERO)
        .expect_err("zero error delay is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);
}
