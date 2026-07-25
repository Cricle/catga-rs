//! Transport batch publication tests.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Delivery, Envelope, ErrorCode, MessageMetadata, MessageTransport,
};

struct ProbeTransport {
    fail_id: u64,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    completed: AtomicUsize,
}

impl ProbeTransport {
    fn new(fail_id: u64) -> Self {
        Self {
            fail_id,
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl MessageTransport for ProbeTransport {
    async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let current = self.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_in_flight.fetch_max(current, Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.completed.fetch_add(1, Ordering::AcqRel);
        if envelope.id() == self.fail_id {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "simulated transport failure",
            ));
        }
        Ok(())
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "probe transport does not receive deliveries",
        ))
    }
}

fn envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[tokio::test]
async fn batch_publish_bounds_concurrency_and_attempts_every_envelope_after_a_failure() {
    let transport = ProbeTransport::new(2);

    let error = transport
        .publish_batch_with_concurrency(vec![envelope(1), envelope(2), envelope(3), envelope(4)], 2)
        .await
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(transport.max_in_flight.load(Ordering::Acquire), 2);
    assert_eq!(transport.completed.load(Ordering::Acquire), 4);
}

#[tokio::test]
async fn batch_publish_rejects_a_zero_concurrency_limit() {
    let transport = ProbeTransport::new(0);

    let error = transport
        .publish_batch_with_concurrency(vec![envelope(1)], 0)
        .await
        .unwrap_err();

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "transport batch concurrency limit must be greater than zero"
    );
}
