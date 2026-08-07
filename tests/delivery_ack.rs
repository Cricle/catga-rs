//! Delivery acknowledgement tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::memory::MemoryTransport;
use catga_core::{
    Acknowledger, CatgaResult, Delivery, Envelope, MessageMetadata, MessageTransport,
};

struct CounterAcknowledger(Arc<AtomicUsize>);

#[async_trait]
impl Acknowledger for CounterAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn acknowledgement_is_consumed_with_its_delivery() {
    let count = Arc::new(AtomicUsize::new(0));
    let delivery = Delivery::with_acknowledger(
        Envelope::new(1, "event", vec![], MessageMetadata::new(1, None)),
        Box::new(CounterAcknowledger(count.clone())),
    );

    MemoryTransport::new(1)
        .unwrap()
        .ack(delivery)
        .await
        .unwrap();
    assert_eq!(count.load(Ordering::Relaxed), 1);
}
