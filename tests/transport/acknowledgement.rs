//! Transport acknowledgement tests.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use catga_core::{Acknowledger, CatgaResult, Delivery, Envelope, MessageMetadata};

struct NegativeAcknowledger(Arc<AtomicBool>);

#[async_trait]
impl Acknowledger for NegativeAcknowledger {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        Ok(())
    }

    async fn negative_acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0.store(true, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn delivery_can_request_redelivery_through_its_acknowledgement_token() {
    let redelivered = Arc::new(AtomicBool::new(false));
    let delivery = Delivery::with_acknowledger(
        Envelope::new(73, "orders.retry", vec![], MessageMetadata::new(73, None)),
        Box::new(NegativeAcknowledger(Arc::clone(&redelivered))),
    );

    delivery
        .negative_acknowledge()
        .await
        .expect("negative acknowledgement succeeds");
    assert!(redelivered.load(Ordering::Acquire));
}
