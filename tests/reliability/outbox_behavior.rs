//! Outbox pipeline composition tests.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Handler, Mediator, MessageMetadata,
    OutboxBehavior, OutboxEnvelope, OutboxStore, Pipeline, Registry, Request,
};
use catga_core::memory::MemoryOutbox;

#[derive(Debug)]
struct OrderPublished(u64);

impl catga_core::Message for OrderPublished {}

impl Request for OrderPublished {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl OutboxEnvelope for OrderPublished {
    fn outbox_envelope(&self) -> Envelope {
        Envelope::new(
            self.0,
            "orders.published",
            vec![self.0 as u8],
            MessageMetadata::new(self.0, None),
        )
    }
}

struct SuccessfulHandler;

#[async_trait]
impl Handler<OrderPublished> for SuccessfulHandler {
    async fn handle(&self, message: OrderPublished) -> CatgaResult<u64> {
        Ok(message.0)
    }
}

struct FailedHandler;

#[async_trait]
impl Handler<OrderPublished> for FailedHandler {
    async fn handle(&self, _: OrderPublished) -> CatgaResult<u64> {
        Err(CatgaError::new(ErrorCode::Conflict, "order was rejected"))
    }
}

fn mediator<H: Handler<OrderPublished> + 'static>(handler: H) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<OrderPublished, _>(handler)
        .expect("test registry accepts one handler");
    Mediator::new(registry)
}

#[tokio::test]
async fn successful_request_is_persisted_for_the_outbox_processor() {
    let store = Arc::new(MemoryOutbox::default());
    let pipeline = Pipeline::new().with(OutboxBehavior::new(Arc::clone(&store)));

    let result = mediator(SuccessfulHandler)
        .send_with(OrderPublished(42), &pipeline)
        .await;

    assert_eq!(result, Ok(42));
    let pending = store
        .claim("outbox-worker", 8)
        .await
        .expect("store claim succeeds");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].envelope().id(), 42);
    assert_eq!(pending[0].envelope().message_type(), "orders.published");
}

#[tokio::test]
async fn failed_request_does_not_create_an_outbox_message() {
    let store = Arc::new(MemoryOutbox::default());
    let pipeline = Pipeline::new().with(OutboxBehavior::new(Arc::clone(&store)));

    let result = mediator(FailedHandler)
        .send_with(OrderPublished(7), &pipeline)
        .await;

    assert_eq!(
        result.expect_err("handler failure is preserved").code(),
        ErrorCode::Conflict
    );
    assert!(
        store
            .claim("outbox-worker", 8)
            .await
            .expect("store claim succeeds")
            .is_empty()
    );
}
