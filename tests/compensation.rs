//! Request compensation pipeline tests.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CompensationBehavior, CompensationPublisher, Event,
    EventCompensationPublisher, EventHandler, Handler, Mediator, Pipeline, Registry, Request,
};
use tokio::sync::mpsc;

#[derive(Clone)]
struct ChargeCard {
    order_id: u64,
}

impl catga_core::Message for ChargeCard {}

impl Request for ChargeCard {
    type Response = ();
}

#[derive(Clone)]
struct ChargeCompensated {
    order_id: u64,
}

impl catga_core::Message for ChargeCompensated {}
impl Event for ChargeCompensated {}

struct RejectCharge;

#[async_trait]
impl Handler<ChargeCard> for RejectCharge {
    async fn handle(&self, _: ChargeCard) -> CatgaResult<()> {
        Err(catga_core::CatgaError::new(
            catga_core::ErrorCode::Transient,
            "payment provider is unavailable",
        ))
    }
}

struct ChannelCompensationPublisher {
    sender: mpsc::Sender<(ChargeCard, CatgaError)>,
}

#[async_trait]
impl CompensationPublisher<ChargeCard> for ChannelCompensationPublisher {
    async fn publish(&self, request: &ChargeCard, error: &CatgaError) -> CatgaResult<()> {
        self.sender
            .send((request.clone(), error.clone()))
            .await
            .map_err(|_| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, "closed"))
    }
}

struct FailingCompensationPublisher;

#[async_trait]
impl CompensationPublisher<ChargeCard> for FailingCompensationPublisher {
    async fn publish(&self, _: &ChargeCard, _: &CatgaError) -> CatgaResult<()> {
        Err(catga_core::CatgaError::new(
            catga_core::ErrorCode::Transient,
            "compensation broker is unavailable",
        ))
    }
}

struct CompensationEventHandler {
    sender: mpsc::Sender<ChargeCompensated>,
}

#[async_trait]
impl EventHandler<ChargeCompensated> for CompensationEventHandler {
    async fn handle(&self, event: ChargeCompensated) -> CatgaResult<()> {
        self.sender
            .send(event)
            .await
            .map_err(|_| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, "closed"))
    }
}

fn mediator() -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<ChargeCard, _>(RejectCharge)
        .unwrap();
    Mediator::new(registry)
}

#[tokio::test]
async fn compensation_publishes_the_original_request_and_error_after_a_handler_failure() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mediator = mediator();
    let pipeline = Pipeline::new().with(CompensationBehavior::new(Arc::new(
        ChannelCompensationPublisher { sender },
    )));

    let error = mediator
        .send_with(ChargeCard { order_id: 7 }, &pipeline)
        .await
        .expect_err("handler error must be returned");
    let (request, compensation_error) = receiver.recv().await.expect("compensation is published");

    assert_eq!(error.message(), "payment provider is unavailable");
    assert_eq!(request.order_id, 7);
    assert_eq!(compensation_error, error);
}

#[tokio::test]
async fn compensation_failure_never_overrides_the_handler_failure() {
    let mediator = mediator();
    let pipeline = Pipeline::new().with(CompensationBehavior::new(Arc::new(
        FailingCompensationPublisher,
    )));

    let error = mediator
        .send_with(ChargeCard { order_id: 8 }, &pipeline)
        .await
        .expect_err("handler error must be returned");

    assert_eq!(error.message(), "payment provider is unavailable");
}

#[tokio::test]
async fn event_compensation_publisher_reuses_mediator_event_fan_out() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut registry = Registry::new();
    registry
        .register_request::<ChargeCard, _>(RejectCharge)
        .unwrap();
    registry.register_event::<ChargeCompensated, _>(CompensationEventHandler { sender });
    let mediator = Arc::new(Mediator::new(registry));
    let publisher = EventCompensationPublisher::<ChargeCard, ChargeCompensated, _>::new(
        Arc::clone(&mediator),
        |request, _| {
            Some(ChargeCompensated {
                order_id: request.order_id,
            })
        },
    );
    let pipeline = Pipeline::new().with(CompensationBehavior::new(Arc::new(publisher)));

    let _ = mediator
        .send_with(ChargeCard { order_id: 9 }, &pipeline)
        .await
        .expect_err("handler must still fail");
    let event = receiver
        .recv()
        .await
        .expect("compensation event is published");

    assert_eq!(event.order_id, 9);
}
