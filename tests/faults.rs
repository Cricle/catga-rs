//! Fault event and fault-publishing behavior tests.

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, EventHandler, Fault, FaultPublisher, FaultPublishingBehavior, Handler,
    Mediator, Pipeline, Registry, Request, current_correlation_id, scope_correlation_id,
};
use tokio::sync::mpsc;

#[derive(Clone)]
struct CreateOrder {
    id: u64,
}

impl catga_core::Message for CreateOrder {}

impl Request for CreateOrder {
    type Response = ();
}

struct RejectOrder;

#[async_trait]
impl Handler<CreateOrder> for RejectOrder {
    async fn handle(&self, _: CreateOrder) -> CatgaResult<()> {
        Err(CatgaError::new(
            catga_core::ErrorCode::Validation,
            "order cannot be created",
        ))
    }
}

struct ChannelFaultPublisher {
    sender: mpsc::Sender<Fault<CreateOrder>>,
}

#[async_trait]
impl FaultPublisher<CreateOrder> for ChannelFaultPublisher {
    async fn publish(&self, fault: Fault<CreateOrder>) -> CatgaResult<()> {
        self.sender.send(fault).await.map_err(|_| {
            CatgaError::new(
                catga_core::ErrorCode::Internal,
                "fault test receiver is unavailable",
            )
        })
    }
}

struct FailingFaultPublisher;

#[async_trait]
impl FaultPublisher<CreateOrder> for FailingFaultPublisher {
    async fn publish(&self, _: Fault<CreateOrder>) -> CatgaResult<()> {
        Err(CatgaError::new(
            catga_core::ErrorCode::Transient,
            "fault publisher is unavailable",
        ))
    }
}

struct FaultEventHandler {
    sender: mpsc::Sender<Fault<CreateOrder>>,
}

#[async_trait]
impl EventHandler<Fault<CreateOrder>> for FaultEventHandler {
    async fn handle(&self, fault: Fault<CreateOrder>) -> CatgaResult<()> {
        self.sender.send(fault).await.map_err(|_| {
            CatgaError::new(
                catga_core::ErrorCode::Internal,
                "fault test receiver is unavailable",
            )
        })
    }
}

#[tokio::test]
async fn fault_behavior_publishes_the_original_message_error_and_ambient_correlation() {
    let mut registry = Registry::new();
    registry
        .register_request::<CreateOrder, _>(RejectOrder)
        .unwrap();
    let mediator = Mediator::new(registry);
    let (sender, mut receiver) = mpsc::channel(1);
    let pipeline = Pipeline::new().with(FaultPublishingBehavior::new(Arc::new(
        ChannelFaultPublisher { sender },
    )));

    let error = scope_correlation_id(77, mediator.send_with(CreateOrder { id: 42 }, &pipeline))
        .await
        .expect_err("handler failure must be returned");
    let fault = receiver.recv().await.expect("fault must be published");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(fault.message().id, 42);
    assert_eq!(fault.error().code(), catga_core::ErrorCode::Validation);
    assert_eq!(fault.error().message(), "order cannot be created");
    assert_eq!(fault.correlation_id(), Some(77));
    assert!(fault.occurred_at() <= SystemTime::now());
    assert!(!fault.host().is_empty());
    assert_eq!(current_correlation_id(), None);
}

#[tokio::test]
async fn fault_publisher_failure_never_overrides_the_original_handler_failure() {
    let mut registry = Registry::new();
    registry
        .register_request::<CreateOrder, _>(RejectOrder)
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(FaultPublishingBehavior::new(Arc::new(
        FailingFaultPublisher,
    )));

    let error = mediator
        .send_with(CreateOrder { id: 42 }, &pipeline)
        .await
        .expect_err("fault publishing must not replace the handler failure");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(error.message(), "order cannot be created");
}

#[tokio::test]
async fn mediator_publishes_fault_events_without_a_custom_publisher_adapter() {
    let (sender, mut receiver) = mpsc::channel(1);
    let mut registry = Registry::new();
    registry
        .register_request::<CreateOrder, _>(RejectOrder)
        .unwrap();
    registry.register_event::<Fault<CreateOrder>, _>(FaultEventHandler { sender });
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Pipeline::new().with(FaultPublishingBehavior::new(Arc::clone(&mediator)));

    let error = mediator
        .send_with(CreateOrder { id: 7 }, &pipeline)
        .await
        .expect_err("handler failure must be returned");
    let fault = receiver
        .recv()
        .await
        .expect("mediator must publish a fault event");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(fault.message().id, 7);
}
