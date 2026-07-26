//! Mediator routing tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    Behavior, CatgaResult, Command, CommandHandler, ErrorCode, Event, EventHandler, Handler,
    Mediator, MediatorHandle, Next, Pipeline, Registry, Request,
};

#[derive(Debug)]
struct Double(u64);

impl catga_core::Message for Double {}

impl Request for Double {
    type Response = u64;
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, message: Double) -> CatgaResult<u64> {
        Ok(message.0 * 2)
    }
}

struct PanickingDoubleHandler;

#[async_trait]
impl Handler<Double> for PanickingDoubleHandler {
    async fn handle(&self, _: Double) -> CatgaResult<u64> {
        panic!("request handler panic must not escape the mediator");
    }
}

struct PanickingDoubleBehavior;

#[async_trait]
impl Behavior<Double> for PanickingDoubleBehavior {
    async fn handle(&self, _: Double, _: Next<Double>) -> CatgaResult<u64> {
        panic!("pipeline behavior panic must not escape the mediator");
    }
}

#[tokio::test]
async fn request_handler_panics_become_internal_errors() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(PanickingDoubleHandler)?;
    let mediator = Mediator::new(registry);

    let error = match mediator.send(Double(1)).await {
        Ok(_) => {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Internal,
                "panic must be isolated",
            ));
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Internal);
    Ok(())
}

#[tokio::test]
async fn pipeline_behavior_panics_become_internal_errors() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(DoubleHandler)?;
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(PanickingDoubleBehavior);

    let error = match mediator.send_with(Double(1), &pipeline).await {
        Ok(_) => {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Internal,
                "panic must be isolated",
            ));
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Internal);
    Ok(())
}

#[tokio::test]
async fn pipeline_terminal_handler_panics_become_internal_errors() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Double, _>(PanickingDoubleHandler)?;
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new();

    let error = match mediator.send_with(Double(1), &pipeline).await {
        Ok(_) => {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Internal,
                "panic must be isolated",
            ));
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Internal);
    Ok(())
}

#[derive(Debug)]
struct ShipOrder(u64);

impl catga_core::Message for ShipOrder {}
impl Command for ShipOrder {}

struct ShipOrderHandler {
    shipped_order: Arc<AtomicUsize>,
}

#[async_trait]
impl CommandHandler<ShipOrder> for ShipOrderHandler {
    async fn handle(&self, command: ShipOrder) -> CatgaResult<()> {
        self.shipped_order
            .store(command.0 as usize, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn command_routes_to_its_sole_handler() -> CatgaResult<()> {
    let shipped_order = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_command::<ShipOrder, _>(ShipOrderHandler {
        shipped_order: Arc::clone(&shipped_order),
    })?;
    let mediator = Mediator::new(registry);

    mediator.send_command(ShipOrder(42)).await?;

    assert_eq!(shipped_order.load(Ordering::Relaxed), 42);
    Ok(())
}

#[tokio::test]
async fn command_registration_rejects_duplicates_without_replacing_the_handler() -> CatgaResult<()>
{
    let first_shipped_order = Arc::new(AtomicUsize::new(0));
    let replacement_shipped_order = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_command::<ShipOrder, _>(ShipOrderHandler {
        shipped_order: Arc::clone(&first_shipped_order),
    })?;

    assert!(matches!(
        registry.register_command::<ShipOrder, _>(ShipOrderHandler {
            shipped_order: Arc::clone(&replacement_shipped_order),
        }),
        Err(error) if error.code() == ErrorCode::Conflict
    ));

    Mediator::new(registry).send_command(ShipOrder(42)).await?;
    assert_eq!(first_shipped_order.load(Ordering::Relaxed), 42);
    assert_eq!(replacement_shipped_order.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn command_dispatch_reports_missing_handler_and_unbound_handle() -> CatgaResult<()> {
    let handle = MediatorHandle::new();

    assert!(matches!(
        handle.send_command(ShipOrder(7)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    assert!(matches!(
        Mediator::new(Registry::new()).send_command(ShipOrder(7)).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    let shipped_order = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_command::<ShipOrder, _>(ShipOrderHandler {
        shipped_order: Arc::clone(&shipped_order),
    })?;
    let mediator = Arc::new(Mediator::new(registry));
    handle.bind(Arc::clone(&mediator))?;

    handle.send_command(ShipOrder(7)).await?;
    assert_eq!(shipped_order.load(Ordering::Relaxed), 7);
    Ok(())
}

#[derive(Clone, Debug)]
struct OrderCreated;

impl catga_core::Message for OrderCreated {}
impl Event for OrderCreated {}

struct AuditOrder {
    count: Arc<AtomicUsize>,
}

struct NotifyCustomer {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl EventHandler<OrderCreated> for AuditOrder {
    async fn handle(&self, _: OrderCreated) -> CatgaResult<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl EventHandler<OrderCreated> for NotifyCustomer {
    async fn handle(&self, _: OrderCreated) -> CatgaResult<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn request_routes_to_one_handler_and_event_fans_out() -> CatgaResult<()> {
    let audit_count = Arc::new(AtomicUsize::new(0));
    let notify_count = Arc::new(AtomicUsize::new(0));

    let mut registry = Registry::new();
    registry.register_request::<Double, _>(DoubleHandler)?;
    registry.register_event::<OrderCreated, _>(AuditOrder {
        count: Arc::clone(&audit_count),
    });
    registry.register_event::<OrderCreated, _>(NotifyCustomer {
        count: Arc::clone(&notify_count),
    });
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Double(4)).await?, 8);
    mediator.publish(OrderCreated).await?;
    assert_eq!(audit_count.load(Ordering::Relaxed), 1);
    assert_eq!(notify_count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[derive(Debug)]
struct CloneTrackedEvent {
    clone_count: Arc<AtomicUsize>,
}

impl Clone for CloneTrackedEvent {
    fn clone(&self) -> Self {
        self.clone_count.fetch_add(1, Ordering::Relaxed);
        Self {
            clone_count: Arc::clone(&self.clone_count),
        }
    }
}

impl catga_core::Message for CloneTrackedEvent {}
impl Event for CloneTrackedEvent {}

struct CloneTrackedHandler {
    delivery_count: Arc<AtomicUsize>,
}

#[async_trait]
impl EventHandler<CloneTrackedEvent> for CloneTrackedHandler {
    async fn handle(&self, _: CloneTrackedEvent) -> CatgaResult<()> {
        self.delivery_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FollowUpCreated;

impl catga_core::Message for FollowUpCreated {}
impl Event for FollowUpCreated {}

struct FollowUpAudit {
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl EventHandler<FollowUpCreated> for FollowUpAudit {
    async fn handle(&self, _: FollowUpCreated) -> CatgaResult<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct PublishCreated;

impl catga_core::Message for PublishCreated {}

impl Request for PublishCreated {
    type Response = ();
}

struct FollowUpHandler {
    mediator: MediatorHandle,
}

#[async_trait]
impl Handler<PublishCreated> for FollowUpHandler {
    async fn handle(&self, _: PublishCreated) -> CatgaResult<()> {
        self.mediator.publish(FollowUpCreated).await
    }
}

#[tokio::test]
async fn mediator_handle_binds_after_registry_startup_and_publishes_from_a_handler()
-> CatgaResult<()> {
    let follow_up_count = Arc::new(AtomicUsize::new(0));
    let handle = MediatorHandle::new();

    assert!(matches!(
        handle.publish(FollowUpCreated).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));

    let mut registry = Registry::new();
    registry.register_request::<PublishCreated, _>(FollowUpHandler {
        mediator: handle.clone(),
    })?;
    registry.register_event::<FollowUpCreated, _>(FollowUpAudit {
        count: Arc::clone(&follow_up_count),
    });
    let mediator = Arc::new(Mediator::new(registry));
    handle.bind(Arc::clone(&mediator))?;

    assert!(matches!(
        handle.bind(Arc::clone(&mediator)),
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    mediator.send(PublishCreated).await?;
    assert_eq!(follow_up_count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn event_fan_out_moves_the_final_delivery_instead_of_cloning_it() -> CatgaResult<()> {
    let clone_count = Arc::new(AtomicUsize::new(0));
    let delivery_count = Arc::new(AtomicUsize::new(0));

    let mut registry = Registry::new();
    registry.register_event::<CloneTrackedEvent, _>(CloneTrackedHandler {
        delivery_count: Arc::clone(&delivery_count),
    });
    registry.register_event::<CloneTrackedEvent, _>(CloneTrackedHandler {
        delivery_count: Arc::clone(&delivery_count),
    });
    registry.register_event::<CloneTrackedEvent, _>(CloneTrackedHandler {
        delivery_count: Arc::clone(&delivery_count),
    });
    let mediator = Mediator::new(registry);

    mediator
        .publish(CloneTrackedEvent {
            clone_count: Arc::clone(&clone_count),
        })
        .await?;

    assert_eq!(delivery_count.load(Ordering::Relaxed), 3);
    assert_eq!(clone_count.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn bounded_event_fan_out_rejects_zero_concurrency() -> CatgaResult<()> {
    let mediator = Mediator::new(Registry::new());

    let error = match mediator.publish_with_concurrency(OrderCreated, 0).await {
        Ok(()) => {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Internal,
                "zero handler concurrency must be rejected",
            ));
        }
        Err(error) => error,
    };

    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}
