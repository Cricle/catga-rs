//! Mediator routing tests.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_core::{CatgaResult, Event, EventHandler, Handler, Mediator, Registry, Request};

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

#[derive(Clone, Debug)]
struct OrderCreated;

impl catga_core::Message for OrderCreated {}
impl Event for OrderCreated {}

static AUDIT_COUNT: AtomicUsize = AtomicUsize::new(0);
static NOTIFY_COUNT: AtomicUsize = AtomicUsize::new(0);

struct AuditOrder;
struct NotifyCustomer;

#[async_trait]
impl EventHandler<OrderCreated> for AuditOrder {
    async fn handle(&self, _: OrderCreated) -> CatgaResult<()> {
        AUDIT_COUNT.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl EventHandler<OrderCreated> for NotifyCustomer {
    async fn handle(&self, _: OrderCreated) -> CatgaResult<()> {
        NOTIFY_COUNT.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn request_routes_to_one_handler_and_event_fans_out() {
    AUDIT_COUNT.store(0, Ordering::Relaxed);
    NOTIFY_COUNT.store(0, Ordering::Relaxed);

    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler)
        .unwrap();
    registry.register_event::<OrderCreated, _>(AuditOrder);
    registry.register_event::<OrderCreated, _>(NotifyCustomer);
    let mediator = Mediator::new(registry);

    assert_eq!(mediator.send(Double(4)).await.unwrap(), 8);
    mediator.publish(OrderCreated).await.unwrap();
    assert_eq!(AUDIT_COUNT.load(Ordering::Relaxed), 1);
    assert_eq!(NOTIFY_COUNT.load(Ordering::Relaxed), 1);
}
