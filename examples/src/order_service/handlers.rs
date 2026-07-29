use std::{sync::Arc, sync::atomic::Ordering};

use async_trait::async_trait;
use catga_cluster::ClusterCoordinator;
use catga_core::{
    CatgaError, CatgaResult, CommandHandler, Envelope, ErrorCode, EventHandler, EventStore,
    Handler, MessageMetadata, OutboxMessage, OutboxProcessor, OutboxStore,
};
use catga_flow::Flow;

use super::{
    app::OrderRuntime,
    domain::{GetOrder, OrderAccepted, OrderCompleted, PlaceOrder, RecordOrder},
};

pub(super) struct PlaceOrderHandler {
    pub(super) runtime: Arc<OrderRuntime>,
}

pub(super) struct RecordOrderHandler {
    pub(super) runtime: Arc<OrderRuntime>,
}

pub(super) struct GetOrderHandler {
    pub(super) runtime: Arc<OrderRuntime>,
}

pub(super) struct OrderCompletedHandler {
    pub(super) runtime: Arc<OrderRuntime>,
}

#[async_trait]
impl Handler<PlaceOrder> for PlaceOrderHandler {
    async fn handle(&self, order: PlaceOrder) -> CatgaResult<OrderAccepted> {
        if order.quantity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "an order must contain at least one item",
            ));
        }
        let total_cents = order
            .unit_price_cents
            .checked_mul(u64::from(order.quantity))
            .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "order total overflows u64"))?;
        let sequence = self.runtime.next_order_id.fetch_add(1, Ordering::Relaxed) + 1;
        let order_id: Box<str> = format!("order-{sequence}").into();
        self.runtime
            .mediator
            .send_command(RecordOrder {
                order_id: order_id.clone(),
                quantity: order.quantity,
                total_cents,
            })
            .await?;
        self.runtime.mediator.send(GetOrder { order_id }).await
    }
}

#[async_trait]
impl CommandHandler<RecordOrder> for RecordOrderHandler {
    async fn handle(&self, command: RecordOrder) -> CatgaResult<()> {
        if !self.runtime.node.is_leader() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "checkout must execute on the elected order-service leader",
            ));
        }
        let reserve_id = command.order_id.clone();
        let release_id = command.order_id.clone();
        let capture_id = command.order_id.clone();
        let refund_id = command.order_id.clone();
        let reserve = Arc::clone(&self.runtime);
        let release = Arc::clone(&self.runtime);
        let capture = Arc::clone(&self.runtime);
        let refund = Arc::clone(&self.runtime);
        let result = Flow::new("order-checkout")
            .step(
                move || {
                    let runtime = Arc::clone(&reserve);
                    let id = reserve_id.clone();
                    async move { runtime.reserve_inventory(&id) }
                },
                move || {
                    let runtime = Arc::clone(&release);
                    let id = release_id.clone();
                    async move { runtime.release_inventory(&id) }
                },
            )
            .step(
                move || {
                    let runtime = Arc::clone(&capture);
                    let id = capture_id.clone();
                    async move { runtime.capture_payment(&id) }
                },
                move || {
                    let runtime = Arc::clone(&refund);
                    let id = refund_id.clone();
                    async move { runtime.refund_payment(&id) }
                },
            )
            .run()
            .await;
        if let Some(error) = result.error() {
            return Err(error.clone());
        }

        let accepted = OrderAccepted {
            order_id: command.order_id.clone(),
            quantity: command.quantity,
            total_cents: command.total_cents,
        };
        let event = OrderCompleted {
            order_id: command.order_id.clone(),
            total_cents: command.total_cents,
        };
        let envelope = event_envelope(command.order_id.as_ref(), &event)?;
        self.runtime
            .event_store
            .append("orders", vec![envelope.clone()], None)
            .await?;
        self.runtime
            .outbox
            .enqueue(OutboxMessage::new(envelope))
            .await?;
        self.runtime.mediator.publish(event).await?;
        self.runtime
            .lock_orders()?
            .insert(command.order_id, accepted);
        OutboxProcessor::new(
            Arc::clone(&self.runtime.outbox),
            Arc::clone(&self.runtime.transport),
            "order-service-inline-flush",
            1,
        )?
        .flush_once()
        .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<GetOrder> for GetOrderHandler {
    async fn handle(&self, query: GetOrder) -> CatgaResult<OrderAccepted> {
        self.runtime
            .lock_orders()?
            .get(query.order_id.as_ref())
            .cloned()
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order read model is missing"))
    }
}

#[async_trait]
impl EventHandler<OrderCompleted> for OrderCompletedHandler {
    async fn handle(&self, _: OrderCompleted) -> CatgaResult<()> {
        self.runtime
            .completed_handlers
            .fetch_add(1, Ordering::Release);
        Ok(())
    }
}

fn event_envelope(order_id: &str, event: &OrderCompleted) -> CatgaResult<Envelope> {
    let payload = serde_json::to_vec(event).map_err(|error| {
        CatgaError::new(
            ErrorCode::SerializationFailed,
            "serialize completed-order outbox event",
        )
        .with_details(error.to_string())
    })?;
    let id = order_id
        .strip_prefix("order-")
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "order-service generated an invalid order identifier",
            )
        })?;
    Ok(Envelope::new(
        id,
        "order.completed",
        payload,
        MessageMetadata::new(id, None),
    ))
}
