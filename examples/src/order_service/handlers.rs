use std::{sync::Arc, sync::atomic::Ordering};

use catga_cluster::ClusterCoordinator;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, EventStore, MessageMetadata, OutboxMessage,
    OutboxProcessor, OutboxStore,
};
use catga_flow::compensating_flow;

use super::{
    app::OrderRuntime,
    domain::{GetOrder, OrderAccepted, OrderCompleted, PlaceOrder, RecordOrder},
};

pub(super) async fn place_order(
    runtime: Arc<OrderRuntime>,
    order: PlaceOrder,
) -> CatgaResult<OrderAccepted> {
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
    let sequence = runtime.next_order_id.fetch_add(1, Ordering::Relaxed) + 1;
    let order_id: Box<str> = format!("order-{sequence}").into();
    runtime
        .mediator
        .send_command(RecordOrder {
            order_id: order_id.clone(),
            quantity: order.quantity,
            total_cents,
        })
        .await?;
    runtime.mediator.send(GetOrder { order_id }).await
}

pub(super) async fn record_order(
    runtime: Arc<OrderRuntime>,
    command: RecordOrder,
) -> CatgaResult<()> {
    if !runtime.node.is_leader() {
        return Err(CatgaError::new(
            ErrorCode::Unavailable,
            "checkout must execute on the elected order-service leader",
        ));
    }
    let result = compensating_flow! {
        "order-checkout";
        context = Checkout::new(Arc::clone(&runtime), command.order_id.clone());
        steps {
            reserve_inventory => release_inventory;
            capture_payment => refund_payment;
        }
    }
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
    runtime
        .event_store
        .append("orders", vec![envelope.clone()], None)
        .await?;
    runtime.outbox.enqueue(OutboxMessage::new(envelope)).await?;
    runtime.mediator.publish(event).await?;
    runtime.lock_orders()?.insert(command.order_id, accepted);
    OutboxProcessor::new(
        Arc::clone(&runtime.outbox),
        Arc::clone(&runtime.transport),
        "order-service-inline-flush",
        1,
    )?
    .flush_once()
    .await?;
    Ok(())
}

/// The checkout business context shared by every forward action and compensation.
#[derive(Clone)]
struct Checkout {
    runtime: Arc<OrderRuntime>,
    order_id: Box<str>,
}

impl Checkout {
    fn new(runtime: Arc<OrderRuntime>, order_id: Box<str>) -> Self {
        Self { runtime, order_id }
    }

    async fn reserve_inventory(self) -> CatgaResult<()> {
        self.runtime.reserve_inventory(&self.order_id)
    }

    async fn release_inventory(self) -> CatgaResult<()> {
        self.runtime.release_inventory(&self.order_id)
    }

    async fn capture_payment(self) -> CatgaResult<()> {
        self.runtime.capture_payment(&self.order_id)
    }

    async fn refund_payment(self) -> CatgaResult<()> {
        self.runtime.refund_payment(&self.order_id)
    }
}

pub(super) async fn get_order(
    runtime: Arc<OrderRuntime>,
    query: GetOrder,
) -> CatgaResult<OrderAccepted> {
    runtime
        .lock_orders()?
        .get(query.order_id.as_ref())
        .cloned()
        .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order read model is missing"))
}

pub(super) async fn project_completed(
    runtime: Arc<OrderRuntime>,
    _: OrderCompleted,
) -> CatgaResult<()> {
    runtime.completed_handlers.fetch_add(1, Ordering::Release);
    Ok(())
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
