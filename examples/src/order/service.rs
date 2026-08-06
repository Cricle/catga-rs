//! Order service — CQRS + Event Sourcing best practices
//!
//! - Commands: modify state via compensating flows
//! - Queries: read from materialized view
//! - Events: immutable facts stored in event store
//!
//! ```bash
//! cargo run -p catga-examples --bin order
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use catga_core::compensating_flow;
use catga_core::{catga_service, CatgaError, CatgaResult, ErrorCode, Event};
use tokio::sync::RwLock;

use super::domain::*;

/// In-memory order state store.
pub type OrderStore = Arc<RwLock<HashMap<Box<str>, OrderState>>>;

/// In-memory event log.
pub type EventLog = Arc<RwLock<Vec<EventRecord>>>;

/// Current state of an order.
#[derive(Clone)]
pub struct OrderState {
    /// The order identifier.
    pub order_id: Box<str>,
    /// Number of items.
    pub quantity: u32,
    /// Total price in cents.
    pub total_cents: u64,
    /// Current status.
    pub status: OrderStatus,
    /// Payment transaction ID if captured.
    pub payment_id: Option<Box<str>>,
}

/// A recorded event for event sourcing.
#[derive(Clone)]
pub struct EventRecord {
    /// The order this event belongs to.
    pub order_id: Box<str>,
    /// Event type name.
    pub event_type: Box<str>,
    /// Serialized event payload.
    pub data: Box<[u8]>,
}

/// Order service with CQRS handlers and event sourcing.
#[derive(Clone, Default)]
pub struct OrderService {
    orders: OrderStore,
    events: EventLog,
}

/// Order mediator handlers.
///
/// # Handlers
///
/// - Queries: `get_order`, `get_order_status`
/// - Commands: `place_order`, `confirm_payment`, `cancel_order`
/// - Events: `on_payment_confirmed`
#[allow(missing_docs)]
#[catga_service(OrderMediator)]
impl OrderService {
    // Query handlers
    async fn get_order(&self, query: GetOrder) -> CatgaResult<OrderPlaced> {
        let orders = self.orders.read().await;
        orders
            .get(query.order_id.as_ref())
            .map(|s| OrderPlaced {
                order_id: s.order_id.clone(),
                quantity: s.quantity,
                total_cents: s.total_cents,
                status: s.status,
            })
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order not found"))
    }

    async fn get_order_status(&self, query: GetOrderStatus) -> CatgaResult<OrderStatus> {
        let orders = self.orders.read().await;
        orders
            .get(query.order_id.as_ref())
            .map(|s| s.status)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order not found"))
    }

    // Command handlers
    async fn place_order(&self, cmd: PlaceOrder) -> CatgaResult<OrderPlaced> {
        if cmd.quantity == 0 {
            return Err(CatgaError::new(ErrorCode::Validation, "quantity must be at least 1"));
        }
        let total_cents = cmd
            .unit_price_cents
            .checked_mul(u64::from(cmd.quantity))
            .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "total overflow"))?;

        let order_id: Box<str> = format!("order-{}", uuid_simple()).into();
        let state = OrderState {
            order_id: order_id.clone(),
            quantity: cmd.quantity,
            total_cents,
            status: OrderStatus::Pending,
            payment_id: None,
        };

        self.orders.write().await.insert(order_id.clone(), state.clone());

        let event = OrderPlaced {
            order_id: order_id.clone(),
            quantity: cmd.quantity,
            total_cents,
            status: OrderStatus::Pending,
        };
        self.record_event(&order_id, &event).await?;

        Ok(event)
    }

    async fn confirm_payment(&self, cmd: ConfirmPayment) -> CatgaResult<()> {
        let result = compensating_flow!("payment"; context = PaymentCtx {
            orders: Arc::clone(&self.orders),
            order_id: cmd.order_id.clone(),
            payment_id: cmd.payment_id.clone(),
        }; steps {
            reserve => release;
        })
        .run()
        .await;

        if result.is_success() {
            // Update state and emit event
            {
                let mut orders = self.orders.write().await;
                if let Some(order) = orders.get_mut(cmd.order_id.as_ref()) {
                    order.status = OrderStatus::Confirmed;
                    order.payment_id = Some(cmd.payment_id.clone());
                }
            }

            let event = PaymentConfirmed {
                order_id: cmd.order_id.clone(),
                payment_id: cmd.payment_id.clone(),
            };
            self.record_event(&cmd.order_id, &event).await?;
            Ok(())
        } else {
            Err(result
                .error()
                .cloned()
                .unwrap_or_else(|| CatgaError::new(ErrorCode::Internal, "payment flow failed")))
        }
    }

    async fn cancel_order(&self, cmd: CancelOrder) -> CatgaResult<()> {
        let mut orders = self.orders.write().await;
        let order = orders
            .get_mut(cmd.order_id.as_ref())
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order not found"))?;

        if order.status != OrderStatus::Pending {
            return Err(CatgaError::new(ErrorCode::Validation, "can only cancel pending orders"));
        }

        order.status = OrderStatus::Cancelled;

        let event = OrderCancelled {
            order_id: cmd.order_id.clone(),
            reason: "user requested".into(),
        };
        self.record_event(&cmd.order_id, &event).await?;
        Ok(())
    }

    // Event handler
    async fn on_payment_confirmed(&self, event: PaymentConfirmed) -> CatgaResult<()> {
        let mut orders = self.orders.write().await;
        if let Some(order) = orders.get_mut(event.order_id.as_ref()) {
            order.status = OrderStatus::Confirmed;
            order.payment_id = Some(event.payment_id);
        }
        Ok(())
    }
}

impl OrderService {
    /// Creates a new order service.
    pub fn new() -> Self {
        Self {
            orders: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn record_event<E: Event + serde::Serialize>(
        &self,
        order_id: &str,
        event: &E,
    ) -> CatgaResult<()> {
        let data = serde_json::to_vec(event)
            .map_err(|_| CatgaError::new(ErrorCode::SerializationFailed, "serialize event"))?;
        let event_type = <E::TypeId as catga_core::MessageTypeId>::NAME;
        let record = EventRecord {
            order_id: order_id.into(),
            event_type: event_type.into(),
            data: data.into_boxed_slice(),
        };
        self.events.write().await.push(record);
        Ok(())
    }

    /// Returns all recorded events.
    pub async fn get_events(&self) -> Vec<EventRecord> {
        self.events.read().await.clone()
    }
}

struct PaymentCtx {
    orders: OrderStore,
    order_id: Box<str>,
    payment_id: Box<str>,
}

impl Clone for PaymentCtx {
    fn clone(&self) -> Self {
        Self {
            orders: Arc::clone(&self.orders),
            order_id: self.order_id.clone(),
            payment_id: self.payment_id.clone(),
        }
    }
}

impl PaymentCtx {
    async fn reserve(self) -> CatgaResult<()> {
        let mut orders = self.orders.write().await;
        let order = orders
            .get_mut(self.order_id.as_ref())
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "order not found"))?;
        if order.payment_id.is_some() {
            return Err(CatgaError::new(ErrorCode::Conflict, "payment already captured"));
        }
        order.payment_id = Some(self.payment_id);
        Ok(())
    }

    async fn release(self) -> CatgaResult<()> {
        let mut orders = self.orders.write().await;
        if let Some(order) = orders.get_mut(self.order_id.as_ref()) {
            order.payment_id = None;
        }
        Ok(())
    }
}

fn uuid_simple() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
        % 1_000_000
}
