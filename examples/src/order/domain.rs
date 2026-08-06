//! Order domain models and messages
//!
//! CQRS: Commands modify state, Queries read state, Events record changes.

use catga_core::{Command, Event, Message, Request};
use serde::{Deserialize, Serialize};

// ============================================================================
// Queries — intent to read state (responses are events)
// ============================================================================

/// Query to get full order details.
#[derive(Clone)]
pub struct GetOrder {
    /// The order identifier.
    pub order_id: Box<str>,
}

impl Message for GetOrder {}

impl Request for GetOrder {
    type Response = OrderPlaced;
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Query to get only the order status.
#[derive(Clone)]
pub struct GetOrderStatus {
    /// The order identifier.
    pub order_id: Box<str>,
}

impl Message for GetOrderStatus {}

impl Request for GetOrderStatus {
    type Response = OrderStatus;
    type TypeId = catga_core::DefaultMessageTypeId;
}

// ============================================================================
// Commands — intent to change state
// ============================================================================

/// Command to place a new order.
#[derive(Deserialize)]
pub struct PlaceOrder {
    /// Number of items to order.
    pub quantity: u32,
    /// Price per unit in cents.
    pub unit_price_cents: u64,
}

impl Message for PlaceOrder {}

impl Request for PlaceOrder {
    type Response = OrderPlaced;
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Command to confirm payment for an order.
#[derive(Clone)]
pub struct ConfirmPayment {
    /// The order identifier.
    pub order_id: Box<str>,
    /// The payment transaction identifier.
    pub payment_id: Box<str>,
}

impl Message for ConfirmPayment {}

impl Command for ConfirmPayment {
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Command to cancel a pending order.
#[derive(Clone)]
pub struct CancelOrder {
    /// The order identifier.
    pub order_id: Box<str>,
}

impl Message for CancelOrder {}

impl Command for CancelOrder {
    type TypeId = catga_core::DefaultMessageTypeId;
}

// ============================================================================
// Events — immutable facts that happened
// ============================================================================

/// Event emitted when an order is placed.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrderPlaced {
    /// The order identifier.
    pub order_id: Box<str>,
    /// Number of items ordered.
    pub quantity: u32,
    /// Total price in cents.
    pub total_cents: u64,
    /// Initial order status.
    pub status: OrderStatus,
}

impl Message for OrderPlaced {}

impl Event for OrderPlaced {
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Event emitted when payment is confirmed.
#[derive(Clone, Serialize, Deserialize)]
pub struct PaymentConfirmed {
    /// The order identifier.
    pub order_id: Box<str>,
    /// The payment transaction identifier.
    pub payment_id: Box<str>,
}

impl Message for PaymentConfirmed {}

impl Event for PaymentConfirmed {
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Event emitted when an order is cancelled.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrderCancelled {
    /// The order identifier.
    pub order_id: Box<str>,
    /// Reason for cancellation.
    pub reason: Box<str>,
}

impl Message for OrderCancelled {}

impl Event for OrderCancelled {
    type TypeId = catga_core::DefaultMessageTypeId;
}

// ============================================================================
// Domain models
// ============================================================================

/// Order lifecycle status.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Order placed, awaiting payment.
    #[default]
    Pending,
    /// Payment confirmed.
    Confirmed,
    /// Order cancelled.
    Cancelled,
}
