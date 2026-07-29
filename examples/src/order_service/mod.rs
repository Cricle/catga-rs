//! A complete in-memory order service assembled from Catga components.
//!
//! Start it with `cargo run -p catga-examples --bin order_service`, then create an order with:
//!
//! ```text
//! curl -sS -X POST http://127.0.0.1:3000/orders \
//!   -H 'content-type: application/json' \
//!   -d '{"quantity":2,"unit_price_cents":1299}'
//! ```
//!
//! The service uses in-memory adapters only to keep the sample runnable. They deliberately make
//! process-loss semantics visible: replace the event store, outbox, transport, and cluster
//! coordinator with durable deployment adapters before treating the application as production
//! infrastructure.

mod app;
mod domain;
mod handlers;

pub use app::{OrderService, OrderServiceHealth, OrderServiceOptions};
pub use domain::OrderAccepted;
