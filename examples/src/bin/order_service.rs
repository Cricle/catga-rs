//! Runs the complete in-memory order-service application.
//!
//! Start the server with `cargo run -p catga-examples --bin order_service`, then submit an order:
//!
//! ```text
//! curl -sS -X POST http://127.0.0.1:3000/orders \
//!   -H 'content-type: application/json' \
//!   -d '{"quantity":2,"unit_price_cents":1299}'
//! ```
//!
//! See [`catga_examples::order_service`] for the adapter boundaries to replace in a deployment.

use catga_core::CatgaResult;
use catga_examples::order_service::{OrderService, OrderServiceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    OrderService::in_memory(OrderServiceOptions::default())?
        .serve("127.0.0.1:3000")
        .await
}
