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

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_examples::order_service::{OrderService, OrderServiceOptions};

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let service = OrderService::in_memory(OrderServiceOptions::default())?;
    let app = service.router()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|error| {
            CatgaError::new(ErrorCode::Unavailable, "bind order-service listener")
                .with_details(error.to_string())
        })?;
    println!("order service listening on http://127.0.0.1:3000/orders");
    axum::serve(listener, app).await.map_err(|error| {
        CatgaError::new(ErrorCode::Unavailable, "serve order-service API")
            .with_details(error.to_string())
    })
}
