//! Starts a typed Axum checkout endpoint backed by Catga's CQRS mediator.
//!
//! Run it with `cargo run -p catga-examples --bin axum_checkout`, then submit a request:
//! `curl -sS -X POST http://127.0.0.1:3000/orders -H 'content-type: application/json' \
//!   -d '{"quantity":2,"unit_price_cents":1299}'`.
//! The route is declared with [`catga_axum::catga_routes!`], so changing the request type or
//! handler registration remains a compile-time checked startup operation.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Message, Request, catga_handlers,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct CreateOrder {
    quantity: u32,
    unit_price_cents: u64,
}

impl Message for CreateOrder {}

impl Request for CreateOrder {
    type Response = OrderAccepted;
}

#[derive(Serialize)]
struct OrderAccepted {
    order_id: String,
    total_cents: u64,
}

struct CreateOrderHandler;

#[async_trait]
impl Handler<CreateOrder> for CreateOrderHandler {
    async fn handle(&self, order: CreateOrder) -> CatgaResult<OrderAccepted> {
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
        Ok(OrderAccepted {
            order_id: format!("order-{total_cents}"),
            total_cents,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mediator = Arc::new(Mediator::new(
        catga_handlers! { request CreateOrder => CreateOrderHandler }?,
    ));
    let app = catga_axum::catga_routes! {
        mediator = mediator;
        requests { @post "/orders" => CreateOrder }
        events {}
    }?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|error| {
            CatgaError::new(ErrorCode::Unavailable, "bind Axum checkout listener")
                .with_details(error.to_string())
        })?;
    println!("checkout API listening on http://127.0.0.1:3000/orders");
    axum::serve(listener, app).await.map_err(|error| {
        CatgaError::new(ErrorCode::Unavailable, "serve Axum checkout API")
            .with_details(error.to_string())
    })
}
