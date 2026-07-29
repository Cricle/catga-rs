//! A complete Axum + Catga application demonstrating current best practices.
//!
//! Architecture at a glance:
//!
//! ```text
//! HTTP request
//!   → CorrelationLayer (assign/echo x-correlation-id)
//!   → TraceContextLayer (validate & scope W3C traceparent)
//!   → Axum router (standard handlers, any extractors)
//!   → MediatorState extractor (one Arc clone, zero reflection)
//!   → Mediator.send() (typed dispatch through Registry)
//!   → Business handler (pure async fn, testable without HTTP)
//!   → CatgaResult<T> → CatgaHttpError → structured JSON error
//! ```
//!
//! Run:
//! ```text
//! cargo run -p catga-examples --bin axum_checkout
//! ```
//!
//! Create an order:
//! ```text
//! curl -sS -X POST http://127.0.0.1:3000/orders \
//!   -H 'content-type: application/json' \
//!   -d '{"quantity":2,"unit_price_cents":1299}'
//! ```
//!
//! Query it back:
//! ```text
//! curl -sS http://127.0.0.1:3000/orders/order-1
//! ```
//!
//! Observe correlation echoing:
//! ```text
//! curl -sS -D- -X POST http://127.0.0.1:3000/orders \
//!   -H 'x-correlation-id: my-trace-42' \
//!   -H 'content-type: application/json' \
//!   -d '{"quantity":1,"unit_price_cents":500}'
//! # Response headers include: x-correlation-id: my-trace-42
//! ```

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::Path,
    routing::{get, post},
};
use catga_axum::{
    CatgaHttpError, CatgaHttpResult, CorrelationLayer, MediatorState, TraceContextLayer,
};
use catga_core::{
    CatgaError, CatgaResult, EndpointValidation, ErrorCode, Mediator, Message, Registry, Request,
    request_handler_with, validate_positive,
};
use serde::{Deserialize, Serialize};

// ===========================================================================
// Step 1: Define messages — the compile-time contract
// ===========================================================================

/// Inbound command: "create an order."
#[derive(Deserialize)]
struct CreateOrder {
    quantity: u32,
    unit_price_cents: u64,
}

impl Message for CreateOrder {}
impl Request for CreateOrder {
    type Response = OrderView;
}

/// Internal query message (never deserialized from HTTP — constructed in the handler).
struct GetOrder {
    order_id: String,
}

impl Message for GetOrder {}
impl Request for GetOrder {
    type Response = OrderView;
}

/// The API response model.
#[derive(Clone, Serialize)]
struct OrderView {
    order_id: String,
    quantity: u32,
    total_cents: u64,
}

// ===========================================================================
// Step 2: Business handlers — pure async fns, fully testable without HTTP
// ===========================================================================

/// In-memory storage. `std::sync::Mutex` is intentionally chosen over `tokio::Mutex`:
/// the critical section is a single Vec push/scan (nanoseconds, never awaits), so the
/// lighter std mutex outperforms the async-aware alternative.
type OrderStore = std::sync::Mutex<Vec<OrderView>>;

/// Locks the store, converting poisoning into a structured Catga error (zero panics).
fn lock_store(store: &OrderStore) -> CatgaResult<std::sync::MutexGuard<'_, Vec<OrderView>>> {
    store
        .lock()
        .map_err(|_| CatgaError::new(ErrorCode::Internal, "order store lock poisoned"))
}

/// Command handler with aggregated validation.
async fn place_order(store: Arc<OrderStore>, command: CreateOrder) -> CatgaResult<OrderView> {
    // Aggregate all validation failures into one 422 response.
    let mut validation = EndpointValidation::new();
    validation.add(validate_positive(command.quantity, "quantity"));
    validation.add(validate_positive(
        command.unit_price_cents,
        "unit_price_cents",
    ));
    validation.into_result()?;

    let total_cents = command
        .unit_price_cents
        .checked_mul(u64::from(command.quantity))
        .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "order total overflows u64"))?;

    let mut orders = lock_store(&store)?;
    let view = OrderView {
        order_id: format!("order-{}", orders.len() + 1),
        quantity: command.quantity,
        total_cents,
    };
    orders.push(view.clone());
    Ok(view)
}

/// Query handler: lookup by ID, structured 404 on miss.
async fn find_order(store: Arc<OrderStore>, query: GetOrder) -> CatgaResult<OrderView> {
    lock_store(&store)?
        .iter()
        .find(|o| o.order_id == query.order_id)
        .cloned()
        .ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "order not found")
                .with_details(format!("order_id={}", query.order_id))
        })
}

// ===========================================================================
// Step 3: Build the mediator — explicit registration, no macros, no discovery
// ===========================================================================

fn build_mediator() -> CatgaResult<Arc<Mediator>> {
    let store: Arc<OrderStore> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut registry = Registry::new();
    registry.register_request::<CreateOrder, _>(request_handler_with(
        Arc::clone(&store),
        place_order,
    ))?;
    registry.register_request::<GetOrder, _>(request_handler_with(store, find_order))?;

    Ok(Arc::new(Mediator::new(registry)))
}

// ===========================================================================
// Step 4: HTTP handlers — thin adapters, all logic lives in Step 2
// ===========================================================================

/// POST /orders → 201 Created.
async fn post_create_order(
    mediator: MediatorState,
    Json(command): Json<CreateOrder>,
) -> CatgaHttpResult<(axum::http::StatusCode, Json<OrderView>)> {
    let view = mediator.send(command).await.map_err(CatgaHttpError::from)?;
    Ok((axum::http::StatusCode::CREATED, Json(view)))
}

/// GET /orders/{id} → 200 OK or structured 404.
async fn get_order_by_id(
    mediator: MediatorState,
    Path(id): Path<String>,
) -> CatgaHttpResult<Json<OrderView>> {
    mediator
        .send(GetOrder { order_id: id })
        .await
        .map(Json)
        .map_err(CatgaHttpError::from)
}

// ===========================================================================
// Step 5: Router assembly + graceful shutdown
// ===========================================================================

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let app = Router::new()
        .route("/orders", post(post_create_order))
        .route("/orders/{id}", get(get_order_by_id))
        // Opt-in layers — omit either if not needed. Order: outermost first.
        .layer(TraceContextLayer::new())
        .layer(CorrelationLayer::new())
        // Mediator as state: MediatorState extracts it with one atomic Arc clone.
        .with_state(build_mediator()?);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .map_err(|e| {
            CatgaError::new(ErrorCode::Unavailable, "bind listener").with_details(e.to_string())
        })?;

    println!("checkout API listening on http://127.0.0.1:3000");
    println!("  POST /orders       — create (validates, returns 201)");
    println!("  GET  /orders/{{id}} — query (returns 200 or 404)");

    // Graceful shutdown: stop accepting on Ctrl+C, drain in-flight requests.
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            // If signal installation fails, proceed without graceful shutdown.
            let _ = tokio::signal::ctrl_c().await;
            println!("\nshutting down…");
        })
        .await
        .map_err(|e| {
            CatgaError::new(ErrorCode::Unavailable, "serve API").with_details(e.to_string())
        })
}
