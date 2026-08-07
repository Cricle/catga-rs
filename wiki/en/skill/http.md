# HTTP Integration (catga-axum)

The Axum adapter provides composable primitives; server lifecycle, request size limits, and authentication remain owned by the caller.

## 1. Recommended Approach: Standard Axum + `MediatorState`

`MediatorState` is a standard extractor that composes with any combination of `Path`/`Query`/`State`:

```rust,ignore
use axum::{Json, Router, extract::Path, routing::post};
use catga_axum::{CatgaHttpResult, MediatorState};

async fn place_order(
    MediatorState(mediator): MediatorState,
    Json(command): Json<PlaceOrder>,
) -> CatgaHttpResult<Json<OrderAck>> {
    mediator.send(command).await.map(Json).map_err(Into::into)
}

let app = Router::new()
    .route("/orders", post(place_order))
    .layer(catga_axum::CorrelationLayer)        // Correlation ID propagation (see below)
    .layer(catga_axum::TraceContextLayer)       // W3C traceparent/tracestate propagation
    .with_state(AppState { mediator });
```

## 2. Error and Response Mapping

- `CatgaHttpResult<T> = Result<T, CatgaHttpError>` — handler returns directly; `CatgaError` maps status codes via `ErrorCode::http_status_u16()`, body is compact JSON `{ code, message }`.
- `IntoCatgaHttpResponse` (`CatgaResult<T: Serialize>` extension):
  - `.into_catga_response(StatusCode::OK)` — custom success status; `204 NO_CONTENT` has no body.
  - `.into_catga_created("/orders/42")` — `201 Created` + `Location` header.
- `axum::middleware::from_fn(endpoint_panic_middleware)` — converts handler panics into stable internal-error responses (opt-in).

## 3. Context Propagation

- `CorrelationLayer` / `TraceContextLayer` — tower layer form (preferred for new code).
- Outbound HTTP: `CorrelationHttpClient` preserves correlation/trace headers provided by the caller; ambient Catga context fills gaps only; can also manually call `propagate_correlation_header(&mut headers)` / `propagate_trace_context_headers(&mut headers)`.
- **Trust boundary**: Inbound correlation headers are always treated as untrusted until application middleware validates/replaces them.

## 4. Convenience Macros (prototyping/small services; not required)

```rust,ignore
// Handlers + mediator + typed routing in one composition
let app = catga_axum::catga_application! {
    handlers {
        request GetOrder => GetOrderHandler;
        command PlaceOrder => PlaceOrderHandler;
        event OrderCreated => [ProjectionHandler];
    }
    routes {
        requests {
            @post "/orders/get" => GetOrder,
            "/orders" => PlaceOrder,          // Default POST
        }
        events {
            "/events/order-created" => OrderCreated,
        }
    }
}?;
// app.mediator() / app.router() for application-owned servers

// Routing only: catga_routes! { mediator = m; requests { .. } events { .. } }
// Native axum routing: axum_routes! { router; GET "/healthz" => health, .. }
// OpenAPI metadata: catga_endpoint_metadata! { commands { .. } queries { .. } events { .. } }
```

Single route functions: `mediator_route` / `mediator_route_with_method` / `event_route` / `event_route_with_method`.

## 5. Cluster/Raft Routing

- `leader_forward_route` / `leader_forward_route_at` — follower forwards to leader (works with `HttpClusterForwarder`, see [distributed.md](distributed.md)).
- `raft_message_route` — Raft protocol message HTTP entry point (`RAFT_MESSAGE_PATH = "/api/catga/raft"`, frame limit `MAX_RAFT_MESSAGE_BYTES = 1 MiB`). **Must** be preceded by mTLS/signed frame authentication + `RaftPeerIdentity` + `StaticRaftInboundPolicy`.
- `HttpRaftTransport` — HTTP implementation of `RaftTransport`.

## 6. Endpoint Validation

`EndpointValidation` and validation functions (`validate_required` / `validate_not_empty` / `validate_min_length` / `validate_max_length` / `validate_min_count` / `validate_positive` / `validate_range`) are re-exported from catga-core, used for input validation at handler entry (failure → `ErrorCode::Validation` → HTTP 422).
