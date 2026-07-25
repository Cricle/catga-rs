# Axum Endpoint Panic Middleware Design

## Goal

Provide the Rust runtime equivalent of the upstream endpoint exception
middleware: an application may opt into one Axum middleware boundary that
turns an endpoint-handler unwind into Catga's stable internal-error response.

## API

`catga-axum` will export:

```rust
pub async fn endpoint_panic_middleware(request: AxumRequest, next: Next) -> Response;
```

Applications install it explicitly with
`middleware::from_fn(endpoint_panic_middleware)`. The middleware awaits
`next.run(request)` under `AssertUnwindSafe(...).catch_unwind()` and returns
the unmodified response when the downstream handler completes normally.

When an unwind is caught, it returns the existing `CatgaHttpError` conversion
of `CatgaError::new(ErrorCode::Internal, "endpoint handler panicked")`. This
gives callers the same compact, stable error schema as every other Catga HTTP
failure without exposing panic payloads or implementation types to clients.

## Boundaries

This is intentionally opt-in and is not a replacement for returning
`CatgaResult` from ordinary handlers. It cannot catch abort-mode panics or
repair a response whose headers/body have already been sent. Applications
needing bespoke panic telemetry or response formats retain Axum's standard
layer composition points; Rust does not need a global DI serializer registry.

The successful path introduces no application-data allocation. The panic path
allocates only the structured error message required to build its JSON
response.

## Tests

Integration tests create Routers with deliberately panicking and successful
endpoints, install the middleware, and dispatch in-process requests. They
verify the panic path returns a `500 Internal Server Error` with the stable
Catga JSON body and that a normal `202` JSON response is unchanged.
