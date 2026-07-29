//! Integration tests for the composable Axum primitives: MediatorState, CorrelationLayer,
//! and TraceContextLayer.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{FromRef, Path, Query},
    http::{Request as AxumRequest, StatusCode},
    routing::{get, post},
};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaHttpError, CorrelationLayer, IntoCatgaHttpResponse, MediatorState,
    TraceContextLayer,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Message, Registry, Request,
    current_transport_context,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test messages
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
struct AddRequest {
    a: u32,
    b: u32,
}

impl Message for AddRequest {}
impl Request for AddRequest {
    type Response = u32;
}

struct AddHandler;

#[async_trait]
impl Handler<AddRequest> for AddHandler {
    async fn handle(&self, request: AddRequest) -> CatgaResult<u32> {
        Ok(request.a + request.b)
    }
}

#[derive(Deserialize)]
struct MultiplyQuery {
    factor: u32,
}

#[derive(Deserialize, Serialize)]
struct MultiplyRequest {
    value: u32,
    factor: u32,
}

impl Message for MultiplyRequest {}
impl Request for MultiplyRequest {
    type Response = u32;
}

struct MultiplyHandler;

#[async_trait]
impl Handler<MultiplyRequest> for MultiplyHandler {
    async fn handle(&self, request: MultiplyRequest) -> CatgaResult<u32> {
        Ok(request.value * request.factor)
    }
}

#[derive(Deserialize, Serialize)]
struct TraceProbe;

impl Message for TraceProbe {}
impl Request for TraceProbe {
    type Response = Option<String>;
}

struct TraceProbeHandler;

#[async_trait]
impl Handler<TraceProbe> for TraceProbeHandler {
    async fn handle(&self, _: TraceProbe) -> CatgaResult<Option<String>> {
        Ok(current_transport_context().and_then(|ctx| {
            ctx.headers()
                .and_then(|headers| headers.get("traceparent"))
                .map(str::to_owned)
        }))
    }
}

fn test_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<AddRequest, _>(AddHandler)
        .unwrap();
    registry
        .register_request::<MultiplyRequest, _>(MultiplyHandler)
        .unwrap();
    registry
        .register_request::<TraceProbe, _>(TraceProbeHandler)
        .unwrap();
    Arc::new(Mediator::new(registry))
}

// ---------------------------------------------------------------------------
// MediatorState extractor tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mediator_state_extracts_from_arc_mediator_state() {
    async fn add(
        mediator: MediatorState,
        Json(request): Json<AddRequest>,
    ) -> Result<Json<u32>, catga_axum::CatgaHttpError> {
        mediator.send(request).await.map(Json).map_err(Into::into)
    }

    let app = Router::new()
        .route("/add", post(add))
        .with_state(test_mediator());

    let request = AxumRequest::post("/add")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"a":20,"b":22}"#))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "42");
}

#[tokio::test]
async fn mediator_state_combines_with_path_and_query_extractors() {
    async fn multiply(
        mediator: MediatorState,
        Path(value): Path<u32>,
        Query(query): Query<MultiplyQuery>,
    ) -> Result<Json<u32>, catga_axum::CatgaHttpError> {
        mediator
            .send(MultiplyRequest {
                value,
                factor: query.factor,
            })
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/multiply/{value}", get(multiply))
        .with_state(test_mediator());

    let request = AxumRequest::get("/multiply/6?factor=7")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "42");
}

#[tokio::test]
async fn mediator_state_returns_structured_error_for_unregistered_requests() {
    #[derive(Deserialize, Serialize)]
    struct Unknown;
    impl Message for Unknown {}
    impl Request for Unknown {
        type Response = ();
    }

    async fn unknown(mediator: MediatorState) -> Result<Json<()>, catga_axum::CatgaHttpError> {
        mediator.send(Unknown).await.map(Json).map_err(Into::into)
    }

    let app = Router::new()
        .route("/unknown", post(unknown))
        .with_state(test_mediator());

    let request = AxumRequest::post("/unknown").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert!(body.starts_with(br#"{"code":"#));
}

// ---------------------------------------------------------------------------
// CorrelationLayer tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn correlation_layer_echoes_an_inbound_correlation_header() {
    async fn noop() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/noop", post(noop))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::post("/noop")
        .header(CORRELATION_ID_HEADER, "req-abc-123")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("req-abc-123")
    );
}

#[tokio::test]
async fn correlation_layer_generates_a_numeric_id_when_no_header_is_present() {
    async fn noop() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/noop", post(noop))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::post("/noop").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();

    let header = response
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .expect("correlation header must be generated");
    assert!(
        header.parse::<u64>().is_ok(),
        "generated correlation must be numeric, got: {header}"
    );
}

// ---------------------------------------------------------------------------
// TraceContextLayer tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trace_context_layer_scopes_valid_w3c_traceparent_through_mediator() {
    async fn probe(
        mediator: MediatorState,
    ) -> Result<Json<Option<String>>, catga_axum::CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .with_state(test_mediator());

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let request = AxumRequest::post("/probe")
        .header("traceparent", traceparent)
        .header("tracestate", "vendor=state")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(result.as_deref(), Some(traceparent));
}

#[tokio::test]
async fn trace_context_layer_leaves_invalid_traceparent_unscoped() {
    async fn probe(
        mediator: MediatorState,
    ) -> Result<Json<Option<String>>, catga_axum::CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .with_state(test_mediator());

    let request = AxumRequest::post("/probe")
        .header("traceparent", "invalid-garbage")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(result, None);
}

// ---------------------------------------------------------------------------
// Combined layers test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn correlation_and_trace_layers_compose_on_the_same_router() {
    async fn probe(
        mediator: MediatorState,
    ) -> Result<Json<Option<String>>, catga_axum::CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .layer(CorrelationLayer::new())
        .with_state(test_mediator());

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let request = AxumRequest::post("/probe")
        .header("traceparent", traceparent)
        .header(CORRELATION_ID_HEADER, "combined-42")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("combined-42")
    );
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(result.as_deref(), Some(traceparent));
}

// ---------------------------------------------------------------------------
// ErrorCode::http_status_u16() exhaustive verification
// ---------------------------------------------------------------------------

#[test]
fn http_status_u16_covers_every_error_code_variant() {
    let cases: &[(ErrorCode, u16)] = &[
        (ErrorCode::Validation, 422),
        (ErrorCode::HandlerFailed, 400),
        (ErrorCode::HandlerNotFound, 404),
        (ErrorCode::PipelineFailed, 400),
        (ErrorCode::PersistenceFailed, 503),
        (ErrorCode::LockFailed, 503),
        (ErrorCode::TransportFailed, 503),
        (ErrorCode::SerializationFailed, 400),
        (ErrorCode::NotFound, 404),
        (ErrorCode::Conflict, 409),
        (ErrorCode::Unauthorized, 401),
        (ErrorCode::Forbidden, 403),
        (ErrorCode::Cancelled, 408),
        (ErrorCode::Timeout, 408),
        (ErrorCode::FlowFailed, 400),
        (ErrorCode::FlowCancelled, 408),
        (ErrorCode::FlowTimeout, 408),
        (ErrorCode::FlowCompensating, 400),
        (ErrorCode::Unsupported, 501),
        (ErrorCode::Transient, 503),
        (ErrorCode::Unavailable, 503),
        (ErrorCode::Internal, 500),
    ];
    for (code, expected) in cases {
        assert_eq!(code.http_status_u16(), *expected, "{code:?}");
    }
}

#[tokio::test]
async fn catga_http_error_body_is_strict_json_with_code_and_message() {
    use axum::response::IntoResponse;

    let error = CatgaError::new(ErrorCode::Conflict, "order already exists");
    let response = CatgaHttpError::from(error).into_response();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["code"], "conflict");
    assert_eq!(parsed["message"], "order already exists");
    // Strictly two keys, no extra fields leaked.
    assert_eq!(parsed.as_object().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// CorrelationLayer strict edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn correlation_layer_ignores_an_empty_header_and_generates_a_fallback() {
    async fn noop() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/noop", post(noop))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::post("/noop")
        .header(CORRELATION_ID_HEADER, "")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    let header = response
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .expect("a fallback correlation must be generated for an empty header");
    assert!(
        header.parse::<u64>().is_ok(),
        "empty header must produce a numeric fallback, got: {header}"
    );
}

#[tokio::test]
async fn correlation_layer_preserves_opaque_non_numeric_values_verbatim() {
    async fn noop() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/noop", post(noop))
        .layer(CorrelationLayer::new());

    let opaque = "browser-7f3a9c2e-4b1d-4e8f-a6c5-2d9e0f1a3b4c";
    let request = AxumRequest::post("/noop")
        .header(CORRELATION_ID_HEADER, opaque)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some(opaque)
    );
}

// ---------------------------------------------------------------------------
// TraceContextLayer strict edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trace_context_layer_discards_invalid_tracestate_but_retains_valid_parent() {
    async fn probe(mediator: MediatorState) -> Result<Json<Option<String>>, CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .with_state(test_mediator());

    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let request = AxumRequest::post("/probe")
        .header("traceparent", traceparent)
        .header("tracestate", "INVALID_UPPER=value")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    // Parent is still scoped even though tracestate was invalid.
    assert_eq!(result.as_deref(), Some(traceparent));
}

#[tokio::test]
async fn trace_context_layer_rejects_version_ff_traceparent() {
    async fn probe(mediator: MediatorState) -> Result<Json<Option<String>>, CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .with_state(test_mediator());

    // Version ff is explicitly forbidden by W3C spec.
    let request = AxumRequest::post("/probe")
        .header(
            "traceparent",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(result, None, "version ff must be rejected");
}

#[tokio::test]
async fn trace_context_layer_rejects_all_zero_trace_id() {
    async fn probe(mediator: MediatorState) -> Result<Json<Option<String>>, CatgaHttpError> {
        mediator
            .send(TraceProbe)
            .await
            .map(Json)
            .map_err(Into::into)
    }

    let app = Router::new()
        .route("/probe", post(probe))
        .layer(TraceContextLayer::new())
        .with_state(test_mediator());

    let request = AxumRequest::post("/probe")
        .header(
            "traceparent",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        )
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let result: Option<String> = serde_json::from_slice(&body).unwrap();
    assert_eq!(result, None, "all-zero trace-id must be rejected");
}

// ---------------------------------------------------------------------------
// MediatorState with custom AppState via FromRef
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CustomAppState {
    mediator: Arc<Mediator>,
    #[allow(dead_code)]
    app_name: &'static str,
}

impl FromRef<CustomAppState> for Arc<Mediator> {
    fn from_ref(state: &CustomAppState) -> Self {
        Arc::clone(&state.mediator)
    }
}

#[tokio::test]
async fn mediator_state_extracts_from_a_custom_app_state_via_from_ref() {
    async fn add(
        mediator: MediatorState,
        Json(request): Json<AddRequest>,
    ) -> Result<Json<u32>, CatgaHttpError> {
        mediator.send(request).await.map(Json).map_err(Into::into)
    }

    let state = CustomAppState {
        mediator: test_mediator(),
        app_name: "test-app",
    };
    let app = Router::new().route("/add", post(add)).with_state(state);

    let request = AxumRequest::post("/add")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"a":100,"b":200}"#))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "300");
}

// ---------------------------------------------------------------------------
// IntoCatgaHttpResponse strict behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn into_catga_response_maps_errors_regardless_of_success_status() {
    let result: CatgaResult<u32> = Err(CatgaError::new(ErrorCode::Forbidden, "denied"));
    let response = result.into_catga_response(StatusCode::OK);

    // Error wins: the requested 200 is ignored.
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body, r#"{"code":"forbidden","message":"denied"}"#);
}

#[tokio::test]
async fn into_catga_created_rejects_invalid_location_header_without_panicking() {
    let result: CatgaResult<u32> = Ok(42);
    let response = result.into_catga_created("/orders\n7");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(
        body,
        r#"{"code":"internal","message":"invalid Location header"}"#
    );
}

// ---------------------------------------------------------------------------
// CORRELATION_ID_HEADER is the same constant from catga_core
// ---------------------------------------------------------------------------

#[test]
fn correlation_header_constant_matches_core() {
    assert_eq!(CORRELATION_ID_HEADER, catga_core::CORRELATION_ID_HEADER);
    assert_eq!(CORRELATION_ID_HEADER, "x-correlation-id");
}

// ---------------------------------------------------------------------------
// Validation re-exported from catga_core works through catga_axum
// ---------------------------------------------------------------------------

#[test]
fn validation_reexport_produces_correct_error_codes() {
    use catga_axum::{EndpointValidation, validate_positive, validate_required};

    let mut validation = EndpointValidation::new();
    validation.add(validate_required(None, "name"));
    validation.add(validate_positive(0u32, "quantity"));
    assert_eq!(validation.len(), 2);

    let error = validation.into_result().unwrap_err();
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(error.message().contains("name is required"));
    assert!(error.message().contains("quantity must be positive"));
}
