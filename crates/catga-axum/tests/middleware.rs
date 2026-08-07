//! Tests for HTTP middleware behavior: CorrelationLayer, TraceContextLayer, and
//! the standalone correlation/panic middleware functions.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::State,
    http::{Request as AxumRequest, StatusCode},
    routing::{get, post},
    Json, Router,
};
use catga_axum::{
    correlation_middleware, correlation_id, endpoint_panic_middleware,
    propagate_correlation_header, propagate_trace_context_headers, CorrelationLayer,
    IntoCatgaHttpResponse, MediatorState, TraceContextLayer, CORRELATION_ID_HEADER,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Handler, Mediator, Message, Registry, Request,
    current_correlation_id, current_correlation_value, current_transport_context,
    scope_correlation_id, scope_correlation_value,
};
use http::{HeaderMap, HeaderValue, Method};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Test messages
// ---------------------------------------------------------------------------

#[derive(Deserialize, Serialize)]
struct EchoRequest {
    value: String,
}

impl Message for EchoRequest {}
impl Request for EchoRequest {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct EchoHandler;

#[async_trait]
impl Handler<EchoRequest> for EchoHandler {
    async fn handle(&self, request: EchoRequest) -> CatgaResult<String> {
        Ok(request.value)
    }
}

fn test_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry.register_request::<EchoRequest, _>(EchoHandler).unwrap();
    Arc::new(Mediator::new(registry))
}

// ---------------------------------------------------------------------------
// CorrelationLayer integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn correlation_layer_scopes_numeric_correlation_id() {
    async fn check_correlation() -> String {
        current_correlation_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_correlation()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::get("/check")
        .header(CORRELATION_ID_HEADER, "12345")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body, "12345");
}

#[tokio::test]
async fn correlation_layer_scopes_correlation_value() {
    async fn check_value() -> String {
        current_correlation_value()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_value()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::get("/check")
        .header(CORRELATION_ID_HEADER, "uuid-abcd-1234")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body, "uuid-abcd-1234");
}

#[tokio::test]
async fn correlation_layer_generates_numeric_id_when_missing() {
    async fn check_correlation() -> String {
        current_correlation_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_correlation()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::get("/check").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let correlation = String::from_utf8(body.to_vec()).unwrap();
    // Generated IDs should be positive integers
    assert!(correlation.parse::<u64>().is_ok());
    assert!(correlation.parse::<u64>().unwrap() > 0);
}

#[tokio::test]
async fn correlation_layer_preserves_existing_numeric_header() {
    async fn handler() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/test", get(handler))
        .layer(CorrelationLayer::new());

    let request = AxumRequest::get("/test")
        .header(CORRELATION_ID_HEADER, "999")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("999")
    );
}

#[tokio::test]
async fn correlation_layer_does_not_override_existing_value() {
    async fn check_value() -> String {
        current_correlation_value()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_value()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(CorrelationLayer::new());

    // Send opaque (non-numeric) correlation value
    let request = AxumRequest::get("/check")
        .header(CORRELATION_ID_HEADER, "opaque-correlation-value")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(body, "opaque-correlation-value");
}

// ---------------------------------------------------------------------------
// TraceContextLayer integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trace_context_layer_scopes_valid_traceparent() {
    async fn check_trace() -> Option<String> {
        current_transport_context()
            .and_then(|ctx| ctx.headers()?.get("traceparent"))
            .map(str::to_owned)
    }

    async fn handler() -> String {
        check_trace().unwrap_or_else(|| "none".to_string())
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let request = AxumRequest::get("/check")
        .header("traceparent", traceparent)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), traceparent);
}

#[tokio::test]
async fn trace_context_layer_scopes_traceparent_with_tracestate() {
    async fn check_trace() -> (Option<String>, Option<String>) {
        let ctx = current_transport_context()?;
        let traceparent = ctx.headers()?.get("traceparent").map(str::to_owned);
        let tracestate = ctx.headers()?.get("tracestate").map(str::to_owned);
        Some((traceparent, tracestate)).unwrap_or((None, None))
    }

    async fn handler() -> String {
        check_trace()
            .map(|(tp, ts)| format!("{:?}|{:?}", tp, ts))
            .unwrap_or_else(|| "none".to_string())
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let tracestate = "congo=t61rcWkgMzE";
    let request = AxumRequest::get("/check")
        .header("traceparent", traceparent)
        .header("tracestate", tracestate)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let result = String::from_utf8(body.to_vec()).unwrap();
    assert!(result.contains(traceparent));
    assert!(result.contains("congo"));
}

#[tokio::test]
async fn trace_context_layer_ignores_invalid_traceparent() {
    async fn check_trace() -> String {
        current_transport_context()
            .map(|_| "scoped".to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_trace()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    let request = AxumRequest::get("/check")
        .header("traceparent", "invalid-traceparent")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "none");
}

#[tokio::test]
async fn trace_context_layer_discards_invalid_tracestate() {
    async fn check_trace() -> String {
        current_transport_context()
            .map(|_| "scoped".to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_trace()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    // Valid traceparent but invalid tracestate (contains uppercase which is not allowed)
    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let request = AxumRequest::get("/check")
        .header("traceparent", traceparent)
        .header("tracestate", "CONGO=t61rcWkgMzE") // uppercase not allowed
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    // Traceparent should still be scoped even with invalid tracestate
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "scoped");
}

#[tokio::test]
async fn trace_context_layer_rejects_reserved_version_ff() {
    async fn check_trace() -> String {
        current_transport_context()
            .map(|_| "scoped".to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_trace()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    // Version 0xff is reserved
    let request = AxumRequest::get("/check")
        .header("traceparent", "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-01")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "none");
}

#[tokio::test]
async fn trace_context_layer_rejects_all_zero_trace_id() {
    async fn check_trace() -> String {
        current_transport_context()
            .map(|_| "scoped".to_string())
            .unwrap_or_else(|| "none".to_string())
    }

    async fn handler() -> String {
        check_trace()
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new());

    let request = AxumRequest::get("/check")
        .header("traceparent", "00-00000000000000000000000000000000-0af7651916cd43dd8448eb211c80319c-01")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap(), "none");
}

// ---------------------------------------------------------------------------
// correlation_id helper function tests
// ---------------------------------------------------------------------------

#[test]
fn correlation_id_parses_numeric_header() {
    let mut headers = HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("12345"));

    let id = correlation_id(&headers);
    assert_eq!(id, 12345);
}

#[test]
fn correlation_id_generates_fallback_for_missing_header() {
    let headers = HeaderMap::new();

    let id = correlation_id(&headers);
    // Should be > 0 (monotonic counter)
    assert!(id > 0);
}

#[test]
fn correlation_id_generates_fallback_for_non_numeric_header() {
    let mut headers = HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("not-a-number"));

    let id = correlation_id(&headers);
    // Should be > 0 (monotonic counter)
    assert!(id > 0);
}

#[test]
fn correlation_id_parses_large_numbers() {
    let mut headers = HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("18446744073709551615"));

    let id = correlation_id(&headers);
    assert_eq!(id, u64::MAX);
}

// ---------------------------------------------------------------------------
// propagate_correlation_header helper function tests
// ---------------------------------------------------------------------------

#[test]
fn propagate_correlation_header_does_nothing_when_header_exists() {
    let mut headers = HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("existing-value"));

    propagate_correlation_header(&mut headers);

    assert_eq!(
        headers.get(CORRELATION_ID_HEADER).and_then(|v| v.to_str().ok()),
        Some("existing-value")
    );
}

#[test]
fn propagate_correlation_header_adds_header_when_missing() {
    let mut headers = HeaderMap::new();

    // Scope a correlation value first
    let _guard = scope_correlation_value("test-correlation".into(), async {});

    propagate_correlation_header(&mut headers);

    assert_eq!(
        headers.get(CORRELATION_ID_HEADER).and_then(|v| v.to_str().ok()),
        Some("test-correlation")
    );
}

// ---------------------------------------------------------------------------
// endpoint_panic_middleware tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn endpoint_panic_middleware_returns_500_on_panic() {
    async fn handler() -> &'static str {
        panic!("intentional panic for testing");
    }

    let app = Router::new()
        .route("/panic", get(handler))
        .layer(axum::middleware::from_fn(endpoint_panic_middleware));

    let request = AxumRequest::get("/panic").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["code"], "internal");
}

#[tokio::test]
async fn endpoint_panic_middleware_passes_through_normal_response() {
    async fn handler() -> &'static str {
        "success"
    }

    let app = Router::new()
        .route("/ok", get(handler))
        .layer(axum::middleware::from_fn(endpoint_panic_middleware));

    let request = AxumRequest::get("/ok").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Layer composability tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn correlation_layer_composes_with_trace_layer() {
    async fn check_both() -> (String, String) {
        let correlation = current_correlation_value()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".to_string());
        let traceparent = current_transport_context()
            .and_then(|ctx| ctx.headers()?.get("traceparent"))
            .map(str::to_owned)
            .unwrap_or_else(|| "none".to_string());
        (correlation, traceparent)
    }

    async fn handler() -> String {
        let (corr, trace) = check_both();
        format!("{}|{}", corr, trace)
    }

    let app = Router::new()
        .route("/check", get(handler))
        .layer(TraceContextLayer::new())
        .layer(CorrelationLayer::new());

    let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let request = AxumRequest::get("/check")
        .header(CORRELATION_ID_HEADER, "my-correlation")
        .header("traceparent", traceparent)
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024).await.unwrap();
    let result = String::from_utf8(body.to_vec()).unwrap();
    assert!(result.contains("my-correlation"));
    assert!(result.contains(traceparent));
}

#[tokio::test]
async fn multiple_routes_share_layers() {
    async fn handler1() -> &'static str {
        "handler1"
    }

    async fn handler2() -> &'static str {
        "handler2"
    }

    let app = Router::new()
        .route("/one", get(handler1))
        .route("/two", get(handler2))
        .layer(CorrelationLayer::new());

    // First request
    let request1 = AxumRequest::get("/one")
        .header(CORRELATION_ID_HEADER, "req-1")
        .body(Body::empty())
        .unwrap();
    let response1 = app.clone().oneshot(request1).await.unwrap();
    assert_eq!(
        response1
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("req-1")
    );

    // Second request with different correlation
    let request2 = AxumRequest::get("/two")
        .header(CORRELATION_ID_HEADER, "req-2")
        .body(Body::empty())
        .unwrap();
    let response2 = app.oneshot(request2).await.unwrap();
    assert_eq!(
        response2
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("req-2")
    );
}
