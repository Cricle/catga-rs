//! E2E tests for catga-axum HTTP client and server integration.
//!
//! These tests verify end-to-end HTTP client/server communication, correlation header
//! propagation, and middleware chain behavior using actual network connections.

use std::{
    future::IntoFuture,
    num::NonZeroUsize,
    sync::atomic::{AtomicU32, Ordering},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::post,
};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaHttpError, CorrelationHttpClient, HttpClusterForwarder,
    IntoCatgaHttpResponse, catga_routes, correlation_middleware, endpoint_panic_middleware,
    event_route, leader_forward_route, mediator_route,
};
use catga_cluster::ClusterForwarder;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler,
    Mediator, Registry, Request, scope_correlation_id,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Deserialize, Serialize)]
struct E2eRequest {
    value: u32,
}

impl catga_core::Message for E2eRequest {}

impl Request for E2eRequest {
    type Response = u32;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct E2eHandler;

#[async_trait]
impl Handler<E2eRequest> for E2eHandler {
    async fn handle(&self, request: E2eRequest) -> CatgaResult<u32> {
        Ok(request.value + 1)
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct E2eEvent(u32);

impl catga_core::Message for E2eEvent {}

impl Event for E2eEvent {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct E2eEventHandler(std::sync::Arc<AtomicU32>);

#[async_trait]
impl EventHandler<E2eEvent> for E2eEventHandler {
    async fn handle(&self, event: E2eEvent) -> CatgaResult<()> {
        self.0.store(event.0, Ordering::Relaxed);
        Ok(())
    }
}

/// E2E test: HTTP server starts and accepts connections on a real TCP socket.
#[tokio::test]
async fn e2e_http_server_binds_and_accepts_connections() {
    let mut registry = Registry::new();
    registry.register_request::<E2eRequest, _>(E2eHandler).unwrap();
    let app = mediator_route::<E2eRequest>(
        "/api/test",
        std::sync::Arc::new(Mediator::new(registry)),
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    // Make an actual HTTP request over TCP
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/api/test"))
        .json(&E2eRequest { value: 41 })
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<u32>().await.unwrap(), 42);
}

/// E2E test: CorrelationHttpClient propagates correlation headers across actual HTTP requests.
#[tokio::test]
async fn e2e_correlation_http_client_propagates_headers() {
    let observed_correlation = std::sync::Arc::new(AsyncMutex::new(None));
    let app = Router::new().route(
        "/observe",
        post({
            let observed_correlation = std::sync::Arc::clone(&observed_correlation);
            move |headers: HeaderMap| async move {
                *observed_correlation.lock().await = headers
                    .get(CORRELATION_ID_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                StatusCode::NO_CONTENT
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}/observe");

    // Use scope_correlation_id to set ambient correlation
    let result = scope_correlation_id(12345, async {
        CorrelationHttpClient::new(reqwest::Client::new())
            .post(&endpoint, HeaderMap::new())
            .send()
            .await
    })
    .await
    .unwrap();

    server.abort();

    assert_eq!(result.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        *observed_correlation.lock().await,
        Some("12345".to_string())
    );
}

/// E2E test: HttpClusterForwarder forwards requests to leader endpoint.
#[tokio::test]
async fn e2e_http_cluster_forwarder_forwards_to_leader() {
    let app = leader_forward_route::<E2eRequest>({
        let mut registry = Registry::new();
        registry.register_request::<E2eRequest, _>(E2eHandler).unwrap();
        std::sync::Arc::new(Mediator::new(registry))
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}");
    let result = HttpClusterForwarder::new(reqwest::Client::new())
        .forward(E2eRequest { value: 41 }, &endpoint)
        .await
        .unwrap();

    server.abort();

    assert_eq!(result, 42);
}

/// E2E test: Event route publishes events and they are received by handlers.
#[tokio::test]
async fn e2e_event_route_publishes_and_handles_events() {
    let captured = std::sync::Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry.register_event::<E2eEvent, _>(E2eEventHandler(std::sync::Arc::clone(&captured)));
    let app = event_route::<E2eEvent>(
        "/api/events",
        std::sync::Arc::new(Mediator::new(registry)),
    )
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/api/events"))
        .json(&E2eEvent(42))
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 42);
}

/// E2E test: Multiple routes work together in a merged router.
#[tokio::test]
async fn e2e_merged_router_handles_multiple_routes() {
    let captured = std::sync::Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry.register_request::<E2eRequest, _>(E2eHandler).unwrap();
    registry.register_event::<E2eEvent, _>(E2eEventHandler(std::sync::Arc::clone(&captured)));
    let app = catga_routes! {
        mediator = std::sync::Arc::new(Mediator::new(registry));
        requests {
            "/api/forward" => E2eRequest,
        }
        events {
            "/api/event" => E2eEvent,
        }
    }
    .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();

    // Test request route
    let request_response = client
        .post(format!("http://127.0.0.1:{port}/api/forward"))
        .json(&E2eRequest { value: 100 })
        .send()
        .await
        .unwrap();
    assert_eq!(request_response.json::<u32>().await.unwrap(), 101);

    // Test event route
    let event_response = client
        .post(format!("http://127.0.0.1:{port}/api/event"))
        .json(&E2eEvent(99))
        .send()
        .await
        .unwrap();
    assert_eq!(event_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 99);

    server.abort();
}

/// E2E test: HttpClusterForwarder respects response size limits.
#[tokio::test]
async fn e2e_http_cluster_forwarder_enforces_response_limit() {
    let large_body = "x".repeat(256);
    let app = Router::new().route(
        "/api/catga/forward/E2eRequest",
        post(move |_: ()| async move { large_body.clone() }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let endpoint = format!("http://127.0.0.1:{port}");
    let error = HttpClusterForwarder::with_response_limit(
        reqwest::Client::new(),
        NonZeroUsize::new(128).expect("non-zero"),
    )
    .forward(E2eRequest { value: 0 }, &endpoint)
    .await
    .expect_err("oversized response should be rejected");

    server.abort();

    assert_eq!(error.code(), ErrorCode::Transient);
}

async fn validate_handler(Json(payload): Json<E2eRequest>) -> impl IntoResponse {
    if payload.value == 0 {
        CatgaHttpError::from(CatgaError::new(ErrorCode::Validation, "value must be non-zero")).into_response()
    } else {
        let response: CatgaResult<u32> = Ok(payload.value + 1);
        response.into_catga_response(StatusCode::OK)
    }
}

/// E2E test: CatgaHttpError maps to correct HTTP status codes.
#[tokio::test]
async fn e2e_catga_error_maps_to_correct_http_status() {
    let app = Router::new().route("/validate", post(validate_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();

    // Test validation error returns UNPROCESSABLE_ENTITY
    let error_response = client
        .post(format!("http://127.0.0.1:{port}/validate"))
        .json(&E2eRequest { value: 0 })
        .send()
        .await
        .unwrap();
    assert_eq!(error_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

    // Test success returns OK
    let success_response = client
        .post(format!("http://127.0.0.1:{port}/validate"))
        .json(&E2eRequest { value: 10 })
        .send()
        .await
        .unwrap();
    assert_eq!(success_response.status(), StatusCode::OK);
    assert_eq!(success_response.json::<u32>().await.unwrap(), 11);

    server.abort();
}

/// E2E test: correlation_middleware echoes correlation header in response.
#[tokio::test]
async fn e2e_correlation_middleware_echoes_header_in_response() {
    async fn endpoint() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/test", post(endpoint))
        .layer(middleware::from_fn(correlation_middleware));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/test"))
        .header(CORRELATION_ID_HEADER, "test-correlation-123")
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok()),
        Some("test-correlation-123")
    );
}

/// E2E test: endpoint_panic_middleware catches panics and returns stable error.
#[tokio::test]
async fn e2e_endpoint_panic_middleware_returns_stable_error() {
    async fn panicking_endpoint() -> StatusCode {
        panic!("intentional test panic");
    }

    let app = Router::new()
        .route("/panic", post(panicking_endpoint))
        .layer(middleware::from_fn(endpoint_panic_middleware));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/panic"))
        .send()
        .await
        .unwrap();

    server.abort();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["code"], "internal");
    assert_eq!(body["message"], "endpoint handler panicked");
}
