//! Axum adapter integration tests.

use std::{
    future::IntoFuture,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, Request as AxumRequest, StatusCode, header::LOCATION},
    middleware,
    response::IntoResponse,
    routing::post,
};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaHttpError, CorrelationHttpClient, EndpointKind, EndpointMetadata,
    EndpointValidation, HttpClusterForwarder, HttpRaftTransport, IntoCatgaHttpResponse,
    MAX_RAFT_MESSAGE_BYTES, axum_routes, catga_endpoint_metadata, catga_routes, correlation_id,
    correlation_middleware, endpoint_panic_middleware, event_route, leader_forward_route,
    mediator_route, propagate_correlation_header, propagate_trace_context_headers,
    raft_message_route, validate_min_length, validate_required,
};
use catga_cluster::{
    ClusterForwarder, RaftInboundPolicy, RaftInboundPolicyError, RaftInboundRejection, RaftMember,
    RaftMessage, RaftPeerIdentity, RaftTransport, StaticRaftInboundPolicy,
};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeHeaders, ErrorCode, Event, EventHandler, Handler,
    Mediator, MessageMetadata, Registry, Request, current_transport_context, scope_correlation_id,
    scope_transport_context,
};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tower::ServiceExt;

#[tokio::test]
async fn axum_error_response_uses_stable_status_codes_and_compact_json() {
    let response =
        CatgaHttpError::from(CatgaError::new(ErrorCode::Validation, "bad input")).into_response();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"code":"validation","message":"bad input"}"#
    );

    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::NotFound, "missing"))
            .into_response()
            .status(),
        axum::http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Conflict, "busy"))
            .into_response()
            .status(),
        axum::http::StatusCode::CONFLICT
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Transient, "retry"))
            .into_response()
            .status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let unavailable =
        CatgaHttpError::from(CatgaError::new(ErrorCode::Unavailable, "stopping")).into_response();
    assert_eq!(
        unavailable.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        to_bytes(unavailable.into_body(), 1024).await.unwrap(),
        r#"{"code":"unavailable","message":"stopping"}"#
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Unauthorized, "login"))
            .into_response()
            .status(),
        axum::http::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        CatgaHttpError::from(CatgaError::new(ErrorCode::Forbidden, "denied"))
            .into_response()
            .status(),
        axum::http::StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn axum_error_response_preserves_csharp_category_statuses_and_typed_bodies() {
    let cases = [
        (ErrorCode::HandlerFailed, StatusCode::BAD_REQUEST),
        (ErrorCode::HandlerNotFound, StatusCode::NOT_FOUND),
        (ErrorCode::PipelineFailed, StatusCode::BAD_REQUEST),
        (
            ErrorCode::PersistenceFailed,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (ErrorCode::LockFailed, StatusCode::SERVICE_UNAVAILABLE),
        (ErrorCode::TransportFailed, StatusCode::SERVICE_UNAVAILABLE),
        (ErrorCode::SerializationFailed, StatusCode::BAD_REQUEST),
        (ErrorCode::FlowFailed, StatusCode::BAD_REQUEST),
        (ErrorCode::FlowCancelled, StatusCode::REQUEST_TIMEOUT),
        (ErrorCode::FlowTimeout, StatusCode::REQUEST_TIMEOUT),
        (ErrorCode::FlowCompensating, StatusCode::BAD_REQUEST),
    ];

    for (code, status) in cases {
        let response =
            CatgaHttpError::from(CatgaError::new(code, "source contract")).into_response();
        assert_eq!(response.status(), status, "{code:?}");
        let expected_body = format!(
            r#"{{"code":"{}","message":"source contract"}}"#,
            code.as_stable_str()
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            expected_body.as_bytes(),
            "{code:?}"
        );
    }
}

#[test]
fn endpoint_validation_aggregates_ordered_errors_into_a_catga_validation_failure() {
    let mut validation = EndpointValidation::new();
    validation
        .add(validate_required(Some("  "), "name"))
        .add(validate_min_length(Some("abc"), 4, "name"))
        .add_if(true, "quantity must be positive");

    assert_eq!(validation.len(), 3);
    assert_eq!(validation.first(), Some("name is required"));
    assert_eq!(
        validation.errors().collect::<Vec<_>>(),
        vec![
            "name is required",
            "name must be at least 4 characters",
            "quantity must be positive",
        ]
    );
    assert_eq!(
        validation
            .into_result()
            .expect_err("validation must fail")
            .code(),
        ErrorCode::Validation
    );
    assert!(validate_required(Some("Ada"), "name").is_none());
}

#[test]
fn validate_min_length_rejects_a_missing_value() {
    assert_eq!(
        validate_min_length(None, 4, "name").as_deref(),
        Some("name must be at least 4 characters")
    );
}

#[test]
fn axum_correlation_id_uses_the_request_header_or_a_lock_free_fallback() {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(CORRELATION_ID_HEADER, "42".parse().unwrap());
    assert_eq!(correlation_id(&headers), 42);

    let first = correlation_id(&axum::http::HeaderMap::new());
    let second = correlation_id(&axum::http::HeaderMap::new());
    assert!(second > first);
}

#[tokio::test]
async fn correlation_middleware_echoes_an_opaque_nonempty_correlation_header() {
    async fn endpoint() -> StatusCode {
        StatusCode::NO_CONTENT
    }

    let app = Router::new()
        .route("/correlation", post(endpoint))
        .layer(middleware::from_fn(correlation_middleware));
    let request = AxumRequest::post("/correlation")
        .header(CORRELATION_ID_HEADER, "browser-request-4f8c")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("browser-request-4f8c")
    );
}

#[tokio::test]
async fn outgoing_http_headers_inherit_the_scoped_correlation_without_overwriting_an_explicit_value()
 {
    let inherited = scope_correlation_id(717, async {
        let mut headers = HeaderMap::new();
        propagate_correlation_header(&mut headers);
        headers
    })
    .await;
    assert_eq!(
        inherited
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("717")
    );

    let explicit = scope_correlation_id(718, async {
        let mut headers = HeaderMap::new();
        headers.insert(CORRELATION_ID_HEADER, "client-value".parse().unwrap());
        propagate_correlation_header(&mut headers);
        headers
    })
    .await;
    assert_eq!(
        explicit
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("client-value")
    );
}

#[tokio::test]
async fn correlation_http_client_applies_scoped_headers_to_explicit_requests() {
    let observed = Arc::new(std::sync::Mutex::new(None));
    let app = Router::new().route(
        "/correlated-client",
        post({
            let observed = Arc::clone(&observed);
            move |headers: HeaderMap| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().expect("test observer lock is available") = headers
                        .get(CORRELATION_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    StatusCode::NO_CONTENT
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/correlated-client",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let response = scope_correlation_id(717, async {
        CorrelationHttpClient::new(reqwest::Client::new())
            .post(&endpoint, HeaderMap::new())
            .send()
            .await
    })
    .await
    .unwrap();
    server.abort();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        observed
            .lock()
            .expect("test observer lock is available")
            .as_deref(),
        Some("717")
    );
}

#[tokio::test]
async fn outgoing_http_headers_inherit_scoped_w3c_trace_context_without_overwriting_explicit_values()
 {
    let envelope = Envelope::new(
        71,
        "orders.created",
        Vec::new(),
        MessageMetadata::new(71, None),
    )
    .with_headers(
        EnvelopeHeaders::try_new([
            (
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ),
            ("tracestate", "vendor=state"),
        ])
        .expect("valid trace headers"),
    );
    let inherited = scope_transport_context(&envelope, async {
        let mut headers = HeaderMap::new();
        propagate_trace_context_headers(&mut headers);
        headers
    })
    .await;
    assert_eq!(
        inherited
            .get("traceparent")
            .and_then(|value| value.to_str().ok()),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(
        inherited
            .get("tracestate")
            .and_then(|value| value.to_str().ok()),
        Some("vendor=state")
    );

    let explicit = scope_transport_context(&envelope, async {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
                .parse()
                .expect("valid header"),
        );
        propagate_trace_context_headers(&mut headers);
        headers
    })
    .await;
    assert_eq!(
        explicit
            .get("traceparent")
            .and_then(|value| value.to_str().ok()),
        Some("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
    );
    assert!(explicit.get("tracestate").is_none());
}

#[derive(Deserialize, Serialize)]
struct ForwardRequest {
    value: u32,
}

#[tokio::test]
async fn catga_result_response_serializes_success_with_the_requested_status() {
    let response =
        Ok::<_, CatgaError>(ForwardRequest { value: 7 }).into_catga_response(StatusCode::ACCEPTED);

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"value":7}"#
    );
}

#[tokio::test]
async fn catga_result_response_omits_the_body_for_no_content() {
    let response = Ok::<_, CatgaError>(ForwardRequest { value: 7 })
        .into_catga_response(StatusCode::NO_CONTENT);

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn catga_result_response_creates_a_resource_at_the_provided_location() {
    let response = Ok::<_, CatgaError>(ForwardRequest { value: 7 }).into_catga_created("/orders/7");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok()),
        Some("/orders/7")
    );
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"value":7}"#
    );
}

#[tokio::test]
async fn catga_result_response_delegates_failures_to_catga_http_error() {
    let response = Err::<ForwardRequest, _>(CatgaError::new(ErrorCode::NotFound, "missing"))
        .into_catga_response(StatusCode::OK);

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"code":"not_found","message":"missing"}"#
    );
}

#[tokio::test]
async fn catga_result_response_reports_an_invalid_created_location_without_panicking() {
    let response =
        Ok::<_, CatgaError>(ForwardRequest { value: 7 }).into_catga_created("/orders\n7");

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"code":"internal","message":"invalid Location header"}"#
    );
}

#[tokio::test]
async fn endpoint_panic_middleware_returns_a_stable_internal_error() {
    async fn panicking_endpoint() -> StatusCode {
        panic!("test endpoint panic");
    }

    let app = Router::new()
        .route("/panic", post(panicking_endpoint))
        .layer(middleware::from_fn(endpoint_panic_middleware));
    let request = match AxumRequest::post("/panic").body(Body::empty()) {
        Ok(request) => request,
        Err(error) => panic!("test request construction must succeed: {error}"),
    };
    let response = match app.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"code":"internal","message":"endpoint handler panicked"}"#
    );
}

#[tokio::test]
async fn endpoint_panic_middleware_preserves_a_normal_response() {
    async fn successful_endpoint() -> (StatusCode, Json<ForwardRequest>) {
        (StatusCode::ACCEPTED, Json(ForwardRequest { value: 7 }))
    }

    let app = Router::new()
        .route("/normal", post(successful_endpoint))
        .layer(middleware::from_fn(endpoint_panic_middleware));
    let request = match AxumRequest::post("/normal").body(Body::empty()) {
        Ok(request) => request,
        Err(error) => panic!("test request construction must succeed: {error}"),
    };
    let response = match app.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        r#"{"value":7}"#
    );
}

impl catga_core::Message for ForwardRequest {}

impl Request for ForwardRequest {
    type Response = u32;
}

#[test]
fn endpoint_metadata_keeps_openapi_details_explicit_and_allocation_free() {
    let metadata = catga_endpoint_metadata! {
        commands { "/orders" => ForwardRequest }
        queries { "/orders/{id}" => ForwardRequest }
        events { "/orders/published" => OrderPublished }
    };

    assert_eq!(metadata.len(), 3);
    assert_eq!(metadata[0].kind(), EndpointKind::Command);
    assert_eq!(metadata[0].method(), axum::http::Method::POST);
    assert_eq!(metadata[0].path(), "/orders");
    assert_eq!(metadata[0].operation_id(), "ForwardRequest");
    assert_eq!(metadata[0].tag(), "Commands");
    assert_eq!(
        metadata[0].response_statuses(),
        [
            axum::http::StatusCode::OK,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY,
            axum::http::StatusCode::NOT_FOUND,
            axum::http::StatusCode::CONFLICT,
        ]
    );
    assert_eq!(metadata[1].kind(), EndpointKind::Query);
    assert_eq!(metadata[1].tag(), "Queries");
    assert_eq!(metadata[2].kind(), EndpointKind::Event);
    assert_eq!(
        metadata[2].response_statuses(),
        [axum::http::StatusCode::NO_CONTENT]
    );

    let described = EndpointMetadata::command::<ForwardRequest>("/orders")
        .with_operation_id("create-order")
        .with_description("Creates one order through Catga.");
    assert_eq!(described.operation_id(), "create-order");
    assert_eq!(
        described.description(),
        Some("Creates one order through Catga.")
    );

    let empty = catga_endpoint_metadata! {
        commands {}
        queries {}
        events {}
    };
    assert!(empty.is_empty());
}

struct ForwardHandler;

#[async_trait]
impl Handler<ForwardRequest> for ForwardHandler {
    async fn handle(&self, request: ForwardRequest) -> CatgaResult<u32> {
        Ok(request.value + 1)
    }
}

struct NestedTraceForwardHandler {
    endpoint: Arc<str>,
}

#[async_trait]
impl Handler<ForwardRequest> for NestedTraceForwardHandler {
    async fn handle(&self, request: ForwardRequest) -> CatgaResult<u32> {
        HttpClusterForwarder::new(reqwest::Client::new())
            .forward(request, self.endpoint.as_ref())
            .await
    }
}

#[tokio::test]
async fn http_cluster_forwarder_posts_a_typed_request_to_the_leader() {
    let mut registry = Registry::new();
    registry
        .register_request::<ForwardRequest, _>(ForwardHandler)
        .unwrap();
    let app = leader_forward_route::<ForwardRequest>(std::sync::Arc::new(Mediator::new(registry)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let result = HttpClusterForwarder::new(reqwest::Client::new())
        .forward(ForwardRequest { value: 41 }, &endpoint)
        .await
        .unwrap();
    server.abort();

    assert_eq!(result, 42);
}

#[tokio::test]
async fn http_cluster_forwarder_rejects_a_success_response_larger_than_its_limit() {
    let body = format!("42{}", " ".repeat(128));
    let app = Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post(move || {
            let body = body.clone();
            async move {
                (
                    [(axum::http::header::CONTENT_TYPE, "application/json")],
                    body,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let error = HttpClusterForwarder::with_response_limit(
        reqwest::Client::new(),
        NonZeroUsize::new(16).expect("nonzero response limit"),
    )
    .forward(ForwardRequest { value: 41 }, &endpoint)
    .await
    .expect_err("oversized leader response must be rejected");
    server.abort();

    assert_eq!(error.code(), ErrorCode::Transient);
}

#[tokio::test]
async fn http_cluster_forwarder_propagates_the_ambient_correlation_header() {
    let observed = Arc::new(std::sync::Mutex::new(None));
    let app = Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post({
            let observed = Arc::clone(&observed);
            move |headers: HeaderMap, Json(request): Json<ForwardRequest>| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().expect("test observer lock is available") = headers
                        .get(CORRELATION_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    Json(request.value + 1)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let result = scope_correlation_id(717, async {
        HttpClusterForwarder::new(reqwest::Client::new())
            .forward(ForwardRequest { value: 41 }, &endpoint)
            .await
    })
    .await
    .unwrap();
    server.abort();

    assert_eq!(result, 42);
    assert_eq!(
        observed
            .lock()
            .expect("test observer lock is available")
            .as_deref(),
        Some("717")
    );
}

#[tokio::test]
async fn http_cluster_forwarder_preserves_an_opaque_scoped_correlation_header() {
    let observed = Arc::new(std::sync::Mutex::new(None));
    let app = Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post({
            let observed = Arc::clone(&observed);
            move |headers: HeaderMap, Json(request): Json<ForwardRequest>| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().expect("test observer lock is available") = headers
                        .get(CORRELATION_ID_HEADER)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    Json(request.value + 1)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let envelope = Envelope::new(
        72,
        "orders.created",
        Vec::new(),
        MessageMetadata::new(72, Some(72)),
    )
    .with_headers(
        EnvelopeHeaders::try_new([(CORRELATION_ID_HEADER, "browser-request-4f8c")])
            .expect("valid correlation header"),
    );

    let result = scope_transport_context(&envelope, async {
        HttpClusterForwarder::new(reqwest::Client::new())
            .forward(ForwardRequest { value: 41 }, &endpoint)
            .await
    })
    .await
    .expect("leader forwarding succeeds");
    server.abort();

    assert_eq!(result, 42);
    assert_eq!(
        observed
            .lock()
            .expect("test observer lock is available")
            .as_deref(),
        Some("browser-request-4f8c")
    );
}

#[tokio::test]
async fn http_cluster_forwarder_propagates_scoped_w3c_trace_headers() {
    let observed = Arc::new(std::sync::Mutex::new(None));
    let app = Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post({
            let observed = Arc::clone(&observed);
            move |headers: HeaderMap, Json(request): Json<ForwardRequest>| {
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().expect("test observer lock is available") = headers
                        .get("traceparent")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    Json(request.value + 1)
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let envelope = Envelope::new(
        72,
        "orders.created",
        Vec::new(),
        MessageMetadata::new(72, None),
    )
    .with_headers(
        EnvelopeHeaders::try_new([(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )])
        .expect("valid trace header"),
    );

    let result = scope_transport_context(&envelope, async {
        HttpClusterForwarder::new(reqwest::Client::new())
            .forward(ForwardRequest { value: 41 }, &endpoint)
            .await
    })
    .await
    .expect("leader forwarding succeeds");
    server.abort();

    assert_eq!(result, 42);
    assert_eq!(
        observed
            .lock()
            .expect("test observer lock is available")
            .as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
}

#[tokio::test]
async fn mediator_route_dispatches_a_typed_json_request_at_the_registered_path() {
    let mut registry = Registry::new();
    registry
        .register_request::<ForwardRequest, _>(ForwardHandler)
        .unwrap();
    let app = mediator_route::<ForwardRequest>(
        "/orders/forward",
        std::sync::Arc::new(Mediator::new(registry)),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders/forward"))
        .json(&ForwardRequest { value: 41 })
        .send()
        .await
        .unwrap();
    server.abort();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.json::<u32>().await.unwrap(), 42);
}

#[tokio::test]
async fn mediator_route_scopes_inbound_trace_context_for_nested_http_forwarding() {
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let downstream = Router::new().route(
        "/api/catga/forward/ForwardRequest",
        post({
            let observed = Arc::clone(&observed);
            move |headers: HeaderMap, Json(request): Json<ForwardRequest>| {
                let observed = Arc::clone(&observed);
                async move {
                    observed
                        .lock()
                        .expect("test observer lock is available")
                        .push((
                            headers
                                .get("traceparent")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                            headers
                                .get("tracestate")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                        ));
                    Json(request.value + 1)
                }
            }
        }),
    );
    let downstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("downstream listener binds");
    let downstream_endpoint: Arc<str> = format!(
        "http://{}",
        downstream_listener.local_addr().expect("address")
    )
    .into();
    let downstream_server =
        tokio::spawn(axum::serve(downstream_listener, downstream).into_future());

    let mut registry = Registry::new();
    registry
        .register_request::<ForwardRequest, _>(NestedTraceForwardHandler {
            endpoint: Arc::clone(&downstream_endpoint),
        })
        .expect("nested forward handler is accepted");
    let app =
        mediator_route::<ForwardRequest>("/orders/forward", Arc::new(Mediator::new(registry)))
            .expect("mediator route is valid");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("inbound listener binds");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = reqwest::Client::new();
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    for tracestate in ["vendor=state", "vendor=state,,next=value"] {
        let response = client
            .post(format!("{endpoint}/orders/forward"))
            .header("traceparent", traceparent)
            .header("tracestate", tracestate)
            .json(&ForwardRequest { value: 41 })
            .send()
            .await
            .expect("inbound request succeeds");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
    }
    server.abort();
    downstream_server.abort();

    assert_eq!(
        observed
            .lock()
            .expect("test observer lock is available")
            .as_slice(),
        [
            (Some(traceparent.into()), Some("vendor=state".into())),
            (Some(traceparent.into()), None),
        ]
    );
}

#[derive(Clone, Deserialize, Serialize)]
struct OrderPublished(u32);

impl catga_core::Message for OrderPublished {}
impl Event for OrderPublished {}

struct PublishedValue(Arc<AtomicU32>);

#[async_trait]
impl EventHandler<OrderPublished> for PublishedValue {
    async fn handle(&self, event: OrderPublished) -> CatgaResult<()> {
        self.0.store(event.0, Ordering::Relaxed);
        Ok(())
    }
}

type ObservedTraceContext = (String, Option<String>);
type SharedObservedTraceContext = Arc<std::sync::Mutex<Option<ObservedTraceContext>>>;

struct TraceObservedEventHandler(SharedObservedTraceContext);

#[async_trait]
impl EventHandler<OrderPublished> for TraceObservedEventHandler {
    async fn handle(&self, _: OrderPublished) -> CatgaResult<()> {
        let traceparent = current_transport_context()
            .and_then(|context| {
                context
                    .headers()
                    .and_then(|headers| headers.get("traceparent"))
                    .map(str::to_owned)
            })
            .ok_or_else(|| CatgaError::new(ErrorCode::Internal, "missing scoped traceparent"))?;
        let tracestate = current_transport_context().and_then(|context| {
            context
                .headers()
                .and_then(|headers| headers.get("tracestate"))
                .map(str::to_owned)
        });
        *self.0.lock().expect("test observer lock is available") = Some((traceparent, tracestate));
        Ok(())
    }
}

#[tokio::test]
async fn event_route_publishes_a_typed_json_event_at_the_registered_path() {
    let captured = Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry.register_event::<OrderPublished, _>(PublishedValue(Arc::clone(&captured)));
    let app = event_route::<OrderPublished>("/orders/published", Arc::new(Mediator::new(registry)))
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders/published"))
        .json(&OrderPublished(42))
        .send()
        .await
        .unwrap();
    server.abort();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 42);
}

#[tokio::test]
async fn event_route_scopes_inbound_trace_context() {
    let observed = Arc::new(std::sync::Mutex::new(None));
    let mut registry = Registry::new();
    registry.register_event::<OrderPublished, _>(TraceObservedEventHandler(Arc::clone(&observed)));
    let app = event_route::<OrderPublished>("/orders/published", Arc::new(Mediator::new(registry)))
        .expect("event route is valid");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders/published"))
        .header("traceparent", traceparent)
        .header("tracestate", "vendor=state")
        .json(&OrderPublished(42))
        .send()
        .await
        .expect("event request succeeds");
    server.abort();

    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        *observed.lock().expect("test observer lock is available"),
        Some((traceparent.into(), Some("vendor=state".into())))
    );
}

#[tokio::test]
async fn catga_routes_expands_static_request_and_event_routes() {
    let captured = Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<ForwardRequest, _>(ForwardHandler)
        .unwrap();
    registry.register_event::<OrderPublished, _>(PublishedValue(Arc::clone(&captured)));
    let app = catga_routes! {
        mediator = Arc::new(Mediator::new(registry));
        requests {
            "/macro/forward" => ForwardRequest,
        }
        events {
            "/macro/published" => OrderPublished,
        }
    }
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{endpoint}/macro/forward"))
        .json(&ForwardRequest { value: 41 })
        .send()
        .await
        .unwrap();
    assert_eq!(response.json::<u32>().await.unwrap(), 42);
    assert_eq!(
        client
            .post(format!("{endpoint}/macro/published"))
            .json(&OrderPublished(24))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    server.abort();
    assert_eq!(captured.load(Ordering::Relaxed), 24);
}

#[tokio::test]
async fn catga_routes_registers_explicit_http_methods_and_metadata() {
    fn json_request(method: &str, path: &str, body: &'static str) -> AxumRequest<Body> {
        AxumRequest::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    let captured = Arc::new(AtomicU32::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<ForwardRequest, _>(ForwardHandler)
        .unwrap();
    registry.register_event::<OrderPublished, _>(PublishedValue(Arc::clone(&captured)));
    let app = catga_routes! {
        mediator = Arc::new(Mediator::new(registry));
        requests {
            @get "/verbs/get" => ForwardRequest,
            @post "/verbs/post" => ForwardRequest,
            @put "/verbs/put" => ForwardRequest,
            @patch "/verbs/patch" => ForwardRequest,
        }
        events {
            @delete "/verbs/delete" => OrderPublished,
        }
    }
    .unwrap();
    let metadata = catga_endpoint_metadata! {
        commands {
            @get "/verbs/get" => ForwardRequest,
            @post "/verbs/post" => ForwardRequest,
            @put "/verbs/put" => ForwardRequest,
            @patch "/verbs/patch" => ForwardRequest,
        }
        queries {}
        events { @delete "/verbs/delete" => OrderPublished }
    };

    assert_eq!(metadata[0].method(), axum::http::Method::GET);
    assert_eq!(metadata[1].method(), axum::http::Method::POST);
    assert_eq!(metadata[2].method(), axum::http::Method::PUT);
    assert_eq!(metadata[3].method(), axum::http::Method::PATCH);
    assert_eq!(metadata[4].method(), axum::http::Method::DELETE);

    for (method, path) in [
        ("GET", "/verbs/get"),
        ("POST", "/verbs/post"),
        ("PUT", "/verbs/put"),
        ("PATCH", "/verbs/patch"),
    ] {
        let response = app
            .clone()
            .oneshot(json_request(method, path, r#"{"value":41}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "42");
    }

    let response = app
        .oneshot(json_request("DELETE", "/verbs/delete", "24"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(captured.load(Ordering::Relaxed), 24);
}

#[derive(Clone)]
struct StaticRouteState {
    prefix: &'static str,
}

#[derive(Deserialize, Serialize)]
struct StaticRoutePayload {
    value: u32,
}

#[derive(Deserialize, Serialize)]
struct StaticRouteResponse {
    value: u32,
    source: String,
}

async fn static_axum_handler_with_extractors(
    State(state): State<StaticRouteState>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    Json(payload): Json<StaticRoutePayload>,
) -> (StatusCode, Json<StaticRouteResponse>) {
    let source = headers
        .get("x-source")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");

    (
        StatusCode::CREATED,
        Json(StaticRouteResponse {
            value: payload.value + id,
            source: format!("{}-{source}", state.prefix),
        }),
    )
}

async fn static_axum_get_handler() -> &'static str {
    "get"
}

async fn static_axum_post_handler() -> StatusCode {
    StatusCode::ACCEPTED
}

async fn static_axum_patch_handler() -> Json<StaticRoutePayload> {
    Json(StaticRoutePayload { value: 7 })
}

async fn static_axum_delete_handler() -> StatusCode {
    StatusCode::NO_CONTENT
}

#[tokio::test]
async fn axum_routes_registers_native_handlers_with_extractors_methods_and_closures() {
    let closure_prefix = "closure";
    let app = axum_routes! {
        Router::<StaticRouteState>::new();
        GET "/native/get" => static_axum_get_handler,
        POST "/native/post" => static_axum_post_handler,
        PUT "/native/users/{id}" => static_axum_handler_with_extractors,
        PATCH "/native/patch" => static_axum_patch_handler,
        DELETE "/native/delete" => static_axum_delete_handler,
        GET "/native/closure/{id}" => move |Path(id): Path<u32>| async move {
            format!("{closure_prefix}-{id}")
        },
    }
    .with_state(StaticRouteState { prefix: "state" });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{endpoint}/native/get"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "get"
    );
    assert_eq!(
        client
            .post(format!("{endpoint}/native/post"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::ACCEPTED
    );
    let response = client
        .put(format!("{endpoint}/native/users/2"))
        .header("x-source", "request")
        .json(&StaticRoutePayload { value: 40 })
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let response = response.json::<StaticRouteResponse>().await.unwrap();
    assert_eq!(response.value, 42);
    assert_eq!(response.source, "state-request");
    assert_eq!(
        client
            .patch(format!("{endpoint}/native/patch"))
            .send()
            .await
            .unwrap()
            .json::<StaticRoutePayload>()
            .await
            .unwrap()
            .value,
        7
    );
    assert_eq!(
        client
            .delete(format!("{endpoint}/native/delete"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        client
            .get(format!("{endpoint}/native/closure/9"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "closure-9"
    );
    server.abort();
}

#[tokio::test]
async fn http_raft_transport_posts_an_authenticated_protobuf_frame_to_the_runtime_inbox() {
    let (inbox, mut receiver) = mpsc::channel(1);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let router = raft_message_route(inbox, policy).layer(axum::Extension(
        RaftPeerIdentity::new("spiffe://catga/node-2").unwrap(),
    ));
    let server = tokio::spawn(axum::serve(listener, router).into_future());
    let transport = HttpRaftTransport::new(
        reqwest::Client::new(),
        [RaftMember::new(1, endpoint.clone())],
    );
    let message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    };

    transport.send(message.clone()).await.unwrap();
    server.abort();

    assert_eq!(receiver.recv().await, Some(message));
}

#[tokio::test]
async fn raft_route_establishes_a_member_and_target_trust_boundary() {
    let message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    }
    .write_to_bytes()
    .unwrap();
    let make_request = |body: Vec<u8>| {
        AxumRequest::post("/api/catga/raft")
            .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(body))
            .unwrap()
    };

    let (inbox, mut receiver) = mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let response = raft_message_route(inbox, policy)
        .oneshot(make_request(message.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(receiver.try_recv().is_err());

    let (inbox, mut receiver) = mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let response = raft_message_route(inbox, policy)
        .layer(axum::Extension(
            RaftPeerIdentity::new("spiffe://catga/attacker").unwrap(),
        ))
        .oneshot(make_request(message.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(receiver.try_recv().is_err());

    let (inbox, mut receiver) = mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let response = raft_message_route(inbox, policy)
        .layer(axum::Extension(
            RaftPeerIdentity::new("spiffe://catga/node-2").unwrap(),
        ))
        .oneshot(make_request(
            RaftMessage {
                from: 99,
                to: 1,
                ..RaftMessage::default()
            }
            .write_to_bytes()
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(receiver.try_recv().is_err());

    let (inbox, mut receiver) = mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let response = raft_message_route(inbox, policy)
        .layer(axum::Extension(
            RaftPeerIdentity::new("spiffe://catga/node-2").unwrap(),
        ))
        .oneshot(make_request(
            RaftMessage {
                from: 2,
                to: 3,
                ..RaftMessage::default()
            }
            .write_to_bytes()
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(receiver.try_recv().is_err());

    let (inbox, mut receiver) = mpsc::channel(1);
    let policy = StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")]).unwrap();
    let response = raft_message_route(inbox, policy)
        .layer(axum::Extension(
            RaftPeerIdentity::new("spiffe://catga/node-2").unwrap(),
        ))
        .oneshot(make_request(message))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(receiver.recv().await.unwrap().from, 2);
}

#[tokio::test]
async fn http_raft_transport_distinguishes_retryable_backpressure_from_fatal_rejection() {
    for (status, retryable) in [
        (StatusCode::TOO_MANY_REQUESTS, true),
        (StatusCode::FORBIDDEN, false),
    ] {
        let app = Router::new().route("/api/catga/raft", post(move || async move { status }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener binds");
        let endpoint = format!("http://{}", listener.local_addr().expect("address"));
        let server = tokio::spawn(axum::serve(listener, app).into_future());
        let transport =
            HttpRaftTransport::new(reqwest::Client::new(), [RaftMember::new(2, endpoint)]);

        let error = transport
            .send(RaftMessage {
                from: 1,
                to: 2,
                ..RaftMessage::default()
            })
            .await
            .expect_err("the test peer returns a non-success status");
        server.abort();
        assert_eq!(error.is_retryable(), retryable, "{status}");
    }
}

#[tokio::test]
async fn raft_route_rejects_invalid_frames_and_reports_bounded_inbox_backpressure() {
    let valid_message = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    }
    .write_to_bytes()
    .expect("Raft frame serializes");
    let trusted_identity = RaftPeerIdentity::new("spiffe://catga/node-2").expect("identity");
    let policy = || {
        StaticRaftInboundPolicy::new(1, [(2, "spiffe://catga/node-2")])
            .expect("member policy is valid")
    };
    let request = |body: Vec<u8>| {
        AxumRequest::post("/api/catga/raft")
            .header(axum::http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(body))
            .expect("request is valid")
    };

    let (inbox, _receiver) = mpsc::channel(1);
    let response = raft_message_route(inbox, policy())
        .oneshot(
            AxumRequest::post("/api/catga/raft")
                .body(Body::from(valid_message.clone()))
                .expect("request"),
        )
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let (inbox, _receiver) = mpsc::channel(1);
    let response = raft_message_route(inbox, policy())
        .layer(axum::Extension(trusted_identity.clone()))
        .oneshot(request(vec![0xff]))
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let (inbox, _receiver) = mpsc::channel(1);
    let response = raft_message_route(inbox, policy())
        .layer(axum::Extension(trusted_identity.clone()))
        .oneshot(request(vec![0; MAX_RAFT_MESSAGE_BYTES + 1]))
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let (inbox, mut receiver) = mpsc::channel(1);
    inbox
        .try_send(RaftMessage {
            from: 2,
            to: 1,
            ..RaftMessage::default()
        })
        .expect("test inbox fills");
    let response = raft_message_route(inbox, policy())
        .layer(axum::Extension(trusted_identity.clone()))
        .oneshot(request(valid_message.clone()))
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        receiver.recv().await.expect("original message remains"),
        RaftMessage {
            from: 2,
            to: 1,
            ..RaftMessage::default()
        }
    );

    let (inbox, receiver) = mpsc::channel(1);
    drop(receiver);
    let response = raft_message_route(inbox, policy())
        .layer(axum::Extension(trusted_identity))
        .oneshot(request(valid_message))
        .await
        .expect("route responds");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[test]
fn static_raft_policy_rejects_invalid_configuration_and_untrusted_protocol_origins() {
    assert!(matches!(
        RaftPeerIdentity::new(" \t "),
        Err(RaftInboundPolicyError::EmptyIdentity)
    ));
    assert!(matches!(
        StaticRaftInboundPolicy::new(0, std::iter::empty::<(u64, &str)>()),
        Err(RaftInboundPolicyError::ZeroNodeId)
    ));
    assert!(matches!(
        StaticRaftInboundPolicy::new(1, [(2, "node-2"), (2, "node-2-repeated")]),
        Err(RaftInboundPolicyError::DuplicatePeerId)
    ));

    let policy = StaticRaftInboundPolicy::new(1, [(2, "node-2")]).expect("valid member map");
    let peer = RaftPeerIdentity::new("node-2").expect("valid peer identity");
    let valid = RaftMessage {
        from: 2,
        to: 1,
        ..RaftMessage::default()
    };
    assert_eq!(policy.authorize(Some(&peer), &valid), Ok(()));
    assert_eq!(
        policy.authorize(
            Some(&peer),
            &RaftMessage {
                from: 1,
                to: 1,
                ..RaftMessage::default()
            }
        ),
        Err(RaftInboundRejection::Forbidden)
    );
    assert_eq!(
        policy.authorize(
            Some(&peer),
            &RaftMessage {
                from: 0,
                to: 1,
                ..RaftMessage::default()
            }
        ),
        Err(RaftInboundRejection::Forbidden)
    );
}
