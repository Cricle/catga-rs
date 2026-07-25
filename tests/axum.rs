use std::{
    future::IntoFuture,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request as AxumRequest, StatusCode, header::LOCATION},
    middleware,
    response::IntoResponse,
    routing::post,
};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaHttpError, EndpointKind, EndpointMetadata, EndpointValidation,
    HttpClusterForwarder, HttpRaftTransport, IntoCatgaHttpResponse, catga_endpoint_metadata,
    catga_routes, correlation_id, correlation_middleware, endpoint_panic_middleware, event_route,
    leader_forward_route, mediator_route, propagate_correlation_header, raft_message_route,
    validate_min_length, validate_required,
};
use catga_cluster::{ClusterForwarder, RaftMember, RaftMessage, RaftTransport};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, EventHandler, Handler, Mediator, Registry, Request,
    scope_correlation_id,
};
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

#[tokio::test]
async fn http_raft_transport_posts_a_protobuf_frame_to_the_runtime_inbox() {
    let (inbox, mut receiver) = mpsc::channel(1);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(axum::serve(listener, raft_message_route(inbox)).into_future());
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
