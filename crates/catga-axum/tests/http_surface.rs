//! Contract tests for the catga-axum HTTP surface: error mapping, response
//! helpers, endpoint metadata, propagation helpers, middleware, and macros.

#[path = "common/server.rs"]
mod server;

use std::sync::Arc;

use axum::{Router, middleware, response::IntoResponse, routing::get};
use catga_axum::{
    CORRELATION_ID_HEADER, CatgaApplication, CatgaHttpError, EndpointKind, EndpointMetadata,
    EndpointMethod, IntoCatgaHttpResponse, correlation_id, correlation_middleware,
    endpoint_panic_middleware, propagate_correlation_header, propagate_trace_context_headers,
};
use catga_core::{
    CatgaError, CatgaResult, EnvelopeHeaders, ErrorCode, Event, Mediator, MediatorHandle, Message,
    Registry, Request, TraceContext, current_correlation_value, event_handler, request_handler,
    scope_correlation_id, scope_correlation_value, scope_transport_context_value,
};
use http::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ping {
    value: u64,
}
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pinged;
impl Message for Pinged {}
impl Event for Pinged {}

fn ping_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<Ping, _>(request_handler(
            |ping: Ping| async move { Ok(ping.value + 1) },
        ))
        .expect("ping handler registers");
    registry.register_event::<Pinged, _>(event_handler(|_: Pinged| async { Ok(()) }));
    Arc::new(Mediator::new(registry))
}

async fn body_string(response: http::Response<axum::body::Body>) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body must collect");
    String::from_utf8(bytes.to_vec()).expect("body must be UTF-8")
}

#[tokio::test]
async fn catga_http_error_maps_each_category_to_its_http_status() {
    let cases = [
        (
            ErrorCode::Validation,
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation",
        ),
        (
            ErrorCode::HandlerFailed,
            StatusCode::BAD_REQUEST,
            "handler_failed",
        ),
        (ErrorCode::NotFound, StatusCode::NOT_FOUND, "not_found"),
        (ErrorCode::Conflict, StatusCode::CONFLICT, "conflict"),
        (
            ErrorCode::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        ),
        (ErrorCode::Forbidden, StatusCode::FORBIDDEN, "forbidden"),
        (ErrorCode::Timeout, StatusCode::REQUEST_TIMEOUT, "timeout"),
        (
            ErrorCode::Transient,
            StatusCode::SERVICE_UNAVAILABLE,
            "transient",
        ),
        (
            ErrorCode::TransportFailed,
            StatusCode::SERVICE_UNAVAILABLE,
            "transport_failed",
        ),
        (
            ErrorCode::Unsupported,
            StatusCode::NOT_IMPLEMENTED,
            "unsupported",
        ),
        (
            ErrorCode::Internal,
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
        ),
    ];
    for (code, status, stable) in cases {
        let response = CatgaHttpError::from(CatgaError::new(code, "boom")).into_response();
        assert_eq!(response.status(), status, "code {stable} must map");
        let body = body_string(response).await;
        assert!(
            body.contains(&format!("\"code\":\"{stable}\"")),
            "body must carry the stable code, got {body}"
        );
        assert!(
            body.contains("\"message\":\"boom\""),
            "body must carry the message, got {body}"
        );
    }
}

#[tokio::test]
async fn into_catga_response_honors_the_success_status_and_error_mapping() {
    let ok: CatgaResult<u64> = Ok(7);
    let response = ok.into_catga_response(StatusCode::ACCEPTED);
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(body_string(response).await, "7");

    // A 204 success deliberately carries no body.
    let ok: CatgaResult<u64> = Ok(7);
    let response = ok.into_catga_response(StatusCode::NO_CONTENT);
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(body_string(response).await, "");

    let err: CatgaResult<u64> = Err(CatgaError::new(ErrorCode::Conflict, "taken"));
    let response = err.into_catga_response(StatusCode::OK);
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(body_string(response).await.contains("taken"));

    let created: CatgaResult<u64> = Ok(3);
    let response = created.into_catga_created("/orders/3");
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(response.headers()[http::header::LOCATION], "/orders/3");
    assert_eq!(body_string(response).await, "3");

    // An invalid Location value becomes a structured internal error, not a panic.
    let created: CatgaResult<u64> = Ok(3);
    let response = created.into_catga_created("bad\nlocation");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let err: CatgaResult<u64> = Err(CatgaError::new(ErrorCode::NotFound, "gone"));
    let response = err.into_catga_created("/unused");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.headers().get(http::header::LOCATION).is_none());
}

#[test]
fn endpoint_method_and_kind_expose_stable_metadata() {
    assert_eq!(EndpointMethod::Get.as_http_method(), Method::GET);
    assert_eq!(EndpointMethod::Post.as_http_method(), Method::POST);
    assert_eq!(EndpointMethod::Put.as_http_method(), Method::PUT);
    assert_eq!(EndpointMethod::Patch.as_http_method(), Method::PATCH);
    assert_eq!(EndpointMethod::Delete.as_http_method(), Method::DELETE);

    assert_eq!(EndpointKind::Command.tag(), "Commands");
    assert_eq!(EndpointKind::Query.tag(), "Queries");
    assert_eq!(EndpointKind::Event.tag(), "Events");
}

#[test]
fn endpoint_metadata_builders_record_kind_method_and_path() {
    let command = EndpointMetadata::command::<Ping>("/api/ping")
        .with_operation_id("pingOp")
        .with_description("ping the service");
    assert_eq!(command.kind(), EndpointKind::Command);
    assert_eq!(command.method(), Method::POST);
    assert_eq!(command.path(), "/api/ping");
    assert_eq!(command.operation_id(), "pingOp");
    assert_eq!(command.description(), Some("ping the service"));
    assert_eq!(command.tag(), "Commands");
    assert_eq!(
        command.response_statuses(),
        &[
            StatusCode::OK,
            StatusCode::UNPROCESSABLE_ENTITY,
            StatusCode::NOT_FOUND,
            StatusCode::CONFLICT
        ]
    );

    let query = EndpointMetadata::query_with_method::<Ping>(EndpointMethod::Get, "/api/ping-q");
    assert_eq!(query.kind(), EndpointKind::Query);
    assert_eq!(query.method(), Method::GET);
    assert_eq!(query.operation_id(), "Ping");
    assert_eq!(query.description(), None);
    assert_eq!(
        query.response_statuses(),
        &[StatusCode::OK, StatusCode::NOT_FOUND]
    );

    let event = EndpointMetadata::event::<Pinged>("/api/pinged");
    assert_eq!(event.kind(), EndpointKind::Event);
    assert_eq!(event.operation_id(), "Pinged");
    assert_eq!(event.response_statuses(), &[StatusCode::NO_CONTENT]);
}

#[test]
fn catga_endpoint_metadata_expands_to_a_static_catalog() {
    let empty = catga_axum::catga_endpoint_metadata! {
        commands {}
        queries {}
        events {}
    };
    assert_eq!(empty.len(), 0);

    let catalog = catga_axum::catga_endpoint_metadata! {
        commands { "/api/ping" => Ping, @put "/api/ping-put" => Ping }
        queries { @get "/api/ping-q" => Ping }
        events { "/api/pinged" => Pinged, @delete "/api/pinged-gone" => Pinged, @patch "/api/pinged-patch" => Pinged }
    };
    assert_eq!(catalog.len(), 6);
    assert_eq!(catalog[0].kind(), EndpointKind::Command);
    assert_eq!(catalog[1].method(), Method::PUT);
    assert_eq!(catalog[2].kind(), EndpointKind::Query);
    assert_eq!(catalog[2].method(), Method::GET);
    assert_eq!(catalog[3].kind(), EndpointKind::Event);
    assert_eq!(catalog[4].method(), Method::DELETE);
    assert_eq!(catalog[5].method(), Method::PATCH);
}

#[test]
fn propagate_correlation_header_preserves_an_explicit_value() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CORRELATION_ID_HEADER,
        "explicit".parse().expect("valid header value"),
    );
    propagate_correlation_header(&mut headers);
    assert_eq!(headers[CORRELATION_ID_HEADER], "explicit");
}

#[tokio::test]
async fn propagate_correlation_header_prefers_the_ambient_value() {
    let mut headers = HeaderMap::new();
    scope_correlation_value("corr-ambient".into(), async {
        propagate_correlation_header(&mut headers);
    })
    .await;
    assert_eq!(headers[CORRELATION_ID_HEADER], "corr-ambient");
}

#[tokio::test]
async fn propagate_correlation_header_falls_back_to_transport_context_headers() {
    let context = catga_core::TransportContext::from_headers(
        EnvelopeHeaders::try_new([(CORRELATION_ID_HEADER, "corr-transport")])
            .expect("headers must build"),
    );
    let mut headers = HeaderMap::new();
    scope_transport_context_value(context, async {
        propagate_correlation_header(&mut headers);
    })
    .await;
    assert_eq!(headers[CORRELATION_ID_HEADER], "corr-transport");
}

#[tokio::test]
async fn propagate_correlation_header_uses_the_numeric_scope_as_a_last_resort() {
    let mut headers = HeaderMap::new();
    scope_correlation_id(42, async {
        propagate_correlation_header(&mut headers);
    })
    .await;
    assert_eq!(headers[CORRELATION_ID_HEADER], "42");

    let mut empty = HeaderMap::new();
    propagate_correlation_header(&mut empty);
    assert!(empty.get(CORRELATION_ID_HEADER).is_none());
}

#[test]
fn propagate_trace_context_headers_preserves_explicit_values() {
    let mut headers = HeaderMap::new();
    headers.insert(
        catga_core::TRACEPARENT_HEADER,
        "keep".parse().expect("valid header value"),
    );
    propagate_trace_context_headers(&mut headers);
    assert_eq!(headers[catga_core::TRACEPARENT_HEADER], "keep");

    let mut state_only = HeaderMap::new();
    state_only.insert(
        catga_core::TRACESTATE_HEADER,
        "keep".parse().expect("valid header value"),
    );
    propagate_trace_context_headers(&mut state_only);
    assert_eq!(state_only[catga_core::TRACESTATE_HEADER], "keep");
    assert!(state_only.get(catga_core::TRACEPARENT_HEADER).is_none());
}

#[tokio::test]
async fn propagate_trace_context_headers_stamps_the_scoped_context() {
    let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let context = TraceContext::parse(parent, Some("congo=t61rcWkgMzE"))
        .expect("valid context")
        .to_transport_context()
        .expect("context converts");

    let mut headers = HeaderMap::new();
    scope_transport_context_value(context, async {
        propagate_trace_context_headers(&mut headers);
    })
    .await;
    assert_eq!(headers[catga_core::TRACEPARENT_HEADER], parent);
    assert_eq!(headers[catga_core::TRACESTATE_HEADER], "congo=t61rcWkgMzE");

    let mut empty = HeaderMap::new();
    propagate_trace_context_headers(&mut empty);
    assert!(empty.get(catga_core::TRACEPARENT_HEADER).is_none());
}

#[test]
fn correlation_id_parses_numeric_headers_and_generates_monotonic_fallbacks() {
    let mut headers = HeaderMap::new();
    headers.insert(
        CORRELATION_ID_HEADER,
        "123".parse().expect("valid header value"),
    );
    assert_eq!(correlation_id(&headers), 123);

    let mut non_numeric = HeaderMap::new();
    non_numeric.insert(
        CORRELATION_ID_HEADER,
        "abc".parse().expect("valid header value"),
    );
    let first = correlation_id(&non_numeric);
    let second = correlation_id(&HeaderMap::new());
    assert!(second > first, "fallback ids must increase monotonically");
}

#[tokio::test]
async fn correlation_middleware_scopes_and_echoes_the_inbound_value() {
    let app = Router::new()
        .route(
            "/probe",
            get(|| async { current_correlation_value().unwrap_or_default().to_string() }),
        )
        .layer(middleware::from_fn(correlation_middleware));
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/probe"))
        .header(CORRELATION_ID_HEADER, "corr-9")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.headers()[CORRELATION_ID_HEADER], "corr-9");
    assert_eq!(response.text().await.expect("body"), "corr-9");

    // Without a header a numeric fallback is generated, scoped, and echoed.
    let response = client
        .get(format!("{base}/probe"))
        .send()
        .await
        .expect("request succeeds");
    let echoed = response.headers()[CORRELATION_ID_HEADER]
        .to_str()
        .expect("echo header")
        .to_string();
    let body = response.text().await.expect("body");
    assert_eq!(echoed, body);
    assert!(body.parse::<u64>().is_ok(), "fallback must be numeric");

    server.abort();
}

#[tokio::test]
async fn endpoint_panic_middleware_converts_unwinds_into_internal_errors() {
    async fn exploding() -> &'static str {
        panic!("handler blew up")
    }
    let app = Router::new()
        .route("/panic", get(exploding))
        .layer(middleware::from_fn(endpoint_panic_middleware));
    let (base, server) = server::spawn_app(app).await;

    let response = reqwest::Client::new()
        .get(format!("{base}/panic"))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("body");
    assert!(body.contains("\"code\":\"internal\""), "got {body}");

    server.abort();
}

#[test]
fn catga_routes_macro_builds_typed_routers() {
    let mediator = ping_mediator();
    let router = catga_axum::catga_routes! {
        mediator = Arc::clone(&mediator);
        requests {
            "/api/ping" => Ping,
            @put "/api/ping-put" => Ping,
        }
        events {
            "/api/pinged" => Pinged,
        }
    }
    .expect("routes must build");
    drop(router);

    let events_only = catga_axum::catga_routes! {
        mediator = Arc::clone(&mediator);
        requests {}
        events {
            "/api/pinged" => Pinged,
            @post "/api/pinged-alt" => Pinged,
        }
    }
    .expect("events-only routes must build");
    drop(events_only);

    let invalid = catga_axum::catga_routes! {
        mediator = ping_mediator();
        requests { "api/relative" => Ping }
        events {}
    };
    assert!(invalid.is_err(), "relative paths must be rejected");
}

#[test]
fn catga_application_macro_composes_registry_mediator_and_router() {
    let application = catga_axum::catga_application! {
        handlers {
            request Ping => request_handler(|ping: Ping| async move { Ok(ping.value) });
        }
        routes {
            requests { "/api/ping" => Ping }
            events { "/api/pinged" => Pinged }
        }
    }
    .expect("application must build");
    let _mediator: Arc<Mediator> = application.mediator();
    let _router: Router = application.router();

    let handle = MediatorHandle::new();
    let application = catga_axum::catga_application! {
        mediator_handle = handle.clone();
        handlers {
            request Ping => request_handler(|ping: Ping| async move { Ok(ping.value) });
        }
        routes {
            requests { "/api/ping" => Ping }
            events {}
        }
    }
    .expect("application must build");
    assert!(handle.is_bound());
    drop(application);
}

#[tokio::test]
async fn axum_routes_macro_expands_native_handlers() {
    async fn hello() -> &'static str {
        "hello"
    }
    let router = catga_axum::axum_routes! {
        Router::new();
        GET "/hello" => hello,
        POST "/hello-post" => hello,
        PUT "/hello-put" => hello,
        PATCH "/hello-patch" => hello,
        DELETE "/hello-delete" => hello,
    };
    let (base, server) = server::spawn_app(router).await;
    let client = reqwest::Client::new();
    for (method, path) in [
        (Method::GET, "/hello"),
        (Method::POST, "/hello-post"),
        (Method::PUT, "/hello-put"),
        (Method::PATCH, "/hello-patch"),
        (Method::DELETE, "/hello-delete"),
    ] {
        let response = client
            .request(method, format!("{base}{path}"))
            .send()
            .await
            .expect("request succeeds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.expect("body"), "hello");
    }
    server.abort();
}

#[test]
fn catga_application_can_be_cloned_from_parts() {
    let mediator = ping_mediator();
    let application = CatgaApplication::new(Arc::clone(&mediator), Router::new());
    let clone = application.clone();
    assert!(Arc::ptr_eq(&clone.mediator(), &mediator));
}
