//! Contract tests for the typed route builders in `compat`: mediator and event
//! routes, leader forwarding routes, and inbound trace-context scoping.

#[path = "common/server.rs"]
mod server;

use std::sync::Arc;

use axum::Router;
use catga_axum::{
    EndpointMethod, HttpClusterForwarder, event_route, event_route_with_method,
    leader_forward_route, leader_forward_route_at, mediator_route, mediator_route_with_method,
};
use catga_core::{
    CatgaError, ClusterForwarder, ErrorCode, Event, Mediator, Message, Registry, Request,
    current_transport_context, event_handler, request_handler,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Add {
    value: u64,
}
impl Message for Add {}
impl Request for Add {
    type Response = u64;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Fail;
impl Message for Fail {}
impl Request for Fail {
    type Response = u64;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Added;
impl Message for Added {}
impl Event for Added {}

fn add_mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<Add, _>(request_handler(|add: Add| async move { Ok(add.value + 1) }))
        .expect("handler registers");
    registry.register_event::<Added, _>(event_handler(|_: Added| async { Ok(()) }));
    Arc::new(Mediator::new(registry))
}

#[tokio::test]
async fn mediator_routes_dispatch_typed_requests_and_map_failures() {
    let mut registry = Registry::new();
    registry
        .register_request::<Add, _>(request_handler(|add: Add| async move { Ok(add.value + 1) }))
        .expect("handler registers");
    registry
        .register_request::<Fail, _>(request_handler(|_: Fail| async {
            Err(CatgaError::new(ErrorCode::Conflict, "rejected"))
        }))
        .expect("handler registers");
    let mediator = Arc::new(Mediator::new(registry));

    let app = Router::new()
        .merge(mediator_route::<Add>("/api/add", Arc::clone(&mediator)).expect("route"))
        .merge(
            mediator_route_with_method::<Add>(
                EndpointMethod::Put,
                "/api/add-put",
                Arc::clone(&mediator),
            )
            .expect("route"),
        )
        .merge(mediator_route::<Fail>("/api/fail", Arc::clone(&mediator)).expect("route"));
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/add"))
        .json(&Add { value: 41 })
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "42");

    // The same typed endpoint registered for another verb dispatches too.
    let response = client
        .put(format!("{base}/api/add-put"))
        .json(&Add { value: 1 })
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "2");

    // Handler failures flow through the Catga error-to-status mapping.
    let response = client
        .post(format!("{base}/api/fail"))
        .json(&Fail)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = response.text().await.expect("body");
    assert!(body.contains("\"code\":\"conflict\""), "got {body}");

    server.abort();
}

#[test]
fn typed_route_registration_rejects_relative_paths() {
    let mediator = add_mediator();
    assert!(mediator_route::<Add>("api/add", Arc::clone(&mediator)).is_err());
    assert!(mediator_route::<Add>("/", Arc::clone(&mediator)).is_err());
    assert!(event_route::<Added>("api/added", Arc::clone(&mediator)).is_err());
    assert!(
        event_route_with_method::<Added>(EndpointMethod::Get, "api/added", Arc::clone(&mediator))
            .is_err()
    );
}

#[tokio::test]
async fn event_routes_publish_and_scope_inbound_trace_context() {
    let mediator = add_mediator();
    let app = Router::new()
        .merge(event_route::<Added>("/api/added", Arc::clone(&mediator)).expect("route"))
        .merge(
            event_route_with_method::<Added>(
                EndpointMethod::Put,
                "/api/added-put",
                Arc::clone(&mediator),
            )
            .expect("route"),
        );
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/added"))
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .json(&Added)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = client
        .put(format!("{base}/api/added-put"))
        .header("traceparent", "not-valid")
        .json(&Added)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    server.abort();
}

#[tokio::test]
async fn leader_forward_routes_serve_the_http_cluster_forwarder() {
    let mediator = add_mediator();
    let app = Router::new()
        .merge(leader_forward_route::<Add>(Arc::clone(&mediator)))
        .merge(leader_forward_route_at::<Add>(
            "/custom/forward",
            Arc::clone(&mediator),
        ));
    let (base, server) = server::spawn_app(app).await;

    let forwarder = HttpClusterForwarder::new(reqwest::Client::new());
    let added = forwarder
        .forward(Add { value: 10 }, &base)
        .await
        .expect("forward succeeds");
    assert_eq!(added, 11);

    let custom = HttpClusterForwarder::new(reqwest::Client::new())
        .with_path_builder(|leader, _| format!("{leader}/custom/forward"));
    let added = custom
        .forward(Add { value: 20 }, &base)
        .await
        .expect("forward succeeds");
    assert_eq!(added, 21);

    server.abort();
}

#[tokio::test]
async fn mediator_routes_run_inside_the_inbound_trace_scope() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Scoped;
    impl Message for Scoped {}
    impl Request for Scoped {
        type Response = bool;
    }

    let mut registry = Registry::new();
    registry
        .register_request::<Scoped, _>(request_handler(|_: Scoped| async {
            Ok(current_transport_context().is_some())
        }))
        .expect("handler registers");
    let mediator = Arc::new(Mediator::new(registry));
    let app = mediator_route::<Scoped>("/api/scoped", mediator).expect("route");
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    // A valid inbound traceparent is scoped through mediator dispatch.
    let response = client
        .post(format!("{base}/api/scoped"))
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .json(&Scoped)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "true");

    // Without one the handler runs unscoped.
    let response = client
        .post(format!("{base}/api/scoped"))
        .json(&Scoped)
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "false");

    server.abort();
}
